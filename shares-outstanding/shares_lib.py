"""shares_lib.py — core engine for extracting "shares issued and outstanding"
from SEC annual filings (10-K, 20-F, 40-F).

This module is imported by the numbered runner scripts in this folder. It is NOT
meant to be run directly; run the scripts (each has its own EDIT THIS block).

What it does, end to end:
  1. fetch + parse the EDGAR full-index master files (one per quarter)
  2. draw a stratified random sample of annual filings (50% 10-K / 40% 20-F / 10% 40-F)
  3. download each filing's primary document and reduce it to clean text
  4. locate the cover-page share-count statement and extract, per share class:
        - the number of shares
        - the kind/class of shares (common / ordinary / preferred / other)
        - the applicable "as of" date
  5. cross-check the extracted number against SEC's own structured XBRL fact
        (dei:EntityCommonStockSharesOutstanding) when available
  6. attach a confidence score and human-readable flags so a reviewer can tell
        right answers from wrong ones, and spot likely false negatives.

Design notes on reliability (the three questions that matter):
  - "Did we get the RIGHT number?"   -> high confidence requires a plausible
       magnitude, an "as of" date, an identified class, and (where the filer
       tags it) agreement with the XBRL dei fact. method='anchor' (the fixed
       regulatory phrase on 20-F/40-F covers) is the most trustworthy.
  - "Did we get a WRONG number?"     -> flags XBRL_MISMATCH, NUMBER_OUT_OF_RANGE,
       DATE_IMPLAUSIBLE, DECOY_CONTEXT fire on the classic traps (balance-sheet
       figures in thousands, authorized shares, weighted-average shares, dollar
       amounts, treasury stock).
  - "Did we MISS it (false negative)?" -> NO_MATCH fires when nothing was found;
       XBRL_FOUND_BUT_NO_PROSE fires when SEC has a tagged number but our prose
       scrape came up empty -> a near-certain miss to review. NO_COVER_MARKERS
       fires when the document doesn't even look like it has a cover page.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import random
import re
import time
import zipfile
from dataclasses import dataclass, field, asdict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# --- config (USER_AGENT is required by the SEC; loaded from the local config.py) -
try:
    from config import USER_AGENT
except ImportError:  # pragma: no cover - guidance only
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT (and DATA_DIR)."
    )

# Forms we treat as "annual filings" and the sampling mix requested.
ANNUAL_FORMS = ("10-K", "20-F", "40-F")
DEFAULT_MIX = {"10-K": 0.50, "20-F": 0.40, "40-F": 0.10}

REQUEST_INTERVAL_SEC = 0.15  # ~6-7 req/s, under SEC's 10 req/s cap
PLAUSIBLE_MIN_SHARES = 1_000          # subsidiaries can have very few shares
PLAUSIBLE_MAX_SHARES = 5_000_000_000_000  # 5 trillion ceiling

# On-disk caches so the iterate-until-correct loop is cheap: the primary document
# (keyed by accession) and the XBRL dei fact (keyed by CIK) are fetched from SEC
# exactly once. After a parser change, re-running the extractor reads everything
# from these caches (no network), so only the parsing logic re-runs. Both live
# under .cache/ (git-ignored). Behaviour with a warm cache is byte-identical to a
# cold fetch — the cache stores exactly what the network call returned.
_CACHE_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
DOC_CACHE_DIR = os.path.join(_CACHE_ROOT, "docs")
XBRL_CACHE_DIR = os.path.join(_CACHE_ROOT, "xbrl")
# Cleaned-text cache: the BeautifulSoup reduction of the primary document is the
# slowest step and never changes when the PARSER changes, so caching it makes the
# iterate-until-correct loop fast (re-extraction reads clean text and re-runs only
# the regex logic). Delete .cache/clean to force a re-clean.
CLEAN_CACHE_DIR = os.path.join(_CACHE_ROOT, "clean")


def _read_doc_cache(accession):
    p = os.path.join(DOC_CACHE_DIR, f"{accession}.json.gz")
    if not os.path.exists(p):
        return None
    try:
        with gzip.open(p, "rt", encoding="utf-8") as f:
            d = json.load(f)
        return d["doc_type"], d["raw"], d["period"]
    except Exception:
        return None


def _write_doc_cache(accession, doc_type, raw, period):
    os.makedirs(DOC_CACHE_DIR, exist_ok=True)
    p = os.path.join(DOC_CACHE_DIR, f"{accession}.json.gz")
    tmp = p + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump({"doc_type": doc_type, "raw": raw, "period": period}, f)
    os.replace(tmp, p)


def _read_clean_cache(accession):
    p = os.path.join(CLEAN_CACHE_DIR, f"{accession}.json.gz")
    if not os.path.exists(p):
        return None
    try:
        with gzip.open(p, "rt", encoding="utf-8") as f:
            d = json.load(f)
        return d["doc_type"], d["text"], d["period"]
    except Exception:
        return None


def _write_clean_cache(accession, doc_type, text, period):
    os.makedirs(CLEAN_CACHE_DIR, exist_ok=True)
    p = os.path.join(CLEAN_CACHE_DIR, f"{accession}.json.gz")
    tmp = p + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump({"doc_type": doc_type, "text": text, "period": period}, f)
    os.replace(tmp, p)


def _read_xbrl_cache(cik):
    p = os.path.join(XBRL_CACHE_DIR, f"CIK{str(int(cik)).zfill(10)}.json")
    if not os.path.exists(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_xbrl_cache(cik, vals):
    os.makedirs(XBRL_CACHE_DIR, exist_ok=True)
    p = os.path.join(XBRL_CACHE_DIR, f"CIK{str(int(cik)).zfill(10)}.json")
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(vals, f)
    os.replace(tmp, p)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class Filing:
    cik: str
    company: str
    form: str
    date_filed: str
    accession: str
    filename: str  # e.g. edgar/data/320193/0000320193-25-000079.txt

    @property
    def txt_url(self) -> str:
        return f"https://www.sec.gov/Archives/{self.filename}"

    @property
    def index_url(self) -> str:
        acc = self.accession.replace("-", "")
        return (
            f"https://www.sec.gov/Archives/edgar/data/{int(self.cik)}/{acc}/"
            f"{self.accession}-index.html"
        )


@dataclass
class ClassEntry:
    """One (number, class, date) tuple extracted for a single share class."""
    shares: int | None
    raw_number: str
    scale: str            # '', 'thousand', 'million', 'billion'
    class_label: str      # the literal label as printed, e.g. "Class A Common Stock"
    share_type: str       # common | ordinary | preferred | depositary | other
    as_of_date: str       # ISO yyyy-mm-dd, or '' if none found
    raw_date: str
    matched_text: str     # the surrounding snippet the value came from


@dataclass
class Extraction:
    filing: Filing
    method: str = ""                  # anchor | cover_window | none
    entries: list[ClassEntry] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)
    confidence: float = 0.0
    doc_type: str = ""                # the <TYPE> of the primary document we read
    period_of_report: str = ""        # CONFORMED PERIOD OF REPORT (fiscal close), ISO
    text_len: int = 0
    xbrl_values: list[dict] = field(default_factory=list)
    error: str = ""


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    if "@" not in USER_AGENT:
        raise SystemExit(
            "Set USER_AGENT in config.py to 'Your Name your@email' — SEC blocks generic UAs."
        )
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"})
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def _throttle():
    time.sleep(REQUEST_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# EDGAR full-index (master.idx) -> Filing rows
# ---------------------------------------------------------------------------
def fetch_master_index(session, year, quarter, cache_dir) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"master_{year}_QTR{quarter}.idx")
    if os.path.exists(cache_path):
        with open(cache_path, encoding="latin-1") as f:
            return f.read()
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    text = resp.text
    with open(cache_path, "w", encoding="latin-1") as f:
        f.write(text)
    _throttle()
    return text


def parse_master_index(text, forms=ANNUAL_FORMS) -> list[Filing]:
    """master.idx is pipe-delimited: CIK|Company|Form|Date Filed|Filename.
    Match form type exactly (so '10-K' does not also pull '10-K/A')."""
    forms = set(forms)
    out: list[Filing] = []
    for ln in text.splitlines():
        parts = ln.split("|")
        if len(parts) != 5:
            continue
        cik, company, form, date_filed, filename = (p.strip() for p in parts)
        if form not in forms or not cik.isdigit():
            continue
        accession = os.path.basename(filename).replace(".txt", "")
        out.append(Filing(cik, company, form, date_filed, accession, filename))
    return out


def build_index(session, year, cache_dir, forms=ANNUAL_FORMS) -> list[Filing]:
    filings: list[Filing] = []
    for q in (1, 2, 3, 4):
        text = fetch_master_index(session, year, q, cache_dir)
        filings.extend(parse_master_index(text, forms))
    return filings


def stratified_sample(filings, n, mix=DEFAULT_MIX, seed=20260607):
    """Draw `n` filings split by form per `mix`. Dedupes by accession first.
    Returns (sample, report) where report records target vs available vs taken."""
    rng = random.Random(seed)
    by_form: dict[str, list[Filing]] = {}
    seen = set()
    for f in filings:
        if f.accession in seen:
            continue
        seen.add(f.accession)
        by_form.setdefault(f.form, []).append(f)

    sample: list[Filing] = []
    report = {}
    for form, frac in mix.items():
        target = round(n * frac)
        avail = by_form.get(form, [])
        take = min(target, len(avail))
        sample.extend(rng.sample(avail, take))
        report[form] = {"target": target, "available": len(avail), "taken": take}
    rng.shuffle(sample)
    return sample, report


# ---------------------------------------------------------------------------
# Primary document download + text reduction
# ---------------------------------------------------------------------------
def fetch_primary_document(session, filing, cap_bytes=50_000_000, use_cache=True):
    """Return (doc_type, raw_html, period) for the filing's primary document,
    reading from the on-disk cache when present so re-runs don't re-download.
    The network path is in `_fetch_primary_document_network`; the cache stores
    exactly its return value, so a warm read is identical to a cold fetch."""
    if use_cache:
        cached = _read_doc_cache(filing.accession)
        if cached is not None:
            return cached
    result = _fetch_primary_document_network(session, filing, cap_bytes)
    if use_cache and result[1]:  # only cache a non-empty primary document
        _write_doc_cache(filing.accession, *result)
    return result


def get_clean_document(session, filing, use_cache=True):
    """Return (doc_type, clean_text, period) for the filing's primary document,
    reading the cleaned text from cache when present (so neither the network fetch
    nor the BeautifulSoup reduction re-runs). This is the fast path the runner
    scripts use; a warm clean cache makes re-extraction after a parser change
    near-instant."""
    if use_cache:
        c = _read_clean_cache(filing.accession)
        if c is not None:
            return c
    doc_type, raw, period = fetch_primary_document(session, filing, use_cache=use_cache)
    text = html_to_text(raw)
    if use_cache and text:
        _write_clean_cache(filing.accession, doc_type, text, period)
    return doc_type, text, period


def _fetch_primary_document_network(session, filing, cap_bytes=50_000_000):
    """Stream the full submission .txt and return (doc_type, raw_html, period) for
    the FIRST <DOCUMENT> whose <TYPE> matches the filing's form. Streaming lets us
    stop after the primary document so we don't pull megabytes of XBRL/exhibits.
    `period` is the SGML header's CONFORMED PERIOD OF REPORT as ISO (fiscal close).

    The cap is generous (50 MB) because a few large filers (big-bank 20-Fs such as
    Barclays / KB Financial) carry a multi-megabyte inline-XBRL header before the
    readable cover, which a small cap would truncate away. To keep the join cheap
    on those huge docs we only rebuild + scan the buffer when a chunk closes a
    document or every ~1 MB, not on every chunk."""
    resp = session.get(filing.txt_url, timeout=180, stream=True)
    resp.raise_for_status()
    buf, total = [], 0
    try:
        for chunk in resp.iter_content(chunk_size=262144, decode_unicode=True):
            if not chunk:
                continue
            buf.append(chunk)
            total += len(chunk)
            # A chunk closing a document means a complete document is in the buffer:
            # try to extract (require_closed so a half-streamed primary doc is never
            # returned truncated). This bounds the join to once per </DOCUMENT>.
            if total > 40_000 and "</DOCUMENT>" in chunk:
                joined = "".join(buf)
                hit = _first_matching_document(joined, filing.form, require_closed=True)
                if hit:
                    return hit[0], hit[1], _period_of_report(joined)
            if total > cap_bytes:
                break
    finally:
        resp.close()
        _throttle()
    joined = "".join(buf)
    hit = _first_matching_document(joined, filing.form)
    period = _period_of_report(joined)
    return (hit[0], hit[1], period) if hit else ("", "", period)


def _period_of_report(sgml_head):
    m = re.search(r"CONFORMED PERIOD OF REPORT:\s*(\d{4})(\d{2})(\d{2})", sgml_head[:8000])
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""


def _first_matching_document(sgml, form, require_closed=False):
    """Find the first <DOCUMENT> whose <TYPE> matches `form` and return
    (type, text). When `require_closed` (used while still streaming), only a
    document whose <TEXT> is actually closed (</TEXT>/</DOCUMENT>) qualifies, so a
    half-streamed primary document is never returned truncated. The final post-cap
    call leaves it False so a document truncated exactly at the byte cap still
    yields whatever streamed."""
    base = form.upper().split("/")[0]
    tail = r"(</TEXT>|</DOCUMENT>)" if require_closed else r"(</TEXT>|</DOCUMENT>|$)"
    for doc in re.split(r"<DOCUMENT>", sgml)[1:]:
        tm = re.search(r"<TYPE>\s*([^\s<]+)", doc)
        if not tm:
            continue
        dtype = tm.group(1).strip()
        if dtype.upper().split("/")[0] == base:
            txm = re.search(r"<TEXT>(.*?)" + tail, doc, re.DOTALL)
            if txm:
                return dtype, txm.group(1)
    return None


def html_to_text(raw):
    """Reduce a filing document (HTML / inline-XBRL / plain text) to clean text.
    Inline-XBRL hidden facts are stripped so they don't pollute the cover page."""
    if not raw:
        return ""
    if "<" not in raw:  # already plain text
        return _normalize_ws(raw)
    soup = BeautifulSoup(raw, "lxml")
    for tag in soup.find_all(re.compile(r"^ix:(header|hidden|references|resources)$", re.I)):
        tag.decompose()
    for tag in soup(["script", "style"]):
        tag.decompose()
    return _normalize_ws(soup.get_text(" "))


_PUNCT_MAP = {
    "’": "'", "‘": "'", "“": '"', "”": '"',
    "–": "-", "—": "-", "−": "-", " ": " ", "​": "",
    " ": " ", " ": " ", "﻿": "",
}


def _normalize_ws(txt):
    for k, v in _PUNCT_MAP.items():
        txt = txt.replace(k, v)
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\s*\n\s*", "\n", txt)
    return txt.strip()


def cover_region(text):
    """The cover page lives before the table of contents / PART I. Return that
    slice (capped) so the extractor is not distracted by the body of the filing
    (balance sheet, MD&A, business narrative). Capped at 15k chars because real
    cover share-count statements are always near the top."""
    m = re.search(r"\n\s*part\s+i\b", text, re.I)
    end = m.start() if m else 15_000
    return text[: min(end, 15_000)]


# ---------------------------------------------------------------------------
# Low-level finders: dates, share-count numbers, class labels
# ---------------------------------------------------------------------------
_MONTHS = ("January|February|March|April|May|June|July|August|September|"
           "October|November|December")
_DATE_RE = re.compile(
    rf"\b(?:{_MONTHS})\s+\d{{1,2}}\s*,?\s+\d{{4}}\b"
    rf"|\b\d{{1,2}}\s+(?:{_MONTHS})\s*,?\s+\d{{4}}\b"
    rf"|\b\d{{4}}-\d{{2}}-\d{{2}}\b"
    rf"|\b(?:0?[1-9]|1[0-2])/(?:0?[1-9]|[12]\d|3[01])/(?:19|20)\d{{2}}\b",
    re.I,
)
_MONTH_NUM = {m.lower(): i + 1 for i, m in enumerate(
    "January February March April May June July August September October "
    "November December".split())}

SCALE_FACTOR = {"": 1, "thousand": 1_000, "thousands": 1_000,
                "million": 1_000_000, "millions": 1_000_000,
                "billion": 1_000_000_000, "billions": 1_000_000_000}

# a candidate share-count number: grouped (1,234,567) or a bare run of >=3 digits,
# optionally followed by a scale word. Leading (?<![\$.\d]) keeps us off money and
# decimals; we post-filter '%' and 'days'/'Section'/'Rule' contexts.
_NUM_RE = re.compile(
    r"(?<![\$.\d])(\d{1,3}(?:,\d{3})+|\d+\.\d+|\d{3,}"
    r"|\d{1,2}(?=\s*(?:thousand|million|billion)s?\b))"  # "51 million"
    r"\s*(thousand|million|billion|thousands|millions|billions)?",
    re.I,
)


# A run is space-grouped thousands only if it is NOT also comma-grouped: a trailing
# comma/digit means the space joined a label number to a real count ("Series 27 850,000").
_SPACE_GROUPED_RE = re.compile(r"(?<![\d.])\d{1,3}(?: \d{3})+(?![,\d])")

# A comma-grouped number split across HTML table cells ("1,249 302" for 1,249,302):
# an already-comma-grouped prefix followed by exactly one whitespace and a 3-digit
# group. Dates never produce this shape (the space there follows the comma).
_COMMA_SPLIT_RE = re.compile(r"(?<![\d.])(\d{1,3}(?:,\d{3})+)\s(\d{3})(?![\d.])")

# key words with one spurious space injected mid-word by an HTML tag boundary
# ("outsta nding", "Clas s", "share s") — the scanner and the label binders key
# on these exact words.
_KW_SPLIT_RES = [
    (kw, [re.compile(rf"\b{kw[:k]}\s+{kw[k:]}\b", re.I)
          for k in range(2, len(kw))])
    for kw in ("outstanding", "class", "series", "shares")
]


def _repair_artifacts(s):
    prev = None
    while prev != s:
        prev = s
        s = _COMMA_SPLIT_RE.sub(r"\1,\2", s)
    for kw, regexes in _KW_SPLIT_RES:
        for kw_re in regexes:
            s = kw_re.sub(lambda m, w=kw: w.capitalize() if m.group(0)[0].isupper() else w, s)
    return s


def _despace_numbers(s):
    """Collapse European space-grouped thousands ("5 605 850 345" -> "5605850345")
    so the comma/contiguous-digit number regex can read them. Only a clean grouping
    is merged: a 1-3 digit lead group (not preceded by a digit or a decimal point)
    followed by one or more space-separated EXACT 3-digit groups. This leaves dates
    ("December 31 2024"), a decimal next to a separate number ("US$0.0000025
    322,664,816"), and ordinary prose untouched."""
    return _SPACE_GROUPED_RE.sub(lambda m: m.group(0).replace(" ", ""), s)

_SHARE_WORDS = ("share", "stock", "common", "ordinary", "capital stock",
                "preferred", "preference", "units", "depositary")


def iso_date(raw):
    raw = raw.strip().rstrip(".")
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(rf"({_MONTHS})\s+(\d{{1,2}})\s*,?\s+(\d{{4}})", raw, re.I)
    if m:
        mo = _MONTH_NUM[m.group(1).lower()]
        return f"{int(m.group(3)):04d}-{mo:02d}-{int(m.group(2)):02d}"
    m = re.match(rf"(\d{{1,2}})\s+({_MONTHS})\s*,?\s+(\d{{4}})", raw, re.I)
    if m:
        mo = _MONTH_NUM[m.group(2).lower()]
        return f"{int(m.group(3)):04d}-{mo:02d}-{int(m.group(1)):02d}"
    m = re.match(r"(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})", raw)
    if m:  # US cover tables print MM/DD/YYYY
        return f"{int(m.group(3)):04d}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return ""


def class_designator(label):
    """The bare class/series designator: "Class AX common stock" -> "AX",
    "Series A Preferred Stock" -> "A", "Class I-S" -> "I-S"; "" when the label
    names no class."""
    ms = re.findall(r"\b(?:class|series)\s+(?!of\b|no\b)[\"']?([a-z0-9]{1,3}(?:-[a-z0-9]{1,3})?)[\"']?(?=[\s,.;:)]|$)",
                    (label or "").lower())
    return ms[-1].upper() if ms else ""


def classify_share_type(label):
    low = label.lower()
    if "preferred" in low or "preference" in low:
        return "preferred"
    if "depositary" in low or "depository" in low or " ads" in low or low.startswith("ads"):
        return "depositary"
    # units of beneficial interest / LP / trust units are not common stock
    if "unit" in low:
        return "other"
    if "investment share" in low or re.search(r"\bspecial\s+shares?\b", low):
        return "other"
    if "ordinary" in low:
        return "ordinary"
    if "common" in low or "voting share" in low or "capital stock" in low:
        return "common"
    # "Class A stock", "Class C shares" etc. are common equity unless tagged preferred
    if re.search(r"\bclass\s+[a-z]\b", low) and ("stock" in low or "share" in low):
        return "common"
    # a sole, unqualified "shares"/"stock" class is the common/ordinary equity
    if "share" in low or "stock" in low:
        return "common"
    return "other"


_YEAR_RE = re.compile(r"^(?:19|20)\d{2}$")


def _looks_like_year(num_str, scale_word):
    """A bare 4-digit 19xx/20xx with no thousands-comma and no scale word is
    almost certainly a calendar year bleeding in from a date, not a share count."""
    return bool(_YEAR_RE.match(num_str)) and "," not in num_str and not scale_word


def _nearest_date(text, pos, window=260):
    """Date closest to pos; prefer one introduced by 'as of'/'outstanding at/on'
    within the window, and push away dates that belong to the aggregate-market-
    value clause (a $ amount beside the date, or the 'last business day of the
    second fiscal quarter' formula) — those describe the public float, never the
    cover count."""
    lo, hi = max(0, pos - window), min(len(text), pos + window)
    span = text[lo:hi]
    best, best_d = "", 10 ** 9
    for m in _DATE_RE.finditer(span):
        center = lo + (m.start() + m.end()) // 2
        d = abs(center - pos)
        # whitespace-tolerant: HTML tag boundaries split "as of" into "as o f" /
        # "As\nof", which must still earn the bias
        pre = re.sub(r"\s+", " ", span[max(0, m.start() - 22):m.start()]).lower()
        if re.search(r"\bas\s+o\s*f\s*$|\bas\s+at\s*$|\boutstanding\s+(?:at|on)\s*$", pre):
            d -= 50
        # a $ amount in the SAME clause just before the date ("price of $2.45 …
        # as of <date>") — but a $ across a sentence/line boundary ("… was $66.1
        # million. As of <date> …") belongs to the previous sentence, and a
        # par-value decimal ("par value $0.001 per share, outstanding as of …")
        # is not a market-value signal: no penalty for those
        pre36 = span[max(0, m.start() - 36):m.start()]
        seg = pre36.split("$")[-1] if "$" in pre36 else None
        if re.search(r"\b(?:effected\s+on|effective(?:\s+as\s+of)?(?:\s+on)?)\s*$",
                     re.sub(r"\s+", " ", span[max(0, m.start() - 26):m.start()]), re.I):
            continue  # "consolidation effected on <date>" — a corporate-action date
        if (seg is not None and not re.search(r"[.;:]\s|\n", seg)
                and not re.match(r"\s?0?\.\d", seg)) or \
           re.match(r"\s*,?\s*(?:\([^)]{0,14}\)\s*)?(?:the\s+last\s+business\s+day\b|"
                    r"(?:was|is|were|:)\s*(?:approximately\s+)?\$)",
                    span[m.end():m.end() + 60], re.I):
            d += 80
        if d < best_d:
            best, best_d = m.group(0), d
    return best


_CLASS_TAIL = (r"(?:(?:Class|Series)\s+[A-Z0-9]{1,3}(?:-[A-Z0-9]{1,3})?\b[\w ]*?)?(?:Common\s+Stock|Common\s+Shares|"
               r"Ordinary\s+Shares|Preferred\s+Stock|Preference\s+Shares|"
               r"Redeemable\s+Capital\s+Shares|Limited\s+Voting\s+Shares|"
               r"Subordinate\s+Voting\s+Shares|Capital\s+Stock|Common\s+Units|"
               r"Stock|Shares|Units)")


def _grab_class_label(text, num_start, num_end):
    """Heuristic class label. Prefer a label that FOLLOWS the number in a
    'shares of <CLASS>' construction (Apple/Alphabet); otherwise take the
    nearest 'Class X ...' or class keyword before the number (Amerant)."""
    post = text[num_end:num_end + 110]
    # "N [(parenthetical)] [outstanding] shares of [the registrant's] <CLASS>" —
    # tolerates the split-word artifact "o f" and an intervening "outstanding"
    m = re.match(r"[\s,]*(?:\([^)]{0,60}\)\s*)?(?:thousand|million|billion)?\s*"
                 r"(?:outstanding\s+)?shares?\s+o\s*f\s+"
                 r"(?:the\s+|its\s+|Alphabet'?s?\s+|registrant'?s?\s+)*"
                 r"(" + _CLASS_TAIL + r")", post, re.I)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    pre = text[max(0, num_start - 110):num_start]
    pm = list(re.finditer(r"((?:Class|Series)\s+[A-Z0-9]{1,3}(?:-[A-Z0-9]{1,3})?\b[\w ]*?(?:Common\s+Stock|Common\s+Shares|"
                          r"Preferred\s+Stock|Stock|Shares)|Common\s+Stock|Common\s+Shares|Ordinary\s+Shares|"
                          r"Preferred\s+Stock|Preference\s+Shares|Capital\s+Stock|"
                          r"Redeemable\s+Capital\s+Shares|Limited\s+Voting\s+Shares)", pre, re.I))
    # table-row layout: a STANDALONE class label right before the number (not
    # attached to its own preceding count) is this number's label — "Class B
    # Common Stock … (the "Class B Common Stock") 677,234 Class C Common Stock …"
    # "Attached" = a count directly owns the phrase ("18,909,000 of the
    # registrant's Class A …", "51,077,297 outstanding shares of common stock,
    # consisting of …") — then the phrase belongs to THAT number, not this one.
    # list layout: "… 4,772 Class D shares …", "1,758,476 of the registrant's
    # Class A ordinary shares" — a designator-bearing label following the number
    m2 = re.match(r"[\s,]*(?:\([^)]{0,60}\)\s*)?(?:thousand|million|billion)?\s*"
                  r"(?:o\s*f\s+(?:the\s+|its\s+|registrant'?s?\s+|company'?s?\s+|issuer'?s?\s+)+)?"
                  r"(" + _CLASS_TAIL + r")", post, re.I)
    post_label = re.sub(r"\s+", " ", m2.group(1)).strip() \
        if m2 and class_designator(m2.group(1)) else ""
    if pm:
        before = pre[max(0, pm[-1].start() - 44):pm[-1].start()]
        attached = re.search(r"[\d,]{4,}\s*\)?\s*(?:(?:outstanding|issued)\s+)?(?:shares?\s+)?"
                             r"(?:o\s*f\s+)?(?:the\s+|its\s+|registrant'?s?\s+)*$", before)
        connective = re.search(r"(?:consisting\s+of|comprising|composed\s+of|"
                               r"including|as\s+follows)\s*:?\s*$",
                               pre[pm[-1].end():], re.I)
        pre_label = re.sub(r"\s+", " ", pm[-1].group(0)).strip()
        # a standalone pre label wins unless it is generic and a designator-
        # bearing label follows the number (anchor preambles end in "… common
        # stock as of the close of the period …" right before the real list)
        if not attached and not connective and \
           (class_designator(pre_label) or not post_label):
            return pre_label
    if post_label:
        return post_label
    if pm:
        return re.sub(r"\s+", " ", pm[-1].group(0)).strip()
    # last resort: any class keyword in either direction
    ctx = pre + " " + post
    for kw in ("ordinary shares", "common shares", "common stock", "preferred stock",
               "preference shares", "capital stock", "redeemable capital shares",
               "limited voting shares", "common units", "ordinary share",
               "common share", "units", "shares", "stock"):
        mm = re.search(re.escape(kw), ctx, re.I)
        if mm:
            return re.sub(r"\s+", " ", mm.group(0)).strip()
    return ""


# ---------------------------------------------------------------------------
# Extraction strategy A: regulatory anchor (20-F / 40-F covers)
# ---------------------------------------------------------------------------
# The fixed cover instruction on 20-F/40-F. Numbers follow it directly.
# Whitespace-flexible: foreign-issuer covers break this phrase across newlines,
# so every gap is \s+ rather than a literal space.
_ANCHOR_RE = re.compile(
    # variants seen in the corpus: "each of" omitted (some 40-Fs), "as at"
    # (Commonwealth English), "issuer ' s" (tag-boundary artifact), "classes of
    # capital or common shares"
    r"number\s+of\s+outstanding\s+shares\s+of\s+(?:each\s+of\s+)?the\s+issuer\s*'?\s*s?\s+"
    r"classes\s+of\s+(?:capital\s+stock\s+or\s+common|capital\s+or\s+common|capital|common)\s+(?:stock|shares)\s+as\s+(?:of|at)"
    r"[^:.]{0,200}?[:.]",  # newlines allowed: the date/colon often sits on the next line
    re.I,
)

# the "issued" phrasing some foreign covers use instead of the regulatory line:
# "The total number of issued shares of each class of stock … as of … was:"
_ANCHOR_ISSUED_RE = re.compile(
    r"(?:total\s+)?number\s+of\s+(?:issued|outstanding)\s+shares\s+of\s+each\s+class\s+of\s+"
    r"(?:capital\s+stock|stock|shares)\b[^:.]{0,200}?[:.]",
    re.I,
)


def extract_anchor(text, period="", date_filed=""):
    """For 20-F/40-F: find the fixed regulatory phrase, then read the class/number
    pairs that follow it until the next cover checkbox instruction. Handles both
    list shapes seen in practice:
        label-first : "Ordinary Shares ... : 1,228,504,232"  (SAP, Brookfield)
        number-first: "295,935,686 Common Shares 4,866,814 Series A Preferred ..."  (Emera, Cameco)
    `period` (CONFORMED PERIOD OF REPORT) is the as-of date when none is printed."""
    text = _repair_artifacts(text)
    m = _ANCHOR_RE.search(text) or _ANCHOR_ISSUED_RE.search(text)
    if not m:
        return []
    # the regulatory phrase often carries the authoritative as-of date itself
    # ("… as of January 28, 2025 was:") — that date, not the fiscal close,
    # is the fallback for entries with no inline date
    anchor_dates = _DATE_RE.findall(m.group(0))
    if anchor_dates:
        period = iso_date(anchor_dates[-1]) or period
    start = m.end()
    stop = re.search(r"Indicate by check mark|\bIf this report\b", text[start:start + 2000], re.I)
    span = text[start: start + (stop.start() if stop else 1600)]
    entries = _extract_span_pairs(_despace_numbers(span), period, date_filed)
    # a bare-designator listing label ('Series "D" Shares') is often defined
    # with its full class name in the 12(b) registered-securities table above
    # the anchor ('Dividend Preferred Shares, without par value ("Series "D"
    # Shares")') — the definition carries the share type the listing omits
    for en in entries:
        desig = class_designator(en.class_label)
        if desig and en.share_type in ("common", "ordinary") and \
                re.fullmatch(r"(?:series|class)\s+[\"']?[a-z0-9-]{1,3}[\"']?\s+shares?",
                             en.class_label, re.I):
            for dm in re.finditer(
                    r"\(\s*[\"'](?:the\s+)?(?:series|class)\s+[\"']?" + re.escape(desig) +
                    r"[\"']?\s+shares?\s*[\"']\s*\)", text[:m.start()], re.I):
                head = text[max(0, dm.start() - 80):dm.start()]
                fm = re.search(r"([A-Z][\w&'.-]*(?:\s+[\w&'.-]+){0,4}\s+(?:Shares?|Stock))"
                               r"\s*(?:,[^()]{0,40})?$", head)
                if fm:
                    t = classify_share_type(fm.group(1))
                    if t in ("preferred", "depositary", "other"):
                        en.share_type = t
                    break
    # an entry pinned to "the date of this report" — the sentinel, not an
    # explicit printed date — defers to an explicit dated statement of the
    # SAME count elsewhere on the cover ("As of March 31, 2025, there were
    # 26,400,000 ordinary shares" in the 15(d) section). A match inside the
    # anchor listing itself never counts (a date there belongs to a sibling
    # count in the same sentence), and an entry whose own raw_date already
    # reads date_filed printed the filing date explicitly — keep it.
    if date_filed:
        for en in entries:
            if en.as_of_date == date_filed and en.raw_number \
                    and iso_date(en.raw_date) != en.as_of_date:
                for dm in re.finditer(r"as\s+(?:of|at)\s+(" + _DATE_RE.pattern + r")[^.;]{0,80}?" +
                                      re.escape(en.raw_number), text, re.I):
                    if start <= dm.start() < start + len(span):
                        continue
                    en.as_of_date = iso_date(dm.group(1)) or en.as_of_date
                    break
    return entries


# A class label = up to ~5 words ending in a class noun, optional Series suffix.
# Case-insensitive so foreign filers' lowercase "ordinary shares" is captured too.
_LABEL_PHRASE_RE = re.compile(
    r"(?:(?:[A-Za-z][\w.&'/-]*|\"[A-Za-z0-9]{1,3}\"|'[A-Za-z0-9]{1,3}'|\d{1,2}(?:\.\d+)?%)\s+){0,7}?"
    r"(?:ordinary\s+shares?|common\s+shares?|common\s+stock|preferred\s+stock|"
    r"preference\s+shares?|preferred\s+shares?|capital\s+stock|"
    # comma-list voting class names ("Subordinate, Restricted and Limited
    # Voting Shares") — the comma form only, so plain labels bind unchanged
    r"redeemable\s+capital\s+shares?|"
    r"(?:[a-z]+,\s+)+(?:[a-z]+\s+and\s+)?(?:limited|subordinate|multiple)\s+voting\s+shares?|"
    r"limited\s+voting\s+shares?|"
    r"subordinate\s+voting\s+shares?|partnership\s+common\s+units?|"
    r"general\s+partner\s+units?|common\s+units?|deferred\s+shares?|"
    r"shares?|stock|units|CPOs?|ADSs?|ADRs?|GDRs?|BDRs?)(?:,?\s+(?:series|class)\s+[\"']?[A-Za-z0-9-]{1,3}[\"']?)?",
    re.I,
)
_LABEL_AT_START_RE = re.compile(
    r"^(?:\.\d+\s*)?[\s,:]*(?:\([^)]{0,60}\)\s*)?(?:without\s+(?:nominal|par)\s+value[,:]?\s*)?"
    r"(?:thousand|million|billion)?\s*"
    r"(?:shares?\s+of\s+(?:the\s+|its\s+|common\s+|registrant'?s?\s+|company'?s?\s+|issuer'?s?\s+)*)?"
    r"(" + _LABEL_PHRASE_RE.pattern + r")",
    re.I,
)


def _clean_label(s):
    return re.sub(r"\s+", " ", s).strip(" ,.:")


def _extract_span_pairs(span, period, date_filed=""):
    """Walk every share-count number in the anchor listing and bind a class label.
    Decide the list orientation once: if a class noun appears before the first
    number it is label-first (SAP / Brookfield / CIBC) and each number takes the
    nearest label before it; otherwise number-first (Cameco / Emera / BTC) and each
    number takes the label printed right after it."""
    nums = [n for n in _NUM_RE.finditer(span)]
    if not nums:
        return []
    label_first = bool(re.search(r"(?:shares?|stock|units)\b", span[:nums[0].start(1)], re.I))
    # decide which numbers are real counts FIRST, so a skipped decoy (e.g. the
    # "(post-reverse stock split adjusted to N)" parenthetical) never truncates
    # the span a kept number reads its label from
    kept = []
    for nm in nums:
        if _looks_like_year(nm.group(1), nm.group(2)):
            continue
        ns, ne = nm.start(1), nm.end()
        # % only when glued to the digits — "8,331,144,875 11% cumulative
        # preference shares" is a count followed by a dividend-rate label
        if "$" in span[max(0, ns - 5):ns] or "%" in span[nm.end(1):nm.end(1) + 1]:
            continue
        # a bare decimal with no scale word is a par value ("£1.00 each",
        # "NIS 1.0"), never a count; "45.0 million" keeps its scale word
        if "." in nm.group(1) and not nm.group(2):
            continue
        if _skip_number_context(span, ns, ne):
            continue
        val = _to_int(nm.group(1), nm.group(2))
        # no magnitude floor in the anchor span: it is a tightly bounded
        # regulatory listing where tiny real classes occur ("480 Class B
        # ordinary shares", "100 common shares"); decoys are guarded above
        if val is None or val < 1:
            continue
        kept.append((nm, val))
    entries = []
    for i, (nm, val) in enumerate(kept):
        ns, ne = nm.start(1), nm.end()
        next_start = kept[i + 1][0].start(1) if i + 1 < len(kept) else len(span)
        # a par/nominal-value descriptor ("nominal value Ps.1.00 per share",
        # "each share") matches the generic phrase tail but is never a class
        # label — it must not shadow the real "Class B ordinary shares" phrase
        # before it. A usable pre label needs a designator or a substantive
        # class keyword.
        ms = [m for m in _LABEL_PHRASE_RE.finditer(span[:ns])
              if not re.search(r"(?:par|nominal)\s+value|per\s+share|each\s+share",
                               m.group(0), re.I)
              and (class_designator(m.group(0)) or
                   re.search(r"ordinary|common|preferr|preference|capital|voting|"
                             r"deferred|partnership|investment|units?\b", m.group(0), re.I))]
        pre_label = ms[-1].group(0) if ms else ""
        am = _LABEL_AT_START_RE.match(span[ne:next_start])
        post_label = am.group(1) if am else ""
        # one safe per-number override of the global orientation guess: a
        # connective right before the number ("… Class A common shares and
        # 23,664,925 …", "… and (ii) 148,500,000 Class C …") proves the pre
        # phrase belongs to the PREVIOUS item — bind forward. Everything else
        # follows the list orientation (a backward tail like "per share" is
        # ambiguous: it ends a label-first row AND a number-first item's label).
        tail = span[max(0, ns - 26):ns].lower()
        pre_attached = False
        if ms:
            pstart = span[:ns].rfind(ms[-1].group(0))
            between = span[pstart + len(ms[-1].group(0)):ns]
            # the phrase belongs to the PREVIOUS sentence's number only when a
            # sentence boundary separates it from this number ("… 4,117,952,894
            # Series D-L Shares. 1,417,048,500 B Units …"); inside one table row
            # ("Class B shares 7,624") it is this number's own label
            if pstart > 0 and re.search(r"[\d,]{4,}[\s)]*$", span[max(0, pstart - 16):pstart]) \
                    and re.search(r"(?<!\d)\.\s", between):
                pre_attached = True
        if re.search(r"\b(?:and|was|were|had)\s*(?:\([ivx0-9]{1,4}\)\s*)?$", tail) \
                and post_label:
            label = post_label  # explicit adjacency binding — final
        elif pre_attached and post_label:
            label = post_label  # the pre phrase is the PREVIOUS number's label
        else:
            label = pre_label if label_first else post_label
            if not label:
                label = post_label if label_first else pre_label
            # only the orientation GUESS may be overridden: an explicit
            # Class/Series designator outranks a generic phrase — "… common
            # stock as of the close of the period … 2,239,234,372 Class A
            # ordinary shares and …" must bind "Class A", not the anchor
            # sentence's own "common stock"
            pre_far = not ms or (ns - (span[:ns].rfind(ms[-1].group(0)) +
                                       len(ms[-1].group(0)))) > 40
            if not class_designator(label) and class_designator(post_label) and pre_far:
                label = post_label
            elif not class_designator(label) and class_designator(pre_label):
                label = pre_label
        if not label and not re.search(r"shares?|stock|units|CPOs?|ADSs?", span[ne:next_start], re.I):
            continue
        # two consecutive classes can share one count (Televisa Series L and
        # Series D, RBC preferred series): a pre label identical to the one the
        # PREVIOUS entry took belongs to that entry — promote this number's own
        # following label so the two rows stay distinct
        if label_first and entries and label == entries[-1].class_label \
                and post_label and post_label != label:
            label = post_label
        m_ser = re.search(r"\b(series\s+[\"']?[A-Za-z0-9]{1,3}[\"']?)\s*$",
                          span[max(0, ns - 24):ns], re.I)
        if m_ser and m_ser.group(1).lower() not in label.lower():
            label = (label + " " + m_ser.group(1)).strip()
        label = _clean_label(label)
        raw_date = _nearest_date(span, ne, window=140)
        ad = iso_date(raw_date) or period
        # the count's OWN clause names its date after it: "N common shares
        # outstanding as of April 29, 2025)" — that beats a nearer date that
        # ends the previous statement
        post_clause = span[ne:ne + 160]
        # a decimal point ("par value $0.0001 per share") is not a clause end —
        # cutting there would hide the clause's own "as of <date>" tail
        pc_cut = re.search(r"[;)]|\.(?!\d)", post_clause)
        if pc_cut:
            post_clause = post_clause[:pc_cut.start()]
        m_aso = re.search(r"\bas\s+(?:of|at)\s+", post_clause, re.I)
        if m_aso:
            dm = _DATE_RE.match(post_clause[m_aso.end():])
            if dm:
                raw_date = dm.group(0)
                ad = iso_date(raw_date) or ad
        if date_filed:
            m_dot = re.search(r"as\s+of\s+the\s+date\s+of\s+this\b", post_clause, re.I)
            if m_dot and not _DATE_RE.search(post_clause[:m_dot.start()]):
                ad = date_filed
            else:
                back_sent = span[max(0, ns - 220):ns]
                bc = back_sent.rfind(". ")
                if bc != -1:
                    back_sent = back_sent[bc + 1:]
                if re.search(r"as\s+of\s+the\s+date\s+of\s+this\b", back_sent, re.I) \
                        and not _DATE_RE.search(back_sent):
                    ad = date_filed
        # a LATER-than-period count inside a parenthetical ("(79,482,768 as of
        # the date of this report)") or a "subsequent to …" sentence is a
        # supplemental update, not the regulatory period-close answer
        if period and ad and ad > period:
            back = span[max(0, ns - 240):ns]
            b = max(back.rfind(". "), back.rfind("\n"))
            if b != -1:
                back = back[b + 1:]
            if back.count("(") > back.count(")") or re.search(r"\bsubsequent\b", back, re.I):
                continue
        entries.append(ClassEntry(
            shares=val, raw_number=nm.group(1), scale=(nm.group(2) or "").lower(),
            class_label=label, share_type=classify_share_type(label),
            as_of_date=ad, raw_date=raw_date or period,
            matched_text=re.sub(r"\s+", " ", span[max(0, ns - 15):ne + 45]).strip(),
        ))
    # 1-2 digit and spelled-out tiny class counts never enter _NUM_RE ("12 Class
    # A Multiple Voting Shares", "2 series C shares", "one special share") —
    # pick them up only when tied to an explicit class noun
    for sm in re.finditer(
            r"(?<![\d,.$-])\b(\d{1,2}|one)\s+"
            r"((?:Class|Series)\s+[\"']?[A-Z0-9]{1,3}[\"']?\s+)?"
            r"((?:[A-Za-z]+\s+){0,3}?shares?)\b", span, re.I):
        whole = (sm.group(2) or "") + sm.group(3)
        if not class_designator(whole) and not re.search(r"\bspecial\b", whole, re.I):
            continue
        if _skip_number_context(span, sm.start(1), sm.end(1)):
            continue
        if re.search(r"\b(?:consisting\s+of|of)\s*$", span[max(0, sm.start() - 16):sm.start(1)], re.I):
            continue
        raw = sm.group(1)
        val = 1 if raw == "one" else int(raw)
        label = _clean_label(((sm.group(2) or "") + sm.group(3)).strip())
        raw_date = _nearest_date(span, sm.end(), window=140)
        entries.append(ClassEntry(
            shares=val, raw_number=raw, scale="",
            class_label=label, share_type=classify_share_type(label),
            as_of_date=iso_date(raw_date) or period, raw_date=raw_date or period,
            matched_text=re.sub(r"\s+", " ", span[max(0, sm.start() - 40):sm.end() + 40]).strip(),
        ))
    for sm in re.finditer(
            r"((?:Class|Series)\s+[\"']?[A-Z0-9]{1,3}[\"']?[\w ,]{0,44}?):\s*"
            r"(\d{1,3})\s+shares\s+outstanding", span, re.I):
        raw_date = _nearest_date(span, sm.end(), window=140)
        label = _clean_label(sm.group(1))
        entries.append(ClassEntry(
            shares=int(sm.group(2)), raw_number=sm.group(2), scale="",
            class_label=label, share_type=classify_share_type(label),
            as_of_date=iso_date(raw_date) or period, raw_date=raw_date or period,
            matched_text=re.sub(r"\s+", " ", sm.group(0)).strip(),
        ))
    return _dedupe(entries)


# ---------------------------------------------------------------------------
# Extraction strategy B: cover-window scan (10-K, and 20-F/40-F fallback)
# ---------------------------------------------------------------------------
def extract_cover_window(text, date_filed=""):
    """Scan every 'outstanding' in the cover region; for each, look for a nearby
    share-count number tied to a share word, skipping decoy contexts. Captures
    multiple share classes (e.g. Alphabet A/B/C) naturally."""
    entries = []
    text = _repair_artifacts(_despace_numbers(text))
    for om in re.finditer(r"outstanding|\bthere\s+(?:were|are)\b|\bregistrant\s+had\b",
                          text, re.I):
        opos = om.start()
        if om.group(0).lower() != "outstanding":
            # auxiliary anchor: the canonical "As of <date>, there were N shares
            # … outstanding" count sentence can soft-wrap across many lines,
            # pushing the early numbers out of reach of the "outstanding" token
            # itself. Only scan if this sentence does end in "outstanding"
            # (sentence end = period not inside a decimal).
            sent = text[opos:opos + 620]
            cut = re.search(r"(?<!\d)\.(?=\s)", sent)
            if cut:
                sent = sent[:cut.start()]
            if "outstanding" not in sent.lower():
                continue
        lo, hi = max(0, opos - 230), min(len(text), opos + 230)
        # extend the base window toward ±500, but only within the same sentence/
        # line: a long multi-class sentence keeps its early classes (Hines-style
        # NAV REITs, "the following outstanding shares … :" listings) while a
        # neighboring line (ticker codes, file numbers, the market-value
        # sentence) never bleeds in
        far_lo = max(0, opos - 500)
        seg = text[far_lo:lo]
        cut = max(seg.rfind(". "), seg.rfind("\n"))
        lo = far_lo + cut + 1 if cut != -1 else far_lo
        far_hi = min(len(text), opos + 500)
        m2 = re.search(r"\.\s|\n", text[hi:far_hi])
        hi = hi + m2.start() if m2 else far_hi
        window = text[lo:hi]
        low = window.lower()
        # need a share word in the window
        if not any(w in low for w in _SHARE_WORDS):
            continue
        for nm in _NUM_RE.finditer(window):
            n_start = lo + nm.start(1)
            n_end = lo + nm.end()
            # skip calendar years bleeding in from dates
            if _looks_like_year(nm.group(1), nm.group(2)):
                continue
            # a digit right after the match means we read a prefix of a longer
            # run ("March 28,2025" -> "28,202" + "5") — malformed, never a count
            if n_end < len(text) and text[n_end].isdigit():
                continue
            # skip dollar amounts ("$ 69,405,000" — note the space after $)
            if "$" in text[max(0, n_start - 5):n_start]:
                continue
            # skip percentages and obvious non-counts
            if "%" in text[n_end:n_end + 3]:
                continue
            tail = text[n_end:n_end + 8].lower()
            if tail.strip().startswith(("days", "%")):
                continue
            if _skip_number_context(text, n_start, n_end):
                continue
            num = _to_int(nm.group(1), nm.group(2))
            if num is None or not (PLAUSIBLE_MIN_SHARES <= num <= PLAUSIBLE_MAX_SHARES):
                continue
            # the number must sit close to a share word. Window is generous (100
            # chars back) because phrasings like "Number of <class> outstanding as
            # of <long date>: <number>" push the class word well before the number.
            near = text[max(0, n_start - 100):n_end + 70].lower()
            if not any(w in near for w in ("share", "stock", "common", "ordinary",
                                           "capital", "preferred", "units", "voting")):
                continue
            # tight decoy rejection — only signals that pertain to THIS number, so
            # we don't reject a real count just because a market-value or authorized
            # clause sits nearby (that was the cause of false negatives).
            post = text[n_end:n_end + 32].lower()
            pre = text[max(0, n_start - 28):n_start].lower()
            # "X [shares] authorized" — authorized capital, not the outstanding count
            if re.match(r"\s*(?:shares?\s+)?authorized", post) and \
               "outstanding" not in post and "issued" not in post:
                continue
            # "X record holders / holders of record" — a holder count, not shares
            if re.search(r"record holder|holders of record|"
                         r"(?:share|stock)holders of record", post):
                continue
            # weighted-average shares (EPS) — only if the cover region bled into it
            if "weighted" in pre:
                continue
            label = _grab_class_label(text, n_start, n_end)
            # primary window 240; widen only when empty so a long multi-class
            # sentence still reaches its single leading "As of <date>" anchor
            # without letting decoy dates into the ordinary case
            raw_date = _nearest_date(text, (n_start + n_end) // 2, window=240)
            if not raw_date:
                raw_date = _nearest_date(text, (n_start + n_end) // 2, window=420)
            # "As of D1 and D2, there were N1 and N2 …": pair the k-th number
            # with the k-th date, not the nearest one
            dpat = rf"(?:{_MONTHS})\s+\d{{1,2}}\s*,?\s+\d{{4}}"
            dual = re.search(rf"as\s+of\s+({dpat})\s+and\s+({dpat})[\s,]+there\s+were\s+"
                             rf"([\d,]+\s+and\s+)?$",
                             re.sub(r"\s+", " ", text[max(0, n_start - 140):n_start]), re.I)
            if dual:
                raw_date = dual.group(2) if dual.group(3) else dual.group(1)
            ad = iso_date(raw_date)
            # "As of the date of this Annual Report, the registrant had N …" —
            # the sentence's own sentinel beats a stale date inherited from the
            # previous sentence (sentinel sentences print no real date at all)
            if date_filed:
                back_sent = text[max(0, n_start - 220):n_start]
                bc = back_sent.rfind(". ")
                if bc != -1:
                    back_sent = back_sent[bc + 1:]
                if re.search(r"as\s+(?:of|at)\s+the\s+date\s+of\s+this\b", back_sent, re.I) \
                        and not _DATE_RE.search(back_sent):
                    ad = date_filed
            entries.append(ClassEntry(
                shares=num, raw_number=nm.group(1), scale=(nm.group(2) or "").lower(),
                class_label=label, share_type=classify_share_type(label) if label else "common",
                as_of_date=ad, raw_date=raw_date,
                matched_text=re.sub(r"\s+", " ", text[max(0, n_start - 80):n_end + 50]).strip(),
            ))
    # tiny named-class counts ("… and 1 Class B ordinary share issued and
    # outstanding") fall below the number-regex/plausibility floor; pick them up
    # only when explicitly tied to a class phrase in an outstanding sentence
    for sm in re.finditer(r"(?<![\d,.$-])(\d{1,2})\s+(Class\s+[A-Z]\s+)"
                          r"(ordinary|common)\s+shares?\b", text, re.I):
        fwd = text[sm.end():sm.end() + 160]
        cut = re.search(r"\.\s|\n", fwd)
        if cut:
            fwd = fwd[:cut.start()]
        if not re.search(r"\bissued\s+and\s+outstanding|\boutstanding\b", fwd, re.I):
            continue
        label = f"{sm.group(2).strip()} {sm.group(3)} shares"
        raw_date = _nearest_date(text, (sm.start(1) + sm.end(1)) // 2, window=400)
        entries.append(ClassEntry(
            shares=int(sm.group(1)), raw_number=sm.group(1), scale="",
            class_label=label, share_type=classify_share_type(label),
            as_of_date=iso_date(raw_date), raw_date=raw_date,
            matched_text=re.sub(r"\s+", " ", text[max(0, sm.start() - 60):sm.end() + 60]).strip(),
        ))
    return _dedupe(entries)


def _skip_number_context(text, ns, ne):
    """Reject numbers that are not a standalone OUTSTANDING class count: a grand
    total that is then broken into its component classes, the 'issued' half of an
    'X issued and Y outstanding' pair, a parenthetical SUBSET or restatement
    ("(including 335,787,795 … ADS)", "(post-reverse-split adjusted to N)"), a
    treasury or warrant count, or a non-affiliate market-value figure."""
    pre = text[max(0, ns - 34):ns].lower()
    post = text[ne:ne + 64].lower()

    # subset / restatement: a portion of, or a re-expression of, another count.
    # Exception: a number introduced by "including" that is itself a named CLASS
    # component ("...including 84,463,737 Class A ... and 45,787,948 Class B") is a
    # real component, not a subset -> keep it (the total before it is dropped below).
    # "excluding / of which / representing / form of ..." ALWAYS marks a subset or a
    # re-expression of another count -> drop it, even when it names a class
    # ("excluding 277,628,320 Class A ordinary shares ... reserved for ADS issuance").
    if re.search(r"\b(?:excludes|excluding|exclusive\s+of|of which|representing|represented\s+by|reserved|"
                 r"equivalent\s+to|adjusted\s+to|in\s+excess\s+of|(?:the\s+)?form\s+of)\b"
                 r"[\s()\divx.]{0,10}$", pre):
        return True
    # "excluding treasury shares of N", "excludes the market value of N" — the
    # qualifier may sit a few words before the number; a comma/paren/period ends
    # the exclusion clause so a real count after it is not swallowed
    if re.search(r"\bexclud(?:es|ing)\b[^.;,)]{0,55}$", text[max(0, ns - 70):ns], re.I):
        return True
    # a number deep inside a still-open "(excluding … N …)" parenthetical is part
    # of the exclusion, however long the clause runs
    back = text[max(0, ns - 420):ns]
    depth, op = 0, -1
    for i in range(len(back) - 1, -1, -1):
        if back[i] == ")":
            depth += 1
        elif back[i] == "(":
            if depth == 0:
                op = i
                break
            depth -= 1
    if op != -1 and re.search(r"\bexclud(?:ing|es)\b|\bexclusive\s+of\b",
                              back[op:op + 30], re.I):
        return True
    # "including N ..." / "(which includes N ...)" is a subset UNLESS N is itself a
    # named class component of the total that precedes it
    # ("...including 84,463,737 Class A ... and 45,787,948 Class B"). A class
    # phrase qualified as tendered/reserved/unvested/... is still a subset — but a
    # qualifier INSIDE a counting parenthetical ("Class A ... (excluding treasury
    # shares ... reserved for future issuance)") belongs to the note, not the class.
    if re.search(r"\binclud(?:ing|es)\b[\s(]*$", pre) \
       and not (re.match(r"\s*[a-z'.&\- ]*?class\s+[a-z0-9]", post)
                and not re.search(r"\b(?:tender|redeem|redempt|reserved|escrow|"
                                  r"forfeit|unvested|underlying|issuable)",
                                  text[ne:ne + 90].split("(")[0].lower())):
        return True
    # "does not include N ..." explicitly marks the number as outside the count
    if re.search(r"\b(?:does\s+not|do\s+not|not)\s+includ(?:e|ed|ing)\b[^.;,]{0,30}$",
                 text[max(0, ns - 45):ns], re.I):
        return True
    # "N shares ... issuable upon ..." — issuable shares are never outstanding
    if re.search(r"^\s*(?:[a-z'.&\- ]{0,30})?shares?\b[^.;]{0,45}\bissuable\b",
                 text[ne:ne + 90], re.I):
        return True
    # "The B Units represent a total of N Series B Shares, M Series D-B Shares
    # and M Series D-L Shares" — every number in that enumeration is a
    # re-expression of the unit count
    rt = re.search(r"\brepresents?\s+a\s+total\s+of\b", text[max(0, ns - 150):ns], re.I)
    if rt and not re.search(r"[.;]", text[max(0, ns - 150) + rt.end():ns]):
        return True
    # the denominator of a split ratio ("1-for-4,000 reverse stock split")
    if re.search(r"\d\s*-\s*for\s*-\s*$", text[max(0, ns - 12):ns], re.I):
        return True
    # regulatory citations ("Rule 405 of the Securities Act", "§ 232.405",
    # "Section 13") are never counts
    if re.search(r"(?:\brule|\bsection|\bitem|\bform|§)\s*$", text[max(0, ns - 10):ns], re.I):
        return True
    # market-value computation figures: "based on N shares", "N shares at $X per
    # share", "N at a closing price of $X" — float math, not the cover count
    if re.search(r"\bbased\s+(?:up)?on(?:\s+the)?\s*$", text[max(0, ns - 22):ns], re.I):
        return True
    if re.search(r"^\s*(?:shares?\s+)?at\s+\$[\d,.]+\s+per\s+share|"
                 r"^\s*(?:shares?\s+)?at\s+a\s+(?:closing\s+|sale\s+)?price\b",
                 text[ne:ne + 60], re.I):
        return True
    # "N shares outstanding ... , or M in the aggregate" — N is a net subset
    # superseded by the aggregate count M that follows
    if re.search(r"\bor\s+[\d,]+(?:\s+\w+){0,4}\s+in\s+the\s+aggregate\b",
                 text[ne:ne + 110], re.I):
        return True
    # "issued and outstanding N1 and N2, respectively" — N1 is the issued half
    if re.search(r"\bissued\s+and\s+outstanding[:\s]*$", text[max(0, ns - 60):ns], re.I) and \
       re.match(r"\s*and\s+[\d,]+[^.;]{0,80}?\brespectively\b", text[ne:ne + 130], re.I):
        return True
    # "there were N1 and N2 ... shares ... issued and outstanding, respectively"
    # (or "... N1 and N2, respectively, shares ... issued and outstanding") —
    # the current number, followed directly by a sibling number, is the issued
    # half. Scan the rest of the number's own sentence, ignoring decimal points
    # (par values) when finding the sentence end.
    if re.match(r"\s*and\s+[\d,]+\b", post):
        fwd = text[ne:ne + 220]
        cut = re.search(r"(?<!\d)\.(?=\s)", fwd)
        if cut:
            fwd = fwd[:cut.start()]
        low_fwd = fwd.lower()
        back200 = text[max(0, ns - 200):ns].lower()
        # sentence boundary = ". " only: a soft-wrap newline mid-sentence
        # ("… shares of each of the\nissuer's classes …") is not a boundary
        b = back200.rfind(". ")
        back_sent = back200[b + 1:] if b != -1 else back200
        if "respectively" in low_fwd and \
           (("issued" in low_fwd and "outstanding" in low_fwd) or
            re.search(r"issued\s+and\s+outstanding", back_sent)):
            return True
    if re.search(r"(?:retroactively|post-?reverse|after\s+giving\s+effect|"
                 r"as\s+adjusted|giving\s+retroactive)\b", pre):
        return True
    # pro-forma / restatement qualifiers farther from the number: "After giving
    # effect to the Business Combination … the issuer had N …", "(N1 and N2 …
    # if retroactively adjusted to reflect the consolidation)", "(… following
    # the 1-for-10 reverse share split)"
    back_ge = text[max(0, ns - 300):ns]
    bg = back_ge.rfind(". ")
    if bg != -1:
        back_ge = back_ge[bg + 1:]
    if re.search(r"\bafter\s+giving\s+effect\b|\bgiving\s+effect\s+to\b", back_ge, re.I):
        return True
    # … but only for a number INSIDE the restatement parenthetical ("(there
    # were N1 … if retroactively adjusted …)"). A real count followed by such a
    # parenthetical ("2,370,139 ordinary shares … (retroactively adjusted to
    # reflect the consolidation)") IS the adjusted, correct count — keep it.
    back240 = text[max(0, ns - 240):ns]
    if back240.count("(") > back240.count(")"):
        fwd300 = text[ne:ne + 300]
        cut = re.search(r"(?<!\d)\.(?=\s)", fwd300)
        if cut:
            fwd300 = fwd300[:cut.start()]
        if re.search(r"\bif\s+retroactively\s+adjusted\b|\bretroactively\s+adjusted\s+to\s+reflect\b|"
                     r"following\s+(?:the\s+)?\S{1,12}\s+reverse\s+(?:share|stock)\s+split\s*\)|"
                     r"\b(?:adjusted|consolidat|split)\w*\b",
                     fwd300, re.I) and \
           re.search(r"\bretroactiv|\breverse\b|\bconsolidat|\bsplit\b|\badjusted\b",
                     fwd300, re.I):
            return True
    # any number sitting inside an "of which ..." sub-clause (same sentence) is a
    # breakdown of the count before it, not a separate class -> drop it. A "." or a
    # ")" between the "of which" and the number ends the sub-clause (e.g. a real
    # class listed after a closed parenthetical), so don't reach across it.
    ofw = re.search(r"\bof which\b", text[max(0, ns - 160):ns], re.I)
    if ofw:
        between = text[max(0, ns - 160) + ofw.end():ns]
        if "." not in between and ")" not in between:
            return True

    # grand total immediately broken into its component classes -> keep components, drop total.
    # The connective ("consisting of"/"being the sum of"/"with A Class A and B
    # Class B"…) may sit past a long class descriptor and par-value clause, so
    # scan the rest of the number's own sentence (decimal points in par values
    # are not sentence ends). Require an actual large component count after the
    # connective: "B Units, each consisting of five Series B Shares" describes
    # unit COMPOSITION, not a redundant total.
    fwd_tot = text[ne:ne + 175]
    cut = re.search(r"(?<!\d)\.(?=\s)", fwd_tot)
    if cut:
        fwd_tot = fwd_tot[:cut.start()]
    m_conn = re.search(r"\b(?:consisting\s+of|comprised\s+of|comprising|composed\s+of|"
                       r"being\s+the\s+sum\s+of|made\s+up\s+of|the\s+sum\s+of|"
                       r"divided\s+into|"
                       r"(?<!each )with(?!\s+(?:a\s+|no\s+)?(?:par|nominal)\b))\b",
                       fwd_tot, re.I)
    # the component count must live in the SAME sentence as the connective
    # ("each consisting of five Series B Shares." + a count in the NEXT
    # sentence is unit composition, not a breakdown)
    if m_conn and re.search(r"(?<![\d.$])(?:\d{1,3}(?:,\d{3})+|\d{5,})",
                            fwd_tot[m_conn.end():]):
        return True
    # "X shares, including A Class A ... and B Class B" -> X is the total, drop
    # it — but only when the included count is a substantial component (>= 15%
    # of X): "including 1,692 Class A shares held by the depositary" is a
    # footnote subset of a REAL class count, not a breakdown
    m_inc = re.search(r"^[^;]{0,90}?\bincluding\s+(\d[\d,]*)\s*"
                      r"(?:thousand|million|billion)?\s*[a-z'.&\- ]*?class\s+[a-z0-9]",
                      fwd_tot, re.I)
    if m_inc:
        cur = _to_int(re.sub(r"[^\d,]", "", text[ns:ne]) or "0", None)
        inc = _to_int(m_inc.group(1), None)
        qual = re.search(r"\b(?:tender|redeem|redempt|reserved|escrow|forfeit|"
                         r"unvested|underlying|issuable|repurchas)",
                         fwd_tot[m_inc.end():m_inc.end() + 60].split("(")[0], re.I)
        if cur and inc and inc >= 0.15 * cur and not qual:
            return True

    # 'X [shares of the registrant's common stock] issued and Y outstanding' ->
    # X is the issued count, not the answer; the class descriptor between the
    # number and "issued" can run several words
    post80 = text[ne:ne + 80].lower()
    if re.search(r"\bissued,?\s+and\s+(?:approximately\s+)?[\d(]", post80) and \
       not re.search(r"\bissued,?\s+and\s+outstanding", post80):
        return True

    # treasury ("N shares held in treasury", "N recorded as treasury stock")
    if re.match(r"\s*(?:class\s+[a-z]\s+)?(?:ordinary\s+|common\s+)?shares?\s+held\s+in\s+treasury", post):
        return True
    if re.match(r"\s+(?:were\s+)?treasury\b", post) or "in treasury" in post[:34] or \
       (re.search(r"\btreasury\s+(?:stock|shares?)\b", post[:40]) and
        not re.search(r"\b(?:net\s+of|excluding|exclusive\s+of|less)\b[^.;]{0,20}treasury",
                      post[:40])):
        return True

    # "a total of N Class A ordinary shares … that have been repurchased" —
    # a buyback subtotal inside a counting note
    if re.search(r"^[^;()]{0,110}?\brepurchas", text[ne:ne + 130], re.I):
        return True
    # a "Total" table-row label right before the number is an arithmetic sum
    # row ("Total First Preferred Shares 141,604,079"); "a total of N" prose is
    # NOT a row label
    if re.search(r"\b(?:sub)?total\b(?!\s+of)[\w\s]{0,32}$", text[max(0, ns - 44):ns], re.I):
        return True

    # "N per share" is a par value or price, never a count ("par value ￦ 5,000
    # per share" — non-USD currency symbols escape the $ guard)
    if re.match(r"\s*per\s+share\b", post):
        return True

    # warrants / options to purchase are never the outstanding share count.
    # Backward: reach a "Warrants to purchase …: N" row label, but never cross
    # into the PREVIOUS row/sentence (a "Warrants" line above a real share row
    # must not kill it). Forward: "N warrants outstanding/to purchase" is a
    # warrant count, but "N Warrants 14,391,150" is the NEXT row's label-first
    # pair — the digit after the warrant word marks that case.
    pre_row = text[max(0, ns - 90):ns]
    b = pre_row.rfind(". ")
    if b != -1:
        pre_row = pre_row[b + 1:]
    if re.search(r"\bwarrants?\b|\boptions?\s+to\s+purchase\b", pre_row.lower()):
        return True
    if re.match(r"\s*(?:[a-z'.&\- ]{0,20})?warrants?\b(?!\s*[\d(])", post):
        return True

    # share repurchase / buyback / tender mentions ("accepted for purchase a total of
    # approximately 56.6 million shares") are not the outstanding count. Guard on ")":
    # a buyback word inside a closed parenthetical must not skip a real class that
    # follows it ("...shares repurchased but not yet cancelled) and 322,483,772 Class B").
    pre48 = text[max(0, ns - 48):ns].lower()
    if ")" not in pre48 and re.search(
            r"repurchas|buy-?back|accepted\s+for\s+purchase|for\s+purchase\s+a\s+total|"
            r"tender(?:ed|\s+offer)|purchased\s+a\s+total", pre48):
        return True

    # non-affiliate market-value / public-float figures: a count whose own
    # SENTENCE ties it to "non-affiliates" nearby is a float or denominator
    # number, never the cover outstanding count ("there were N shares
    # outstanding held by non-affiliates, and the aggregate market value was
    # $X"). Bounded both by the sentence and by ±130 chars: flattened cover
    # TABLES run boundary-less, so the market-value row above must not reach
    # the real count in the row below.
    s_lo = max(0, ns - 130)
    seg = text[s_lo:ns]
    b = max(seg.rfind(". "), seg.rfind("\n"))
    sent_start = s_lo + b + 1 if b != -1 else s_lo
    fwd = text[ne:ne + 130]
    m_end = re.search(r"\.\s|\n", fwd)
    sent_end = ne + (m_end.start() if m_end else 130)
    if re.search(r"non-?\s?affiliate", text[sent_start:sent_end], re.I):
        return True

    return False


def _filter_10k_recency(entries, date_filed):
    """A 10-K cover share count carries a recent practicable date (near filing).
    If any extracted entry is dated within ~1 year of the filing, drop the ones
    that are undated or far older — those are narrative figures (past mergers,
    reverse splits, predecessor entities), not the current outstanding count."""
    def recent(e):
        return bool(e.as_of_date) and _days_between(e.as_of_date, date_filed) <= 366
    if any(recent(e) for e in entries):
        return [e for e in entries if recent(e)]
    return entries


def _to_int(num_str, scale_word):
    # float() so decimal-with-scale-word counts ("45.0 million", "5.5 billion")
    # parse; share counts are well within float64's exact-integer range (< 2^53).
    try:
        base = float(num_str.replace(",", "").replace(" ", ""))
    except ValueError:
        return None
    return int(round(base * SCALE_FACTOR.get((scale_word or "").lower(), 1)))


def _dedupe(entries):
    # class_label is part of the key so distinct preferred series that happen to
    # have the same share count (e.g. Emera Series F and Series J, both 8,000,000)
    # are not collapsed into one.
    seen, out = set(), []
    for e in entries:
        key = (e.shares, e.share_type, e.as_of_date, e.class_label.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


# ---------------------------------------------------------------------------
# XBRL cross-check (independent ground truth from SEC's structured data)
# ---------------------------------------------------------------------------
def fetch_xbrl_shares(session, cik, use_cache=True):
    """dei:EntityCommonStockSharesOutstanding from the companyconcept API.
    Present for most domestic 10-K filers; often absent / per-class for
    multi-class & foreign filers (then we rely on prose + the validator).
    Cached by CIK (a 404/empty result is cached too, so we don't re-hit)."""
    if use_cache:
        cached = _read_xbrl_cache(cik)
        if cached is not None:
            return cached
    cik10 = str(int(cik)).zfill(10)
    url = (f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik10}/dei/"
           f"EntityCommonStockSharesOutstanding.json")
    try:
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            if use_cache:
                _write_xbrl_cache(cik, [])
            return []
        data = r.json()
    except Exception:
        return []
    finally:
        _throttle()
    vals = []
    for unit_vals in data.get("units", {}).values():
        for v in unit_vals:
            vals.append({"val": v.get("val"), "end": v.get("end"),
                         "form": v.get("form"), "fy": v.get("fy"), "fp": v.get("fp")})
    if use_cache:
        _write_xbrl_cache(cik, vals)
    return vals


# ---------------------------------------------------------------------------
# Confidence + flags
# ---------------------------------------------------------------------------
def score_and_flag(ex: Extraction):
    flags, conf = list(ex.flags), 0.0  # keep flags already set during fetch/parse
    filing_year = int(ex.filing.date_filed[:4]) if ex.filing.date_filed[:4].isdigit() else None

    if not ex.entries:
        flags.append("NO_MATCH")
        if ex.xbrl_values:
            flags.append("XBRL_FOUND_BUT_NO_PROSE")  # near-certain false negative
        ex.flags, ex.confidence = flags, 0.0
        return ex

    # method credit
    conf += 0.45 if ex.method == "anchor" else 0.30

    # date plausibility (relative to filing year)
    dated = [e for e in ex.entries if e.as_of_date]
    if not dated:
        flags.append("NO_DATE")
    else:
        conf += 0.20
        if filing_year:
            yrs = {int(e.as_of_date[:4]) for e in dated}
            if any(not (filing_year - 2 <= y <= filing_year + 1) for y in yrs):
                flags.append("DATE_IMPLAUSIBLE")
                conf -= 0.20

    # magnitude plausibility
    if all(PLAUSIBLE_MIN_SHARES <= (e.shares or 0) <= PLAUSIBLE_MAX_SHARES for e in ex.entries):
        conf += 0.10
    else:
        flags.append("NUMBER_OUT_OF_RANGE")

    # class identified?
    if any(e.class_label for e in ex.entries):
        conf += 0.10
    else:
        flags.append("CLASS_UNLABELED")

    if any(e.scale for e in ex.entries):
        flags.append("SCALE_WORD_USED")  # "X million" - benign but worth noting
    if len({e.share_type for e in ex.entries if e.share_type in ("common", "ordinary")}) and \
       len(ex.entries) > 1:
        flags.append("MULTI_CLASS")

    # XBRL cross-check on the common/ordinary total
    if ex.xbrl_values:
        xbrl_match = _xbrl_agreement(ex, filing_year)
        if xbrl_match is True:
            conf += 0.20
            flags.append("XBRL_MATCH")
        elif xbrl_match is False:
            conf -= 0.30
            flags.append("XBRL_MISMATCH")
        # None -> no comparable period; no change
    else:
        flags.append("NO_XBRL")  # foreign/multi-class; rely on prose + validator

    ex.flags = flags
    ex.confidence = round(max(0.0, min(1.0, conf)), 3)
    return ex


def _xbrl_agreement(ex, filing_year, tol=0.005):
    """Cross-check prose numbers against the dei:EntityCommonStockSharesOutstanding
    fact(s). The dei fact reported on THIS filing equals the cover number, so the
    strongest signal is exact membership. Returns True/False/None.

    True  -> a prose entry (or the common/ordinary sum) equals/≈ a recent dei value
    False -> we have a recent dei value for this form but nothing matches it
    None  -> no comparable dei value (don't penalise; e.g. foreign/multi-class)"""
    # Only compare against a CONTEMPORANEOUS dei value (period end within ~400d of
    # the filing). If the filer stopped tagging the fact years ago (common for
    # delisted issuers / LPs), we can't confirm — return None rather than flag a
    # false mismatch against a stale number.
    fd = ex.filing.date_filed
    same_form = [v for v in ex.xbrl_values
                 if v.get("val") and v.get("form", "").split("/")[0] == ex.filing.form]
    recent = [v for v in same_form if _days_between(v.get("end", ""), fd) <= 400]
    if not recent:
        recent = [v for v in ex.xbrl_values
                  if v.get("val") and _days_between(v.get("end", ""), fd) <= 400]
    pool = recent
    if not pool:
        return None
    vals = {v["val"] for v in pool}
    prose_nums = [e.shares for e in ex.entries if e.shares]
    prose_common = sum(e.shares for e in ex.entries
                       if e.share_type in ("common", "ordinary") and e.shares)
    prose_any = sum(prose_nums)
    # exact membership (the common case: the cover number IS a dei fact)
    if any(n in vals for n in prose_nums):
        return True
    # tolerant match of the summed common/ordinary (multi-class rollups)
    for total in (prose_common, prose_any):
        if total and any(abs(total - xv) <= tol * xv for xv in vals):
            return True
    return False


def _days_between(a, b):
    import datetime as _dt
    try:
        da = _dt.date.fromisoformat(a[:10])
        db = _dt.date.fromisoformat(b[:10])
        return abs((da - db).days)
    except Exception:
        return 10 ** 6


# ---------------------------------------------------------------------------
# Top-level: process one filing
# ---------------------------------------------------------------------------
def process_filing(session, filing, do_xbrl=True):
    ex = Extraction(filing=filing)
    try:
        doc_type, text, period = get_clean_document(session, filing)
        ex.doc_type = doc_type
        ex.period_of_report = period
        ex.text_len = len(text)
        if not text:
            ex.error = "empty primary document"
            ex.flags = ["NO_DOCUMENT"]
            return ex
        if not re.search(r"FORM\s+(10-K|20-F|40-F)", text[:3000], re.I) and \
           not _ANCHOR_RE.search(text[:8000]) and "outstanding" not in text[:8000].lower():
            ex.flags.append("NO_COVER_MARKERS")

        cover = cover_region(text)
        # combined filings (e.g. Exelon + ComEd + PECO ...) list several registrants
        if re.search(r"number of shares[^.\n]{0,60}each registrant", cover, re.I):
            ex.flags.append("MULTI_REGISTRANT")
        if filing.form in ("20-F", "40-F"):
            entries = extract_anchor(text, period, filing.date_filed)
            ex.method = "anchor" if entries else ""
            if not entries:
                entries = extract_cover_window(cover, filing.date_filed)
                ex.method = "cover_window" if entries else "none"
        else:  # 10-K
            entries = extract_cover_window(cover)
            ex.method = "cover_window" if entries else "none"
            if not entries:  # last resort: anchor anywhere (rare combined forms)
                entries = extract_anchor(text, period, filing.date_filed)
                ex.method = "anchor" if entries else "none"

        # 20-F / 40-F: the as-of date is the fiscal close; fill it from the SGML
        # header when the cover didn't print one inline.
        if filing.form in ("20-F", "40-F") and period:
            for e in entries:
                if not e.as_of_date:
                    e.as_of_date = period
            # multi-year 20-Fs list historical year-end counts beside the
            # current one: when a share type has its period-close count, the
            # older-dated rows of that type are history, not extra classes
            have_period = {e.share_type for e in entries if e.as_of_date == period}
            entries = [e for e in entries
                       if not (e.share_type in have_period and e.as_of_date
                               and e.as_of_date < period)]
        # 10-K: drop narrative decoys (merger/reverse-split share counts deep in an
        # over-long cover) — keep only counts dated within ~1 year of the filing,
        # provided at least one such recent count exists.
        if filing.form == "10-K":
            entries = _filter_10k_recency(entries, filing.date_filed)
            # dual-date covers ("As of <fiscal close> and <practicable date>,
            # there were N1 and N2 …") and market-value reference counts carry
            # an older date than the real count: keep only the latest-dated
            # entries. Safe corpus-wide — no audited filing states two classes
            # with different as-of dates.
            dts = sorted({e.as_of_date for e in entries if e.as_of_date})
            if len(dts) > 1:
                entries = [e for e in entries if e.as_of_date == dts[-1]]
            # combined multi-registrant filings (Exelon + ComEd …) list each
            # co-registrant's count; the lead registrant's is the largest
            if "MULTI_REGISTRANT" in ex.flags and len(entries) > 1:
                best = {}
                for e in entries:
                    k = (e.share_type, e.as_of_date)
                    if k not in best or e.shares > best[k].shares:
                        best[k] = e
                entries = list(best.values())
        # an entry exactly equal to the sum of two or more sibling entries of
        # the same broad type is a redundant aggregate total ("23,359,000 …"
        # then "8,388,012 Class A + 14,970,988 Class B") — keep the components
        if len(entries) >= 3:
            import itertools
            EQ = {"common", "ordinary"}
            for cand in list(entries):
                # only common/ordinary totals, and only when the candidate is
                # the largest of its type — bank covers list many round-number
                # preferred series whose sums collide by arithmetic accident
                if cand.share_type not in EQ:
                    continue
                sibs = [e for e in entries if e is not cand and e.share_type in EQ]
                if any(e.shares >= cand.shares for e in sibs):
                    continue
                hit = False
                for r in (2, 3, 4):
                    if hit or len(sibs) < r:
                        continue
                    for combo in itertools.combinations(sibs, r):
                        if sum(e.shares for e in combo) == cand.shares:
                            hit = True
                            break
                if hit:
                    entries = [e for e in entries if e is not cand]
        if filing.form in ("20-F", "40-F") and period:
            EQb = {"common", "ordinary"}
            broad = lambda t: "E" if t in EQb else t
            has_period = {broad(e.share_type) for e in entries if e.as_of_date == period}
            entries = [e for e in entries
                       if not (e.as_of_date and e.as_of_date > period
                               and broad(e.share_type) in has_period)]
        ex.entries = entries

        if do_xbrl:
            ex.xbrl_values = fetch_xbrl_shares(session, filing.cik)
    except Exception as e:  # network or parse failure
        ex.error = f"{type(e).__name__}: {e}"
        ex.flags.append("FETCH_ERROR")
        return ex

    return score_and_flag(ex)


def extraction_to_rows(ex: Extraction):
    """Flatten an Extraction into one CSV row per class entry (or one row with
    blanks if nothing was extracted, so misses are visible in the output)."""
    base = {
        "cik": ex.filing.cik, "company": ex.filing.company, "form": ex.filing.form,
        "date_filed": ex.filing.date_filed, "accession": ex.filing.accession,
        "method": ex.method, "doc_type": ex.doc_type, "confidence": ex.confidence,
        "flags": ";".join(ex.flags), "xbrl_n": len(ex.xbrl_values),
        "txt_url": ex.filing.txt_url, "error": ex.error,
    }
    if not ex.entries:
        return [{**base, "shares": "", "raw_number": "", "scale": "",
                 "class_label": "", "share_type": "", "as_of_date": "",
                 "matched_text": ""}]
    rows = []
    for e in ex.entries:
        rows.append({**base, "shares": e.shares, "raw_number": e.raw_number,
                     "scale": e.scale, "class_label": e.class_label,
                     "share_type": e.share_type, "as_of_date": e.as_of_date,
                     "matched_text": e.matched_text})
    return rows
