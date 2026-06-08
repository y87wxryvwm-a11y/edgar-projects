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
    rf"\b(?:{_MONTHS})\s+\d{{1,2}},?\s+\d{{4}}\b"
    rf"|\b\d{{1,2}}\s+(?:{_MONTHS})\s+\d{{4}}\b"
    rf"|\b\d{{4}}-\d{{2}}-\d{{2}}\b",
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
    r"(?<![\$.\d])(\d{1,3}(?:,\d{3})+|\d+\.\d+|\d{3,})\s*(thousand|million|billion|thousands|millions|billions)?",
    re.I,
)


# A run is space-grouped thousands only if it is NOT also comma-grouped: a trailing
# comma/digit means the space joined a label number to a real count ("Series 27 850,000").
_SPACE_GROUPED_RE = re.compile(r"(?<![\d.])\d{1,3}(?: \d{3})+(?![,\d])")


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
    m = re.match(rf"({_MONTHS})\s+(\d{{1,2}}),?\s+(\d{{4}})", raw, re.I)
    if m:
        mo = _MONTH_NUM[m.group(1).lower()]
        return f"{int(m.group(3)):04d}-{mo:02d}-{int(m.group(2)):02d}"
    m = re.match(rf"(\d{{1,2}})\s+({_MONTHS})\s+(\d{{4}})", raw, re.I)
    if m:
        mo = _MONTH_NUM[m.group(2).lower()]
        return f"{int(m.group(3)):04d}-{mo:02d}-{int(m.group(1)):02d}"
    return ""


def classify_share_type(label):
    low = label.lower()
    if "preferred" in low or "preference" in low:
        return "preferred"
    if "depositary" in low or "depository" in low or " ads" in low or low.startswith("ads"):
        return "depositary"
    # units of beneficial interest / LP / trust units are not common stock
    if "unit" in low:
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
    """Date closest to pos; prefer one introduced by 'as of' within the window."""
    lo, hi = max(0, pos - window), min(len(text), pos + window)
    span = text[lo:hi]
    best, best_d = "", 10 ** 9
    for m in _DATE_RE.finditer(span):
        center = lo + (m.start() + m.end()) // 2
        d = abs(center - pos)
        pre = span[max(0, m.start() - 8):m.start()].lower()
        if "as of" in pre or "as at" in pre:
            d -= 50  # bias toward "as of <date>"
        if d < best_d:
            best, best_d = m.group(0), d
    return best


_CLASS_TAIL = (r"(?:Class\s+[A-Z]\b[\w ]*?)?(?:Common\s+Stock|Common\s+Shares|"
               r"Ordinary\s+Shares|Preferred\s+Stock|Preference\s+Shares|"
               r"Redeemable\s+Capital\s+Shares|Limited\s+Voting\s+Shares|"
               r"Subordinate\s+Voting\s+Shares|Capital\s+Stock|Common\s+Units|"
               r"Stock|Shares|Units)")


def _grab_class_label(text, num_start, num_end):
    """Heuristic class label. Prefer a label that FOLLOWS the number in a
    'shares of <CLASS>' construction (Apple/Alphabet); otherwise take the
    nearest 'Class X ...' or class keyword before the number (Amerant)."""
    post = text[num_end:num_end + 110]
    m = re.match(r"[\s,]*(?:thousand|million|billion)?\s*shares?\s+of\s+"
                 r"(?:the\s+|its\s+|Alphabet'?s?\s+|registrant'?s?\s+)*"
                 r"(" + _CLASS_TAIL + r")", post, re.I)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    pre = text[max(0, num_start - 110):num_start]
    pm = list(re.finditer(r"(Class\s+[A-Z]\b[\w ]*?(?:Common\s+Stock|Common\s+Shares|"
                          r"Stock|Shares)|Common\s+Stock|Common\s+Shares|Ordinary\s+Shares|"
                          r"Preferred\s+Stock|Preference\s+Shares|Capital\s+Stock|"
                          r"Redeemable\s+Capital\s+Shares|Limited\s+Voting\s+Shares)", pre, re.I))
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
    r"number\s+of\s+outstanding\s+shares\s+of\s+each\s+of\s+the\s+issuer'?s?\s+"
    r"classes\s+of\s+(?:capital\s+or\s+common|capital|common)\s+stock\s+as\s+of"
    r"[^:.]{0,200}?[:.]",  # newlines allowed: the date/colon often sits on the next line
    re.I,
)


def extract_anchor(text, period=""):
    """For 20-F/40-F: find the fixed regulatory phrase, then read the class/number
    pairs that follow it until the next cover checkbox instruction. Handles both
    list shapes seen in practice:
        label-first : "Ordinary Shares ... : 1,228,504,232"  (SAP, Brookfield)
        number-first: "295,935,686 Common Shares 4,866,814 Series A Preferred ..."  (Emera, Cameco)
    `period` (CONFORMED PERIOD OF REPORT) is the as-of date when none is printed."""
    m = _ANCHOR_RE.search(text)
    if not m:
        return []
    start = m.end()
    stop = re.search(r"Indicate by check mark|\bIf this report\b", text[start:start + 2000], re.I)
    span = text[start: start + (stop.start() if stop else 1600)]
    return _extract_span_pairs(_despace_numbers(span), period)


# A class label = up to ~5 words ending in a class noun, optional Series suffix.
# Case-insensitive so foreign filers' lowercase "ordinary shares" is captured too.
_LABEL_PHRASE_RE = re.compile(
    r"(?:[A-Za-z][\w.&'/-]*\s+){0,5}?"
    r"(?:ordinary\s+shares?|common\s+shares?|common\s+stock|preferred\s+stock|"
    r"preference\s+shares?|preferred\s+shares?|capital\s+stock|"
    r"redeemable\s+capital\s+shares?|limited\s+voting\s+shares?|"
    r"subordinate\s+voting\s+shares?|partnership\s+common\s+units?|"
    r"general\s+partner\s+units?|common\s+units?|deferred\s+shares?|"
    r"shares?|stock|units)(?:,?\s+series\s+[A-Za-z0-9-]+)?",
    re.I,
)
_LABEL_AT_START_RE = re.compile(
    r"^[\s,:]*(?:without\s+(?:nominal|par)\s+value[,:]?\s*)?"
    r"(?:thousand|million|billion)?\s*(?:shares?\s+of\s+(?:the\s+|its\s+|common\s+)*)?"
    r"(" + _LABEL_PHRASE_RE.pattern + r")",
    re.I,
)


def _clean_label(s):
    return re.sub(r"\s+", " ", s).strip(" ,.:")


def _extract_span_pairs(span, period):
    """Walk every share-count number in the anchor listing and bind a class label.
    Decide the list orientation once: if a class noun appears before the first
    number it is label-first (SAP / Brookfield / CIBC) and each number takes the
    nearest label before it; otherwise number-first (Cameco / Emera / BTC) and each
    number takes the label printed right after it."""
    nums = [n for n in _NUM_RE.finditer(span)]
    if not nums:
        return []
    label_first = bool(re.search(r"(?:shares?|stock|units)\b", span[:nums[0].start(1)], re.I))
    entries = []
    for i, nm in enumerate(nums):
        if _looks_like_year(nm.group(1), nm.group(2)):
            continue
        ns, ne = nm.start(1), nm.end()
        if "$" in span[max(0, ns - 5):ns] or "%" in span[ne:ne + 3]:
            continue
        if _skip_number_context(span, ns, ne):
            continue
        val = _to_int(nm.group(1), nm.group(2))
        if val is None or val < PLAUSIBLE_MIN_SHARES:
            continue
        next_start = nums[i + 1].start(1) if i + 1 < len(nums) else len(span)
        label = ""
        if label_first:
            ms = list(_LABEL_PHRASE_RE.finditer(span[:ns]))
            label = ms[-1].group(0) if ms else ""
        else:
            am = _LABEL_AT_START_RE.match(span[ne:next_start])
            label = am.group(1) if am else ""
        if not label:                                   # fall back to label-before
            ms = list(_LABEL_PHRASE_RE.finditer(span[:ns]))
            label = ms[-1].group(0) if ms else ""
        if not label and not re.search(r"shares?|stock|units", span[ne:next_start], re.I):
            continue
        label = _clean_label(label)
        raw_date = _nearest_date(span, ne, window=140)
        entries.append(ClassEntry(
            shares=val, raw_number=nm.group(1), scale=(nm.group(2) or "").lower(),
            class_label=label, share_type=classify_share_type(label),
            as_of_date=iso_date(raw_date) or period, raw_date=raw_date or period,
            matched_text=re.sub(r"\s+", " ", span[max(0, ns - 15):ne + 45]).strip(),
        ))
    return _dedupe(entries)


# ---------------------------------------------------------------------------
# Extraction strategy B: cover-window scan (10-K, and 20-F/40-F fallback)
# ---------------------------------------------------------------------------
def extract_cover_window(text):
    """Scan every 'outstanding' in the cover region; for each, look for a nearby
    share-count number tied to a share word, skipping decoy contexts. Captures
    multiple share classes (e.g. Alphabet A/B/C) naturally."""
    entries = []
    text = _despace_numbers(text)
    for om in re.finditer(r"outstanding", text, re.I):
        opos = om.start()
        lo, hi = max(0, opos - 230), min(len(text), opos + 230)
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
            raw_date = _nearest_date(text, (n_start + n_end) // 2, window=240)
            entries.append(ClassEntry(
                shares=num, raw_number=nm.group(1), scale=(nm.group(2) or "").lower(),
                class_label=label, share_type=classify_share_type(label) if label else "common",
                as_of_date=iso_date(raw_date), raw_date=raw_date,
                matched_text=re.sub(r"\s+", " ", text[max(0, n_start - 80):n_end + 50]).strip(),
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
    if re.search(r"\b(?:excluding|of which|representing|represented\s+by|reserved|"
                 r"equivalent\s+to|adjusted\s+to|in\s+excess\s+of|(?:the\s+)?form\s+of)\b[\s(]*$", pre):
        return True
    # "including N ..." is a subset UNLESS N is itself a named class component of the
    # total that precedes it ("...including 84,463,737 Class A ... and 45,787,948 Class B").
    if re.search(r"\bincluding\b[\s(]*$", pre) \
       and not re.match(r"\s*[a-z'.&\- ]*?class\s+[a-z0-9]", post):
        return True
    if re.search(r"(?:retroactively|post-?reverse|after\s+giving\s+effect|"
                 r"as\s+adjusted|giving\s+retroactive)\b", pre):
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
    # The "consisting of"/"comprising"/"sum of" connective marks a redundant total; a bare
    # "an aggregate of N shares ... outstanding" is just a normal single count -> keep it.
    if re.match(r"[\s,]*[a-z()$.,&'/\-\s]{0,50}?\b(?:consisting\s+of|comprised\s+of|comprising|"
                r"composed\s+of|being\s+the\s+sum\s+of|made\s+up\s+of|the\s+sum\s+of)\b", post):
        return True
    # "X shares, including A Class A ... and B Class B" -> X is the total, drop it
    if re.match(r"[\s,]*[a-z()$.,&'/\-\s]{0,45}?\bincluding\s+\d[\d,. ]*\s*"
                r"(?:thousand|million|billion)?\s*[a-z'.&\- ]*?class\s+[a-z0-9]", post):
        return True

    # 'X shares issued and Y shares outstanding' -> X is the issued count, not the answer
    if re.match(r"\s*(?:shares?\s+)?issued,?\s+and\s+(?:approximately\s+)?[\d(]", post) and \
       not re.match(r"\s*(?:shares?\s+)?issued,?\s+and\s+outstanding", post):
        return True

    # treasury
    if re.match(r"\s*(?:class\s+[a-z]\s+)?(?:ordinary\s+|common\s+)?shares?\s+held\s+in\s+treasury", post):
        return True
    if re.match(r"\s+(?:were\s+)?treasury\b", post) or "in treasury" in post[:34]:
        return True

    # warrants / options to purchase are never the outstanding share count
    if re.search(r"\bwarrants?\b|\boptions?\s+to\s+purchase\b", text[max(0, ns - 30):ne + 30].lower()):
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

    # non-affiliate market-value computation figures (the $-amount is skipped elsewhere).
    # Tight: only a count tied to "non-affiliates" with no "outstanding" nearby — so the real
    # cover count two sentences after a market-value line ("... $X. As of <date>, N shares ...
    # outstanding") is NOT swallowed.
    if re.search(r"non-?affiliate", text[max(0, ns - 55):ne + 45], re.I) and \
       "outstanding" not in text[ne:ne + 55].lower():
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
            entries = extract_anchor(text, period)
            ex.method = "anchor" if entries else ""
            if not entries:
                entries = extract_cover_window(cover)
                ex.method = "cover_window" if entries else "none"
        else:  # 10-K
            entries = extract_cover_window(cover)
            ex.method = "cover_window" if entries else "none"
            if not entries:  # last resort: anchor anywhere (rare combined forms)
                entries = extract_anchor(text, period)
                ex.method = "anchor" if entries else "none"

        # 20-F / 40-F: the as-of date is the fiscal close; fill it from the SGML
        # header when the cover didn't print one inline.
        if filing.form in ("20-F", "40-F") and period:
            for e in entries:
                if not e.as_of_date:
                    e.as_of_date = period
        # 10-K: drop narrative decoys (merger/reverse-split share counts deep in an
        # over-long cover) — keep only counts dated within ~1 year of the filing,
        # provided at least one such recent count exists.
        if filing.form == "10-K":
            entries = _filter_10k_recency(entries, filing.date_filed)
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
