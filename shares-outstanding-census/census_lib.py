"""Engine for the shares-outstanding census pipeline.

Every EDGAR fetch is throttled (~6 req/s, under SEC's 10 req/s limit) and
cached under DATA_DIR, so an interrupted run resumes where it left off and a
finished run re-runs fully offline.
"""

import gzip
import hashlib
import json
import os
import re
import time

import requests

SEC_BASE = "https://www.sec.gov"
THROTTLE_SECONDS = 1.0 / 6.0
ABS_SIC = "6189"  # Asset-Backed Securities

ANNUAL_FORMS = ("10-K", "20-F", "40-F")

_last_request_time = [0.0]


def make_session(user_agent):
    session = requests.Session()
    session.headers.update({
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
    })
    return session


def throttled_get(session, url, stream=False, tries=3, none_on_404=False):
    """GET with a global throttle and simple retry/backoff on 403/429/5xx.
    none_on_404 returns None instead of raising — for APIs where 404 is a
    meaningful negative (concept never tagged by this filer)."""
    for attempt in range(tries):
        wait = THROTTLE_SECONDS - (time.monotonic() - _last_request_time[0])
        if wait > 0:
            time.sleep(wait)
        _last_request_time[0] = time.monotonic()
        try:
            resp = session.get(url, stream=stream, timeout=30)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404 and none_on_404:
                return None
            if resp.status_code in (403, 429, 500, 502, 503) and attempt < tries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
        except requests.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(5 * (attempt + 1))
    raise RuntimeError("unreachable: " + url)


# ------------------------------------------------------------ quarterly index

def fetch_master_index(session, cache_dir, year, quarter):
    """Return the master.idx text for one quarter (downloaded once, then cached)."""
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, "master_%d_QTR%d.idx" % (year, quarter))
    if not os.path.exists(path):
        url = "%s/Archives/edgar/full-index/%d/QTR%d/master.idx" % (
            SEC_BASE, year, quarter)
        resp = throttled_get(session, url)
        with open(path, "wb") as f:
            f.write(resp.content)
    with open(path, "rb") as f:
        return f.read().decode("latin-1")


def parse_master_index(index_text, forms=ANNUAL_FORMS):
    """Rows whose form type matches `forms` EXACTLY — amendments (10-K/A) don't
    sneak in. Each row: accession, index_cik, index_name, form, date_filed,
    txt_path (the relative archive path from the index, used for fetching)."""
    rows = []
    in_body = False
    for line in index_text.splitlines():
        if not in_body:
            if line.startswith("-----"):
                in_body = True
            continue
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, name, form, date_filed, filename = (p.strip() for p in parts)
        if form not in forms or not filename.endswith(".txt"):
            continue
        rows.append({
            "accession": os.path.basename(filename)[:-4],
            "index_cik": cik,
            "index_name": name,
            "form": form,
            "date_filed": date_filed,
            "txt_path": filename,
        })
    return rows


# -------------------------------------------------------------- SGML headers

TRUNCATED_SENTINEL = "#TRUNCATED\n"


def fetch_sgml_header(session, cache_dir, txt_path, accession):
    """The filing's SGML dissemination header: everything before the first
    <DOCUMENT> in the full-submission .txt. Streamed, so only a few KB move
    per filing. Cached per accession. EDGAR serves these latin-1.

    If <DOCUMENT> never appears within the 2 MB cap (it always should), the
    cached header starts with TRUNCATED_SENTINEL so the condition stays
    visible instead of silently caching an incomplete filer list."""
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, accession + ".hdr.txt")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    url = SEC_BASE + "/Archives/" + txt_path
    resp = throttled_get(session, url, stream=True)
    buf = b""
    try:
        for chunk in resp.iter_content(chunk_size=8192):
            buf += chunk
            if b"<DOCUMENT>" in buf or len(buf) > 2 * 1024 * 1024:
                break
    finally:
        resp.close()
    header = buf.split(b"<DOCUMENT>", 1)[0].decode("latin-1")
    if b"<DOCUMENT>" not in buf:
        header = TRUNCATED_SENTINEL + header
    with open(path, "w", encoding="utf-8") as f:
        f.write(header)
    return header


# ---------------------------------------------------------- primary document

def _sgml_doc_field(block, label):
    m = re.search(b"<" + label + b">([^\r\n<]+)", block)
    return m.group(1).decode("latin-1").strip() if m else ""


def _doc_content_and_meta(block, doc_type):
    """Content between <TEXT>...</TEXT> of one <DOCUMENT> block, plus metadata."""
    start = block.find(b"<TEXT>")
    end = block.rfind(b"</TEXT>")
    content = block[start + 6:end] if start >= 0 and end > start else block
    content = content.lstrip(b"\r\n")
    meta = {
        "found": True,
        "type": doc_type,
        "filename": _sgml_doc_field(block, b"FILENAME"),
        "sequence": _sgml_doc_field(block, b"SEQUENCE"),
        "bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }
    return content, meta


def read_cached_document(cache_dir, accession):
    """Cache-only read of a fetched primary document — no network. Returns
    (content_bytes, meta); (None, meta) when the submission has no primary
    document; (None, None) when the filing simply isn't cached."""
    doc_path = os.path.join(cache_dir, accession + ".doc.gz")
    meta_path = os.path.join(cache_dir, accession + ".docmeta.json")
    if not os.path.exists(meta_path):
        return None, None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None, None
    if not meta["found"]:
        return None, meta
    try:
        with gzip.open(doc_path, "rb") as f:
            return f.read(), meta
    except (OSError, EOFError):
        return None, None


def fetch_primary_document(session, cache_dir, txt_path, accession, form):
    """The filing's primary document: the first <DOCUMENT> block in the
    full-submission .txt whose <TYPE> equals the form. Streamed so the
    download stops once the primary document is captured (exhibits and
    graphics behind it never transfer). Cached gzipped per accession.

    Returns (content_bytes, meta_dict); (None, meta) if no matching document
    exists in the submission — that negative result is cached too."""
    os.makedirs(cache_dir, exist_ok=True)
    doc_path = os.path.join(cache_dir, accession + ".doc.gz")
    meta_path = os.path.join(cache_dir, accession + ".docmeta.json")
    if os.path.exists(meta_path):
        # a corrupt sidecar or gzip (process killed mid-write) is a cache miss
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except (json.JSONDecodeError, OSError):
            meta = None
        if meta is not None:
            if not meta["found"]:
                return None, meta
            if os.path.exists(doc_path):
                try:
                    with gzip.open(doc_path, "rb") as f:
                        return f.read(), meta
                except (OSError, EOFError):
                    pass

    url = SEC_BASE + "/Archives/" + txt_path
    resp = throttled_get(session, url, stream=True)
    buf = b""
    content = None
    meta = None
    try:
        for chunk in resp.iter_content(chunk_size=65536):
            buf += chunk
            while content is None:
                start = buf.find(b"<DOCUMENT>")
                if start < 0:
                    buf = buf[-20:]  # keep a tail so a tag split across chunks survives
                    break
                end = buf.find(b"</DOCUMENT>", start)
                if end < 0:
                    buf = buf[start:]  # block incomplete — wait for more chunks
                    break
                block = buf[start:end]
                buf = buf[end + 11:]
                if _sgml_doc_field(block, b"TYPE") == form:
                    content, meta = _doc_content_and_meta(block, form)
            if content is not None:
                break
    finally:
        resp.close()

    if content is None:
        meta = {"found": False, "type": "", "filename": "", "sequence": "",
                "bytes": 0, "sha256": ""}
    else:
        with gzip.open(doc_path, "wb") as f:
            f.write(content)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f)
    return content, meta


# ------------------------------------------------------------- text cleaning

_UNICODE_MAP = {
    "’": "'", "‘": "'", "“": '"', "”": '"',
    " ": " ", "–": "-", "—": "-", "‑": "-",
    "﻿": " ", "​": " ",
}

_IX_STRIP_RE = None  # built lazily to keep bs4 an extract-time-only import


def doc_to_text(content_bytes, filename):
    """Reduce a primary document to clean text for the cover extractor.

    HTML / inline-XBRL documents are parsed with BeautifulSoup after deleting
    the ix:header / ix:hidden blocks (which would otherwise dump a wall of
    XBRL context at the top); plain-text documents are decoded directly.
    Unicode punctuation is normalized, intra-line whitespace collapsed,
    blank lines dropped — one canonical form for every downstream rule."""
    from bs4 import BeautifulSoup

    is_html = filename.lower().endswith((".htm", ".html")) \
        or b"<html" in content_bytes[:2048].lower()
    if is_html:
        soup = BeautifulSoup(content_bytes, "lxml")
        for tag in soup.find_all(re.compile(r"^ix:(header|hidden)$")):
            tag.decompose()
        for tag in soup.find_all(["script", "style"]):
            tag.decompose()
        # newlines at BLOCK boundaries only — a separator at every tag would
        # split words wherever a filer wraps part of a word in its own span
        for tag in soup.find_all(["br"]):
            tag.replace_with("\n")
        for tag in soup.find_all(["p", "div", "tr", "td", "th", "li", "table",
                                  "h1", "h2", "h3", "h4", "h5", "h6"]):
            tag.append("\n")
        text = soup.get_text("")
    else:
        text = content_bytes.decode("latin-1")
        text = text.replace("<PAGE>", "\n")
    for src, dst in _UNICODE_MAP.items():
        text = text.replace(src, dst)
    lines = []
    for line in text.split("\n"):
        line = re.sub(r"[ \t\r\f\v]+", " ", line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


# --------------------------------------------------------- inline-XBRL facts

DEI_NS_PREFIX = "http://xbrl.sec.gov/dei/"
SHARES_FACT_LOCALNAME = "EntityCommonStockSharesOutstanding"
FLOAT_FACT_LOCALNAME = "EntityPublicFloat"


def _ix_fact_value(el):
    """Numeric value of an ix:nonFraction element, honoring format / scale /
    sign. Decimal arithmetic — "66.4" with scale=6 must be exactly 66400000.
    Returns None when nil or unparseable."""
    from decimal import Decimal, InvalidOperation

    if el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
        return None
    fmt = el.get("format", "") or ""
    txt = "".join(el.itertext()).strip()
    if fmt.rsplit(":", 1)[-1] in ("fixed-zero", "num-dot-decimal-zero"):
        num = Decimal(0)
    else:
        t = txt.replace(" ", "").replace(" ", "")
        if "comma-decimal" in fmt:
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")
        if t in ("", "-", "—", "–"):
            return None
        try:
            num = Decimal(t)
        except InvalidOperation:
            return None
    try:
        num *= Decimal(10) ** int(el.get("scale", "0") or "0")
    except ValueError:
        pass
    if el.get("sign") == "-":
        num = -num
    return num


def _ixbrl_root_contexts_units(content_bytes):
    """Parse an inline-XBRL document once: the lxml root, every context
    (id -> instant + sorted dimension members) and every unit
    (id -> measure local part, e.g. USD). Returns (None, {}, {}) when the
    document isn't parseable XML."""
    from lxml import etree

    s = content_bytes
    if s[:6].strip().lower().startswith(b"<xbrl"):
        s = s[s.find(b">") + 1:]
    i = s.find(b"<?xml")
    if 0 <= i <= 512:
        s = s[i:]
    try:
        root = etree.fromstring(
            s, parser=etree.XMLParser(recover=True, huge_tree=True))
    except etree.XMLSyntaxError:
        return None, {}, {}
    if root is None:
        return None, {}, {}

    contexts, units = {}, {}
    for ctx in root.iter():
        if callable(ctx.tag):  # comments / processing instructions
            continue
        ln = etree.QName(ctx).localname
        if ln == "unit":
            measures = [(sub.text or "").strip().split(":")[-1]
                        for sub in ctx.iter()
                        if not callable(sub.tag)
                        and etree.QName(sub).localname == "measure"]
            units[ctx.get("id", "")] = measures[0] if measures else ""
            continue
        if ln != "context":
            continue
        instant = ""
        dims = []
        for sub in ctx.iter():
            if callable(sub.tag):
                continue
            ln2 = etree.QName(sub).localname
            if ln2 in ("instant", "endDate") and sub.text:
                instant = sub.text.strip()
            elif ln2 == "explicitMember":
                axis = (sub.get("dimension") or "").split(":")[-1]
                member = (sub.text or "").strip()
                dims.append("%s=%s" % (axis, member))
        contexts[ctx.get("id", "")] = {"instant": instant,
                                       "dims": "|".join(sorted(dims))}
    return root, contexts, units


def _iter_dei_nonfractions(root, localname):
    """Every ix:nonFraction element whose name is dei:{localname}."""
    from lxml import etree

    for el in root.iter():
        if callable(el.tag):
            continue
        if etree.QName(el).localname != "nonFraction":
            continue
        name = el.get("name") or ""
        if ":" not in name:
            continue
        prefix, local = name.split(":", 1)
        if local != localname:
            continue
        if not el.nsmap.get(prefix, "").startswith(DEI_NS_PREFIX):
            continue
        yield el


def parse_ixbrl_dei_facts(content_bytes):
    """Every dei:EntityCommonStockSharesOutstanding fact tagged in an inline-
    XBRL document, with its as-of instant and dimension members (share-class
    axis, legal-entity axis, ...). Returns a list of dicts, deduped; empty
    list when the document has no inline XBRL or no such facts."""
    root, contexts, _ = _ixbrl_root_contexts_units(content_bytes)
    if root is None:
        return []
    facts = []
    seen = set()
    for el in _iter_dei_nonfractions(root, SHARES_FACT_LOCALNAME):
        value = _ix_fact_value(el)
        if value is None or value < 0 or value != int(value):
            continue
        ctx = contexts.get(el.get("contextRef", ""), {"instant": "", "dims": ""})
        key = (int(value), ctx["instant"], ctx["dims"])
        if key in seen:
            continue
        seen.add(key)
        facts.append({"value": int(value), "instant": ctx["instant"],
                      "dims": ctx["dims"]})
    return facts


def parse_ixbrl_float_facts(content_bytes):
    """Every dei:EntityPublicFloat fact tagged in an inline-XBRL document,
    with its as-of instant, dimension members, and currency unit. Monetary
    semantics differ from the shares fact: zero is a meaningful value (shells
    and wholly-owned registrants), sub-dollar precision is kept, and an
    explicit xsi:nil fact — the filer stating no determinable float — is
    returned with value "" so the signal isn't lost. Negative values are
    discarded as tagging errors. Returns a list of dicts, deduped."""
    root, contexts, units = _ixbrl_root_contexts_units(content_bytes)
    if root is None:
        return []
    facts = []
    seen = set()
    for el in _iter_dei_nonfractions(root, FLOAT_FACT_LOCALNAME):
        if el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
            value_str = ""
        else:
            value = _ix_fact_value(el)
            if value is None or value < 0:
                continue
            value_str = ("%d" % value) if value == int(value) \
                else format(value, "f")
        ctx = contexts.get(el.get("contextRef", ""), {"instant": "", "dims": ""})
        unit = units.get(el.get("unitRef", ""), "")
        key = (value_str, ctx["instant"], ctx["dims"], unit)
        if key in seen:
            continue
        seen.add(key)
        facts.append({"value": value_str, "instant": ctx["instant"],
                      "dims": ctx["dims"], "unit": unit})
    return facts


# ------------------------------------------------- registrant name matching

# Company-name token canonicalization for matching cover labels / XBRL
# legal-entity members to SGML header FILER blocks. EDGAR conformed names
# abbreviate ("ARIZONA PUBLIC SERVICE CO"); covers spell out ("Arizona
# Public Service Company"); both must land on the same tokens.
_NAME_TOKEN_CANON = {
    "COMPANY": "CO", "COMPANIES": "CO", "CORPORATION": "CORP",
    "INCORPORATED": "INC", "LIMITED": "LTD",
}
# multi-token forms fused to their abbreviation AFTER per-token canon:
# "Limited Partnership" -> LTD PARTNERSHIP -> LP, "Public Limited Company"
# -> PUBLIC LTD CO -> PLC, "Limited Liability Company" -> LTD LIABILITY CO
# -> LLC
_NAME_FUSE = [
    (("LTD", "PARTNERSHIP"), "LP"),
    (("LTD", "LIABILITY", "CO"), "LLC"),
    (("PUBLIC", "LTD", "CO"), "PLC"),
]


def _name_tokens(name):
    """Canonical token list for company-name comparison: uppercase, '&' ->
    AND, punctuation stripped per token, runs of single letters fused
    ('L.P.' / 'L P' -> LP), abbreviations canonicalized, leading THE and
    EDGAR state suffixes ('/OH/') dropped, '... and Subsidiaries' tails
    dropped."""
    s = name.upper().replace("&", " AND ")
    s = re.sub(r"/[A-Z]{1,4}/?\s*$", " ", s)
    s = re.sub(r"\b(?:AND\s+(?:ITS\s+)?)?SUBSIDIARIES\s*$", " ", s)
    raw = [t for t in (re.sub(r"[^A-Z0-9]", "", tok) for tok in s.split())
           if t]
    toks, i = [], 0
    while i < len(raw):
        if len(raw[i]) == 1 and i + 1 < len(raw) and len(raw[i + 1]) == 1:
            j = i
            while j < len(raw) and len(raw[j]) == 1:
                j += 1
            toks.append("".join(raw[i:j]))
            i = j
        else:
            toks.append(raw[i])
            i += 1
    toks = [_NAME_TOKEN_CANON.get(t, t) for t in toks]
    if toks and toks[0] == "THE":
        toks = toks[1:]
    for pat, repl in _NAME_FUSE:
        out, i = [], 0
        while i < len(toks):
            if tuple(toks[i:i + len(pat)]) == pat:
                out.append(repl)
                i += len(pat)
            else:
                out.append(toks[i])
                i += 1
        toks = out
    return toks


def _is_subsequence(short, long_):
    it = iter(long_)
    return all(t in it for t in short)


def match_label_to_filer(label, filers):
    """Match a row label (cover table line or XBRL legal-entity member) to
    one of a filing's FILER blocks.

    Returns (filer_dict, remainder) — remainder is the part of the label
    beyond the registrant name (a security designation like "Common Stock,
    without par value"), recovered from the ORIGINAL label text — or
    (None, label) when no unambiguous match exists.

    Tiers: exact name match; label = filer name + suffix (remainder kept);
    label abbreviates the filer (ordered token subsequence, fewest extra
    filer tokens wins). Ties within a tier are ambiguous -> no match."""
    lt = _name_tokens(label)
    if not lt:
        return None, label
    tiers = {0: [], 1: [], 2: []}
    for f in filers:
        ft = _name_tokens(f["name"])
        if not ft:
            continue
        if lt == ft:
            tiers[0].append((0, f))
        elif len(lt) > len(ft) and lt[:len(ft)] == ft:
            tiers[1].append((-len(ft), f))
        elif len(lt) >= 2 and len(ft) > len(lt) and _is_subsequence(lt, ft):
            tiers[2].append((len(ft) - len(lt), f))
    for tier in (0, 1, 2):
        cands = sorted(tiers[tier], key=lambda c: c[0])
        if not cands:
            continue
        if len(cands) > 1 and cands[0][0] == cands[1][0]:
            return None, label  # two filers tie — ambiguous
        f = cands[0][1]
        remainder = ""
        if tier == 1:
            ft = _name_tokens(f["name"])
            orig = label.split()
            for i in range(1, len(orig) + 1):
                if _name_tokens(" ".join(orig[:i])) == ft:
                    remainder = " ".join(orig[i:]).lstrip(" ,.-—–").strip()
                    break
            # a pure par-value note after the name is not a class/series
            if re.fullmatch(r"(?i)\(?\s*(?:without|no)\s+par\s+value\.?\s*\)?",
                            remainder):
                remainder = ""
            # a bare acronym of the matched name ("PSNH" after "Public
            # Service Company of New Hampshire") restates the registrant,
            # not a class/series
            if re.fullmatch(r"\(?[A-Z]{2,6}\)?", remainder):
                initials = iter(t[0] for t in ft)
                if all(ch in initials for ch in remainder.strip("()")):
                    remainder = ""
        return f, remainder
    return None, label


def read_cached_header(cache_dir, accession):
    """Cache-only read of a fetched SGML header — no network. Returns the
    header text, or None when the filing isn't cached."""
    path = os.path.join(cache_dir, accession + ".hdr.txt")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_filers(directory, accession):
    """The filing's FILER blocks (name, cik, sic) from the cached SGML
    header, deduped — cache-only, deterministic. Empty list when the header
    isn't cached."""
    hdr = read_cached_header(os.path.join(directory, "cache", "headers"),
                             accession)
    if hdr is None:
        return []
    filers, seen = [], set()
    for f in parse_sgml_header(hdr)["filers"]:
        key = (f["name"], f["cik"])
        if key in seen:
            continue
        seen.add(key)
        filers.append(f)
    return filers


_SIC_RE = re.compile(
    r"STANDARD INDUSTRIAL CLASSIFICATION:\s*(.*?)\s*\[(\d{4})\]")


def _header_field(label, text):
    m = re.search(re.escape(label) + r":\s*(\S[^\r\n]*)", text)
    return m.group(1).strip() if m else ""


def parse_sgml_header(header_text):
    """Top-level fields plus one dict per FILER block (multi-registrant
    filings — e.g. a utility holding company plus its subsidiaries — have
    several FILER blocks; the first is treated as primary)."""
    out = {
        "submission_type": _header_field("CONFORMED SUBMISSION TYPE", header_text),
        "period_of_report": _header_field("CONFORMED PERIOD OF REPORT", header_text),
        "filed_date": _header_field("FILED AS OF DATE", header_text),
        "filers": [],
    }
    blocks = re.split(r"^FILER:\s*$", header_text, flags=re.M)
    for block in blocks[1:]:
        sic_m = _SIC_RE.search(block)
        cik_raw = _header_field("CENTRAL INDEX KEY", block)
        out["filers"].append({
            "name": _header_field("COMPANY CONFORMED NAME", block),
            "cik": cik_raw.lstrip("0") or ("0" if cik_raw else ""),
            "sic": sic_m.group(2) if sic_m else "",
            "sic_desc": sic_m.group(1).strip() if sic_m else "",
        })
    return out
