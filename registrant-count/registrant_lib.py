"""Engine for the registrant-count dataset.

Self-contained (does not import the census's census_lib): the same EDGAR
source and the same population definition, re-derived here so this folder
stands on its own. Every fetch is throttled (~6 req/s, under SEC's 10 req/s
cap) and cached; a finished run re-runs fully offline.

`cache_dirs` is a *list* of cache roots checked in order for reads (each
expected to hold `indexes/` and `headers/` subfolders); the first is written
to on a miss. Point a later entry at an existing census cache to run offline
without re-fetching 7,650 headers.
"""

import gzip
import json
import os
import re
import time

import requests

SEC_BASE = "https://www.sec.gov"
THROTTLE_SECONDS = 1.0 / 6.0
ABS_SIC = "6189"  # Asset-Backed Securities
ANNUAL_FORMS = ("10-K", "20-F", "40-F")
TRUNCATED_SENTINEL = "#TRUNCATED\n"

DEI_NS_PREFIX = "http://xbrl.sec.gov/dei/"
INCORP_LOCALNAME = "EntityIncorporationStateCountryCode"
ADDR_STATE_LOCALNAME = "EntityAddressStateOrProvince"

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
    none_on_404 returns None instead of raising — for the submissions API,
    where 404 means EDGAR has no submissions record for that CIK."""
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


# ---------------------------------------------- EDGAR submissions (metadata)

def fetch_submissions(session, cache_dirs, cik):
    """EDGAR's authoritative company-metadata record for a CIK
    (data.sec.gov/submissions/CIK##########.json), cached per CIK. Carries the
    normalized stateOfIncorporation and business stateOrCountry in the SAME
    EDGAR code space the SGML header uses — so it fills a header that omitted
    those lines without any name decoding. Returns the parsed dict, or None on
    404 (no record). EDGAR's current record, not the as-filed header; state of
    incorporation is stable, so this is a faithful fill for the blanks."""
    name = "CIK%010d.json" % int(cik)
    for d in cache_dirs:
        p = os.path.join(d, "submissions", name)
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                pass
    primary = os.path.join(cache_dirs[0], "submissions")
    os.makedirs(primary, exist_ok=True)
    resp = throttled_get(session, "https://data.sec.gov/submissions/" + name,
                         none_on_404=True)
    if resp is None:
        return None
    with open(os.path.join(primary, name), "w", encoding="utf-8") as f:
        f.write(resp.text)
    return resp.json()


def submissions_state_of_incorp(j):
    return (j.get("stateOfIncorporation") or "").upper() if j else ""


def submissions_business_state(j):
    if not j:
        return ""
    return (j.get("addresses", {}).get("business", {})
            .get("stateOrCountry") or "").upper()


# ----------------------------------- decode an XBRL state/country display name
# to an EDGAR code, to validate an API fill against the filing's own as-filed
# XBRL. US states use the standard postal codes EDGAR also uses; foreign and
# provincial names are decoded from EDGAR's OWN code<->name pairs as they
# appear in the cached submissions records (stateOfIncorporationDescription),
# never from a hand-typed table.

US_STATE_NAME_TO_CODE = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC", "FLORIDA": "FL", "GEORGIA": "GA",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN",
    "IOWA": "IA", "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA",
    "MAINE": "ME", "MARYLAND": "MD", "MASSACHUSETTS": "MA", "MICHIGAN": "MI",
    "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT",
    "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR",
    "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
    "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY", "PUERTO RICO": "PR", "GUAM": "GU",
    "VIRGIN ISLANDS": "VI", "AMERICAN SAMOA": "AS",
    "NORTHERN MARIANA ISLANDS": "MP",
}


def _name_variants(desc):
    u = re.sub(r"\s+", " ", desc).strip().upper()
    return {x for x in (u, u.split(",")[0].strip()) if x}


def incorporation_name_to_code_map(cache_dirs):
    """Returns (name_upper -> EDGAR code, set_of_known_codes): the US-state
    postal names plus every foreign/provincial stateOfIncorporationDescription
    seen in the cached submissions records."""
    m = dict(US_STATE_NAME_TO_CODE)
    codes = set(m.values())
    for d in cache_dirs:
        sd = os.path.join(d, "submissions")
        if not os.path.isdir(sd):
            continue
        for fn in os.listdir(sd):
            try:
                with open(os.path.join(sd, fn), "r", encoding="utf-8") as f:
                    j = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue
            code = (j.get("stateOfIncorporation") or "").upper()
            desc = (j.get("stateOfIncorporationDescription") or "").strip()
            if code:
                codes.add(code)
            if code and desc and desc.upper() != code:
                for key in _name_variants(desc):
                    m.setdefault(key, code)
    return m, codes


def decode_incorp_name(name, name2code, codeset):
    """An XBRL display name (or raw code) -> EDGAR code, or "" if undecodable."""
    if not name:
        return ""
    u = re.sub(r"\s+", " ", name).strip().upper()
    if len(u) == 2 and u in codeset:
        return u                       # already an EDGAR code
    if u in name2code:
        return name2code[u]
    return name2code.get(u.split(",")[0].strip(), "")


# ------------------------------------------------------------ quarterly index

def fetch_master_index(session, cache_dirs, year, quarter):
    """The master.idx text for one quarter. Read from any cache root that has
    it; otherwise download once into the first cache root."""
    name = "master_%d_QTR%d.idx" % (year, quarter)
    for d in cache_dirs:
        p = os.path.join(d, "indexes", name)
        if os.path.exists(p):
            with open(p, "rb") as f:
                return f.read().decode("latin-1")
    primary = os.path.join(cache_dirs[0], "indexes")
    os.makedirs(primary, exist_ok=True)
    url = "%s/Archives/edgar/full-index/%d/QTR%d/master.idx" % (
        SEC_BASE, year, quarter)
    resp = throttled_get(session, url)
    with open(os.path.join(primary, name), "wb") as f:
        f.write(resp.content)
    return resp.content.decode("latin-1")


def parse_master_index(index_text, forms=ANNUAL_FORMS):
    """Rows whose form type matches `forms` EXACTLY — amendments (10-K/A) don't
    sneak in. Each row: accession, index_cik, index_name, form, date_filed,
    txt_path."""
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

def fetch_sgml_header(session, cache_dirs, txt_path, accession):
    """The filing's SGML dissemination header (everything before the first
    <DOCUMENT>). Read from any cache root that has it; otherwise stream it
    (only a few KB) into the first cache root. EDGAR serves these latin-1."""
    name = accession + ".hdr.txt"
    for d in cache_dirs:
        p = os.path.join(d, "headers", name)
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return f.read()
    primary = os.path.join(cache_dirs[0], "headers")
    os.makedirs(primary, exist_ok=True)
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
    with open(os.path.join(primary, name), "w", encoding="utf-8") as f:
        f.write(header)
    return header


# ------------------------------------------------------- SGML header parsing

_SIC_RE = re.compile(
    r"STANDARD INDUSTRIAL CLASSIFICATION:\s*(.*?)\s*\[(\d{4})\]")
# a subsection header line: a label alone on its line (no value after the
# colon), e.g. "\tBUSINESS ADDRESS:\t"
_SUBSEC_RE = re.compile(r"(?m)^\t*[A-Z][A-Z &/]+:[ \t]*$")


def _field(label, text):
    """First `LABEL: value` on a single line within `text`."""
    m = re.search(re.escape(label) + r":[ \t]*(\S[^\r\n]*)", text)
    return m.group(1).strip() if m else ""


def _subsection(block, header):
    """Text of the `header:` subsection (COMPANY DATA / BUSINESS ADDRESS /
    MAIL ADDRESS) up to the next subsection header — so a STATE: field is read
    from the right address, never the wrong one."""
    m = re.search(r"(?m)^\t*" + re.escape(header) + r":[ \t]*$", block)
    if not m:
        return ""
    rest = block[m.end():]
    nxt = _SUBSEC_RE.search(rest)
    return rest[:nxt.start()] if nxt else rest


def parse_header(header_text):
    """Top-level fields plus one dict per FILER block. The first FILER block is
    the primary registrant (matches the census's primary-filer convention).
    Each filer carries name / cik / sic / sic_desc plus the two location
    fields: business_state and mail_state (EDGAR State-or-Country codes from
    the address blocks) and state_of_incorp (from COMPANY DATA)."""
    out = {
        "submission_type": _field("CONFORMED SUBMISSION TYPE", header_text),
        "period_of_report": _field("CONFORMED PERIOD OF REPORT", header_text),
        "filed_date": _field("FILED AS OF DATE", header_text),
        "filers": [],
    }
    blocks = re.split(r"(?m)^FILER:[ \t]*$", header_text)
    for block in blocks[1:]:
        sic_m = _SIC_RE.search(block)
        cik_raw = _field("CENTRAL INDEX KEY", block)
        company = _subsection(block, "COMPANY DATA")
        business = _subsection(block, "BUSINESS ADDRESS")
        mail = _subsection(block, "MAIL ADDRESS")
        out["filers"].append({
            "name": _field("COMPANY CONFORMED NAME", block),
            "cik": cik_raw.lstrip("0") or ("0" if cik_raw else ""),
            "sic": sic_m.group(2) if sic_m else "",
            "sic_desc": sic_m.group(1).strip() if sic_m else "",
            "state_of_incorp": _field("STATE OF INCORPORATION", company or block),
            "business_state": _field("STATE", business),
            "mail_state": _field("STATE", mail),
            "file_number": _field("SEC FILE NUMBER", block),
        })
    return out


def fmt_date(yyyymmdd):
    """EDGAR 8-digit date -> ISO YYYY-MM-DD; pass anything else through."""
    s = (yyyymmdd or "").strip()
    if len(s) == 8 and s.isdigit():
        return "%s-%s-%s" % (s[:4], s[4:6], s[6:8])
    return s


# ----------------------------------------------- primary document (for XBRL)

def read_cached_doc(cache_dirs, accession):
    """The filing's cached primary document bytes, from any cache root's
    `docs/` subfolder (gzipped, as the census stores them). Returns None when
    not cached or when the submission had no primary document. No network."""
    for d in cache_dirs:
        meta_path = os.path.join(d, "docs", accession + ".docmeta.json")
        doc_path = os.path.join(d, "docs", accession + ".doc.gz")
        if not os.path.exists(meta_path):
            continue
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        if not meta.get("found"):
            return None
        try:
            with gzip.open(doc_path, "rb") as f:
                return f.read()
        except (OSError, EOFError):
            continue
    return None


def fetch_primary_document(session, cache_dirs, txt_path, accession, form):
    """The first <DOCUMENT> block whose <TYPE> equals the form, streamed and
    cached gzipped under the first cache root's `docs/`. Cache-first; only
    fetched when no cache root already has it (so a warm census cache means no
    network). Returns content bytes or None when the submission has none."""
    cached = read_cached_doc(cache_dirs, accession)
    if cached is not None:
        return cached
    # already-cached negative?
    for d in cache_dirs:
        mp = os.path.join(d, "docs", accession + ".docmeta.json")
        if os.path.exists(mp):
            try:
                with open(mp, "r", encoding="utf-8") as f:
                    if not json.load(f).get("found"):
                        return None
            except (json.JSONDecodeError, OSError):
                pass
    out_docs = os.path.join(cache_dirs[0], "docs")
    os.makedirs(out_docs, exist_ok=True)
    url = SEC_BASE + "/Archives/" + txt_path
    resp = throttled_get(session, url, stream=True)
    buf = b""
    content = None
    try:
        for chunk in resp.iter_content(chunk_size=65536):
            buf += chunk
            while content is None:
                start = buf.find(b"<DOCUMENT>")
                if start < 0:
                    buf = buf[-20:]
                    break
                end = buf.find(b"</DOCUMENT>", start)
                if end < 0:
                    buf = buf[start:]
                    break
                block = buf[start:end]
                buf = buf[end + 11:]
                m = re.search(b"<TYPE>([^\r\n<]+)", block)
                if m and m.group(1).decode("latin-1").strip() == form:
                    ts, te = block.find(b"<TEXT>"), block.rfind(b"</TEXT>")
                    content = block[ts + 6:te] if ts >= 0 and te > ts else block
                    content = content.lstrip(b"\r\n")
            if content is not None:
                break
    finally:
        resp.close()
    meta = {"found": content is not None}
    with open(os.path.join(out_docs, accession + ".docmeta.json"), "w",
              encoding="utf-8") as f:
        json.dump(meta, f)
    if content is not None:
        with gzip.open(os.path.join(out_docs, accession + ".doc.gz"), "wb") as f:
            f.write(content)
    return content


# ----------------------------------------- inline-XBRL dei state/incorp tags

def _ixbrl_root_default_contexts(content_bytes):
    """Parse an inline-XBRL doc once. Returns (root, default_ctx_ids) where
    default_ctx_ids is the set of context ids carrying NO explicitMember
    dimension — i.e. the registrant-level (primary) contexts."""
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
        return None, set()
    if root is None:
        return None, set()
    default = set()
    for ctx in root.iter():
        if callable(ctx.tag) or etree.QName(ctx).localname != "context":
            continue
        has_dim = any(not callable(sub.tag)
                      and etree.QName(sub).localname == "explicitMember"
                      for sub in ctx.iter())
        if not has_dim:
            default.add(ctx.get("id", ""))
    return root, default


def read_cached_text(cache_dirs, accession):
    """The census's cleaned cover text for a filing (gzip), or None."""
    for d in cache_dirs:
        p = os.path.join(d, "text", accession + ".txt.gz")
        if os.path.exists(p):
            try:
                with gzip.open(p, "rb") as f:
                    return f.read().decode("utf-8", "replace")
            except (OSError, EOFError):
                continue
    return None


# ----------------------------------------------- cover checkbox / dei facts

_COVER_BOOL = {
    "wksi": "EntityWellKnownSeasonedIssuer",
    "shell": "EntityShellCompany",
    "src": "EntitySmallBusiness",
    "egc": "EntityEmergingGrowthCompany",
}
# longer needles first so "Large accelerated" / "Non-accelerated" win over "Accelerated"
_FILER_CATEGORY = [("LARGE ACCELERATED", "LAF"), ("NON-ACCELERATED", "NAF"),
                   ("NONACCELERATED", "NAF"), ("ACCELERATED", "AF")]


def _cover_bool(entries):
    """A dei checkbox fact -> "1"/"0"/"" .

    The TRANSFORM is authoritative, not the displayed glyph: SEC inline-XBRL
    encodes these booleans with `ixt:booleantrue`/`booleanfalse` (or older
    `fixed-true`/`fixed-false`) — and a `booleanfalse` fact frequently RENDERS
    as ☒, because the filer puts a checked box next to "No". So a format ending
    in true/false wins outright; `boolballotbox` and bare text are read from the
    glyph (a rare filer mis-tags the ☒ next to *No* of a Yes/No question, which
    no glyph rule can recover). Prefers a default-context fact."""
    if not entries:
        return ""
    val, fmt, _ = sorted(entries, key=lambda e: 0 if e[2] else 1)[0]
    f = fmt.rsplit(":", 1)[-1].lower()
    if f.endswith("true"):        # fixed-true, booleantrue
        return "1"
    if f.endswith("false"):       # fixed-false, booleanfalse
        return "0"
    t = val.strip()
    if "☒" in t or "☑" in t:
        return "1"
    if "☐" in t:
        return "0"
    tl = t.lower()
    if tl in ("yes", "true", "x"):
        return "1"
    if tl in ("no", "false", ""):
        return "0"
    return ""


def _filer_category(entries):
    if not entries:
        return ""
    t = sorted(entries, key=lambda e: 0 if e[2] else 1)[0][0]
    t = re.sub(r"\s+", " ", t).upper()   # NBSP/&#160; between words -> plain space
    for needle, code in _FILER_CATEGORY:
        if needle in t:
            return code
    return ""


def extract_cover_facts(content_bytes):
    """One inline-XBRL parse -> the cover dei facts of a filing:
    {wksi, shell, src, egc: "1"/"0"/"" ; afs: NAF/AF/LAF/"" ; has_12b, has_12g}.
    Returns None when the document isn't parseable inline XBRL."""
    from lxml import etree

    root, default = _ixbrl_root_default_contexts(content_bytes)
    if root is None:
        return None
    collected = {}
    has_12b = has_12g = False
    wanted = set(_COVER_BOOL.values()) | {"EntityFilerCategory"}
    for el in root.iter():
        if callable(el.tag) or etree.QName(el).localname != "nonNumeric":
            continue
        name = el.get("name") or ""
        if ":" not in name:
            continue
        prefix, local = name.split(":", 1)
        if not el.nsmap.get(prefix, "").startswith(DEI_NS_PREFIX):
            continue
        if local in ("SecurityExchangeName", "Security12gTitle"):
            v = "".join(el.itertext()).strip()
            nil = el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true"
            real = bool(v) and not nil and v.lower() not in ("none", "n/a", "not applicable")
            if real and local == "Security12gTitle":
                has_12g = True
            elif real:                # a named exchange == a §12(b) registration
                has_12b = True        # (a bare Security12bTitle with no exchange
                                      # is a delisted issuer, not a 12(b) security)
        elif local in wanted:
            collected.setdefault(local, []).append(
                ("".join(el.itertext()).strip(), el.get("format", "") or "",
                 el.get("contextRef", "") in default))
    out = {k: _cover_bool(collected.get(v)) for k, v in _COVER_BOOL.items()}
    out["afs"] = _filer_category(collected.get("EntityFilerCategory"))
    out["has_12b"] = has_12b
    out["has_12g"] = has_12g
    return out


# Anchor on the full heading "Securities registered pursuant to Section 12(b/g)
# of the Act:" — NOT a bare "Section 12(b) of the Act", which also appears in
# the Dodd-Frank clawback sentence ("If securities are registered pursuant to
# Section 12(b) of the Act, indicate by check mark whether the financial
# statements ... reflect the correction of an error ...").
# the Act suffix varies a lot: "of the Act:", "of the Exchange Act:", "of the
# Securities Exchange Act of 1934:", sometimes with no colon before the security
_ACT = r"of\s+the\s+(?:securities\s+)?(?:exchange\s+)?act(?:\s+of\s+1934)?\s*:?"
# the heading varies widely: "Securities [or to be] registered pursuant to /
# under Section 12(x)", "Securities to be registered under Section 12(x)" — but
# never matches the clawback ("If securities ARE registered pursuant to ...").
_REG = (r"securities\s+(?:(?:or\s+)?to\s+be\s+)?registered\s+"
        r"(?:or\s+to\s+be\s+registered\s+)?(?:pursuant\s+to|under)\s+section\s*")
_RE_12B_HEAD = re.compile(r"(?i)" + _REG + r"12\(b\)\s*" + _ACT)
_RE_12G_HEAD = re.compile(r"(?i)" + _REG + r"12\(g\)\s*" + _ACT)
# the standard 12(b) table column headers, stripped before testing for a security
_RE_12B_HEADERS = re.compile(
    r"(?i)title of (?:each )?class|trading symbol\(?s?\)?"
    r"|name of each exchange on which (?:our shares are traded|registered)"
    r"|name of each exchange")
# a real national exchange — 12(b) IS exchange-registration, so its block names
# one; this excludes "Common Stock ... None None" and OTC/Pink listings
_RE_EXCHANGE = re.compile(
    r"(?i)nasdaq|nyse|new york stock exchange|cboe|\bbzx\b|nyse american|nyse arca"
    r"|nyse chicago|nyse texas|chicago stock exchange|\biex\b|investors exchange"
    r"|miax|long.?term stock exchange|stock exchange|stock market")


def cover_has_12b_security(text):
    """True when the cover's 'Securities registered pursuant to Section 12(b)'
    block names a security listed on a real exchange (not None / OTC)."""
    if not text:
        return False
    m = _RE_12B_HEAD.search(text)
    if not m:
        return False
    region = text[m.end():m.end() + 600]
    region = re.split(r"(?i)" + _REG + r"12\(g\)|indicate by check mark", region)[0]
    body = _RE_12B_HEADERS.sub(" ", region).strip(" :\t\r\n.—–-")
    if not body or re.match(r"(?i)(none|not\s*applicable|n/?a)\b", body):
        return False
    return bool(re.search(r"[A-Za-z]", body[:160])) and bool(_RE_EXCHANGE.search(region))


def cover_has_12g_security(text):
    """True when the cover's 'Securities registered pursuant to Section 12(g)'
    block names an actual security. Like the 12(b) check, the column headers
    are STRIPPED (the security sits after 'Title of each class'); the trailing
    'None' is the empty exchange column, not the security. A 12(g) block that
    is itself just 'None' is False."""
    if not text:
        return False
    m = _RE_12G_HEAD.search(text)
    if not m:
        return False
    region = text[m.end():m.end() + 400]
    region = re.split(r"(?i)indicate by check mark|securities\s+(?:registered|for\s+which)",
                      region)[0]
    body = _RE_12B_HEADERS.sub(" ", region).strip(" :\t\r\n.—–-")
    if not body or re.match(r"(?i)(none|not\s*applicable|n/?a)\b", body):
        return False
    return bool(re.search(r"[A-Za-z]", body[:120]))


# --------- cover-checkbox scrape, used only when the XBRL dei tag is ABSENT
_CHECKED = ("☒", "☑")


def _box_state(seg):
    m = re.search(r"[☒☑☐]", seg)
    return ("1" if m.group(0) in _CHECKED else "0") if m else ""


def scrape_checkbox(text, label_re, window=14):
    """The box TIGHTLY after a label, scanning every occurrence so the label in
    the instructional sentence ('...a smaller reporting company, or an emerging
    growth company. See the definitions...') — which has no adjacent box — is
    skipped in favour of the checkbox-area occurrence."""
    if not text:
        return ""
    for m in re.finditer(label_re, text, re.I):
        s = _box_state(text[m.end():m.end() + window])
        if s:
            return s
    return ""


def scrape_yesno(text, label_re, window=220):
    """A Yes/No check-mark question -> 1 if Yes is checked, 0 if No is checked."""
    if not text:
        return ""
    m = re.search(label_re, text, re.I)
    if not m:
        return ""
    seg = text[m.end():m.end() + window]
    ym = re.search(r"yes\s*([☒☑☐])", seg, re.I)
    nm = re.search(r"no\s*([☒☑☐])", seg, re.I)
    if ym and ym.group(1) in _CHECKED:
        return "1"
    if nm and nm.group(1) in _CHECKED:
        return "0"
    if ym and ym.group(1) == "☐":
        return "0"
    return ""


def scrape_filer_category(text):
    """Which of the three accelerated-filer boxes is checked -> LAF/AF/NAF."""
    if not text:
        return ""
    found = {}
    for pat, code in [(r"large\s+accelerated\s+filer\s{0,3}([☒☑☐])", "LAF"),
                      (r"non-?\s*accelerated\s+filer\s{0,3}([☒☑☐])", "NAF"),
                      (r"(?<![a-z\-])accelerated\s+filer\s{0,3}([☒☑☐])", "AF")]:
        for m in re.finditer(pat, text, re.I):   # skip the instructional sentence
            found[code] = m.group(1)
            break
    for code in ("LAF", "AF", "NAF"):
        if found.get(code) in _CHECKED:
            return code
    return ""


def scrape_cover_checkboxes(text):
    """Best-effort cover read of the five checkbox flags, for filings whose
    inline XBRL omits the dei tag. Each value "1"/"0"/"" (afs: LAF/AF/NAF/"")."""
    return {
        "wksi": scrape_yesno(text, r"well[- ]known\s+seasoned\s+issuer"),
        "shell": scrape_yesno(text, r"is\s+a\s+shell\s+company|registrant\s+is\s+a\s+shell"),
        "src": scrape_checkbox(text, r"smaller\s+reporting\s+company"),
        "egc": scrape_checkbox(text, r"emerging\s+growth\s+company"),
        "afs": scrape_filer_category(text),
    }


def extract_dei_state(content_bytes, localname):
    """The registrant-level value of a dei:{localname} ix:nonNumeric tag
    (EntityIncorporationStateCountryCode / EntityAddressStateOrProvince),
    as the displayed text ("Delaware", "Cayman Islands"). Prefers the value in
    a default (undimensioned) context — the primary registrant. Returns "" when
    the document has no inline XBRL or no such tag."""
    from lxml import etree

    root, default = _ixbrl_root_default_contexts(content_bytes)
    if root is None:
        return ""
    default_val, any_val = "", ""
    for el in root.iter():
        if callable(el.tag) or etree.QName(el).localname != "nonNumeric":
            continue
        name = el.get("name") or ""
        if ":" not in name:
            continue
        prefix, local = name.split(":", 1)
        if local != localname:
            continue
        if not el.nsmap.get(prefix, "").startswith(DEI_NS_PREFIX):
            continue
        val = "".join(el.itertext()).strip()
        if not val:
            continue
        any_val = any_val or val
        if el.get("contextRef", "") in default:
            default_val = default_val or val
    return default_val or any_val

