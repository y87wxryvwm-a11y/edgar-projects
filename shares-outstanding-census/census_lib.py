"""Engine for the shares-outstanding census pipeline.

Every EDGAR fetch is throttled (~6 req/s, under SEC's 10 req/s limit) and
cached under DATA_DIR, so an interrupted run resumes where it left off and a
finished run re-runs fully offline.
"""

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


def throttled_get(session, url, stream=False, tries=3):
    """GET with a global throttle and simple retry/backoff on 403/429/5xx."""
    for attempt in range(tries):
        wait = THROTTLE_SECONDS - (time.monotonic() - _last_request_time[0])
        if wait > 0:
            time.sleep(wait)
        _last_request_time[0] = time.monotonic()
        try:
            resp = session.get(url, stream=stream, timeout=30)
            if resp.status_code == 200:
                return resp
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
