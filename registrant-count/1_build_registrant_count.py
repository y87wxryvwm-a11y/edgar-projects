"""Build the registrant-count dataset: one row per annual filing filed in the
year (form type exactly 10-K / 20-F / 40-F in the four EDGAR quarterly
indexes, deduped by accession — the same population the shares-outstanding
census uses), with seven columns:

    CIK, Company Period, Filing Date, SIC, State, State Incorporated,
    Accession Number

CIK / Company Period (CONFORMED PERIOD OF REPORT) / Filing Date / SIC /
Accession Number all come from the filing's SGML header (the primary, i.e.
first, FILER block) and match the census population file row-for-row.

The two location fields, in EDGAR State-or-Country codes (CA / DE / M0=Japan /
A6=Ontario ...), upper-cased (a few filers key "wa"/"ct" lowercase):

  * "State" — the primary filer's BUSINESS-ADDRESS State from the header,
    falling back to its MAIL-ADDRESS State.
  * "State Incorporated" — the primary filer's STATE OF INCORPORATION from the
    header.

The header omits the State-of-Incorporation line for a chunk of filings (and a
few omit the address state). When FILL_BLANKS_FROM_API is True those blanks are
filled from EDGAR's own authoritative company record (the submissions API),
which stores both fields in the same code space — but a fill is KEPT only when
the filing's own as-filed inline-XBRL does not contradict it. EDGAR's record is
reliable (wherever the header DOES state a value it agrees with the record), yet
where the header is silent the record can be stale (a reincorporation) or
conflated (a foreign filer's location mistagged as its incorporation); the
filing's own XBRL catches those, and they are dropped to blank (soi_source
XBRL_CONFLICT) rather than published wrong. Whatever neither the header nor a
validated record provides (mostly funds / trusts / foreign issuers with no
structured state of incorporation) stays blank. Set FILL_BLANKS_FROM_API = False
for a pure as-filed-header dataset.

ABS issuers (header SIC 6189) are annual filers and are INCLUDED; filter on
SIC == 6189 to drop them.

Outputs in DATA_DIR:
  registrant_count_<year>.csv             the 7-column dataset
  registrant_count_<year>_provenance.csv  per-row source (HEADER / API /
                                          XBRL_CONFLICT / NONE) + the raw
                                          header / EDGAR / XBRL values behind
                                          each fill, for audit and verification
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
out_filename = "registrant_count_2025.csv"
FILL_BLANKS_FROM_API = True   # fill header-blank State / State Incorporated
                              # from EDGAR's submissions record (clean codes)
# -----------------------------------------------------------------------------

import os
from collections import Counter

import pandas as pd

try:
    from config import DATA_DIR, USER_AGENT, SEED_CACHE_DIRS
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT and DATA_DIR."
    )

import registrant_lib as lib

COLUMNS = ["CIK", "Company Period", "Filing Date", "SIC", "State",
           "State Incorporated", "Accession Number",
           "BDC", "ABS", "multi", "text_url", "filing_url",
           "wksi", "shell", "afs", "src", "egc",
           "sec_12b", "sec_12g", "sec_15d"]

directory = DATA_DIR.replace("\\", "/")
os.makedirs(directory, exist_ok=True)
cache_dirs = [os.path.join(directory, "cache")] + \
    [d.replace("\\", "/") for d in SEED_CACHE_DIRS]
out_path = os.path.join(directory, out_filename)
prov_path = os.path.join(directory, out_filename.replace(".csv", "_provenance.csv"))

session = lib.make_session(USER_AGENT)

# --- 1. Quarterly indexes -> exact-form rows, deduped by accession -----------

rows = []
for q in (1, 2, 3, 4):
    text = lib.fetch_master_index(session, cache_dirs, year, q)
    quarter_rows = lib.parse_master_index(text)
    print("QTR%d: %d annual-filing rows" % (q, len(quarter_rows)), flush=True)
    rows.extend(quarter_rows)

by_accession = {}
for r in rows:
    entry = by_accession.setdefault(r["accession"], dict(r, txt_paths=[]))
    entry["txt_paths"].append(r["txt_path"])
items = sorted(by_accession.values(), key=lambda r: r["accession"])
print("unique filings: %d" % len(items), flush=True)

# --- 2. Header per filing -> primary filer fields (clean as-filed codes) -----

recs = []
for i, r in enumerate(items, 1):
    acc, txt = r["accession"], sorted(r["txt_paths"])[0]
    parsed = lib.parse_header(lib.fetch_sgml_header(session, cache_dirs, txt, acc))
    filers = parsed["filers"]
    if filers and filers[0]["cik"].isdigit():
        p = filers[0]
        cik = int(p["cik"])
    else:
        p = {"sic": "", "state_of_incorp": "", "business_state": "",
             "mail_state": "", "file_number": ""}
        cik = int(r["index_cik"].lstrip("0") or 0)
    recs.append({
        "cik": cik, "period": lib.fmt_date(parsed["period_of_report"]),
        "filed": r["date_filed"], "sic": p["sic"], "acc": acc,
        "form": r["form"], "txt": txt,
        "n_filers": len(filers),
        "file_numbers": [f.get("file_number", "") for f in filers],
        "all_sics": [f.get("sic", "") for f in filers],
        "h_state": p["business_state"].upper() or p["mail_state"].upper(),
        "h_soi": p["state_of_incorp"].upper(),
    })
    if i % 1000 == 0 or i == len(items):
        print("[%d/%d] headers parsed" % (i, len(items)), flush=True)

# --- 3. Fill header-blank fields from EDGAR's record, validated by XBRL -------

# An API fill is KEPT only when the filing's own as-filed inline-XBRL doesn't
# contradict it. The header is the trusted side; where it is silent, EDGAR's
# record is a good fill EXCEPT when it is stale or conflated (a reincorporation,
# or a foreign filer whose location is mistagged as its incorporation) — those
# the filing's own XBRL catches and we drop rather than publish a wrong value.

api_state, api_soi = {}, {}
xbrl_state, xbrl_soi = {}, {}          # acc -> decoded EDGAR code from XBRL ("" if none)
xbrl_state_raw, xbrl_soi_raw = {}, {}  # acc -> raw XBRL display name (for provenance)
if FILL_BLANKS_FROM_API:
    need_api = sorted({rec["cik"] for rec in recs
                       if not rec["h_state"] or not rec["h_soi"]})
    print("\nfilling %d CIKs with blank fields from the submissions API..." % len(need_api),
          flush=True)
    for i, c in enumerate(need_api, 1):
        j = lib.fetch_submissions(session, cache_dirs, c)
        api_state[c] = lib.submissions_business_state(j)
        api_soi[c] = lib.submissions_state_of_incorp(j)
        if i % 250 == 0 or i == len(need_api):
            print("  [%d/%d]" % (i, len(need_api)), flush=True)

    name2code, codeset = lib.incorporation_name_to_code_map(cache_dirs)
    # parse XBRL only for the filings that actually receive a fill (fast)
    fills = [rec for rec in recs
             if (not rec["h_state"] and api_state.get(rec["cik"]))
             or (not rec["h_soi"] and api_soi.get(rec["cik"]))]
    print("validating %d candidate fills against each filing's own XBRL..." % len(fills),
          flush=True)
    for i, rec in enumerate(fills, 1):
        doc = lib.fetch_primary_document(session, cache_dirs, rec["txt"],
                                         rec["acc"], rec["form"])
        if doc and (b"EntityAddressStateOrProvince" in doc
                    or b"EntityIncorporationStateCountryCode" in doc):
            sn = lib.extract_dei_state(doc, lib.ADDR_STATE_LOCALNAME)
            inn = lib.extract_dei_state(doc, lib.INCORP_LOCALNAME)
            xbrl_state_raw[rec["acc"]] = sn
            xbrl_soi_raw[rec["acc"]] = inn
            xbrl_state[rec["acc"]] = lib.decode_incorp_name(sn, name2code, codeset)
            xbrl_soi[rec["acc"]] = lib.decode_incorp_name(inn, name2code, codeset)
        if i % 100 == 0 or i == len(fills):
            print("  [%d/%d]" % (i, len(fills)), flush=True)

# --- 3b. Cover facts: dei checkboxes (wksi/shell/afs/src/egc) and the 12(b) /
# 12(g) registration sections. The registration sections are SCRAPED from the
# cover text AND cross-checked against the filing's XBRL (Security12b/gTitle):
# a security counts as registered if either source shows it. Cache-only doc
# parse + text scrape over the in-scope cached documents; ABS docs aren't
# cached, so ABS fall to no-checkboxes / 15(d). Cached to a CSV (re-runs fast).

cover_path = os.path.join(directory, "cover_facts_%d.csv" % year)
cover = {}
if os.path.exists(cover_path):
    _cf = pd.read_csv(cover_path, dtype=str, keep_default_na=False)
    cover = {row["acc"]: dict(row) for _, row in _cf.iterrows()}
    print("\nloaded cover-facts cache: %d filings" % len(cover))
else:
    print("\nparsing cover facts (one-time over the cached documents, ~20 min)...",
          flush=True)
    rows_cf, agree12b = [], Counter()
    for i, rec in enumerate(recs, 1):
        a = rec["acc"]
        doc = lib.read_cached_doc(cache_dirs, a)
        text = lib.read_cached_text(cache_dirs, a)
        f = (lib.extract_cover_facts(doc) if doc else None) or {}
        # each checkbox: the XBRL dei tag if present, else scrape the cover text
        sc = lib.scrape_cover_checkboxes(text) if doc else {}
        def resolve_box(k):
            return f.get(k, "") or sc.get(k, "")
        s12b, s12g = lib.cover_has_12b_security(text), lib.cover_has_12g_security(text)
        x12b, x12g = bool(f.get("has_12b")), bool(f.get("has_12g"))
        if doc is not None and text is not None:   # only where both signals exist
            agree12b["agree" if s12b == x12b else "disagree"] += 1
        row = {"acc": a, "wksi": resolve_box("wksi"), "shell": resolve_box("shell"),
               "src": resolve_box("src"), "egc": resolve_box("egc"), "afs": resolve_box("afs"),
               "x12b": "1" if x12b else "0", "x12g": "1" if x12g else "0",
               "s12b": "1" if s12b else "0", "s12g": "1" if s12g else "0"}
        cover[a] = row
        rows_cf.append(row)
        if i % 500 == 0 or i == len(recs):
            print("  [%d/%d]" % (i, len(recs)), flush=True)
    pd.DataFrame(rows_cf).to_csv(cover_path, index=False, encoding="utf-8",
                                 lineterminator="\n")
    print("12(b) scrape vs XBRL where both available:", dict(agree12b))

# --- 4. Resolve + assemble + provenance --------------------------------------

def resolve(header_val, api_val, xbrl_code):
    """header wins; else the API fill unless the filing's own XBRL decodes to a
    DIFFERENT code (then drop). Returns (value, source)."""
    if header_val:
        return header_val, "HEADER"
    if api_val:
        if xbrl_code and xbrl_code != api_val:
            return "", "XBRL_CONFLICT"   # API contradicted by the filing — drop
        return api_val, "API"
    return "", "NONE"


out, prov = [], []
state_src, soi_src, sec_src = Counter(), Counter(), Counter()
for rec in recs:
    c, a = rec["cik"], rec["acc"]
    state, ssrc = resolve(rec["h_state"], api_state.get(c, ""), xbrl_state.get(a, ""))
    soi, isrc = resolve(rec["h_soi"], api_soi.get(c, ""), xbrl_soi.get(a, ""))
    state_src[ssrc] += 1
    soi_src[isrc] += 1

    cv = cover.get(a, {})
    has_12b = cv.get("x12b") == "1" or cv.get("s12b") == "1"
    has_12g = cv.get("x12g") == "1" or cv.get("s12g") == "1"
    sec_12b = "1" if has_12b else "0"
    sec_12g = "1" if (not has_12b and has_12g) else "0"
    sec_15d = "1" if (not has_12b and not has_12g) else "0"
    sec_src["12b" if has_12b else "12g" if has_12g else "15d"] += 1

    out.append({
        "CIK": c, "Company Period": rec["period"], "Filing Date": rec["filed"],
        "SIC": rec["sic"], "State": state, "State Incorporated": soi,
        "Accession Number": a,
        "BDC": "1" if any(fn.strip().startswith("814") for fn in rec["file_numbers"]) else "0",
        "ABS": "1" if any(s == lib.ABS_SIC for s in rec["all_sics"]) else "0",
        "multi": "1" if rec["n_filers"] > 1 else "0",
        "text_url": lib.SEC_BASE + "/Archives/" + rec["txt"],
        "filing_url": "%s/Archives/edgar/data/%d/%s/%s-index.htm"
                      % (lib.SEC_BASE, c, a.replace("-", ""), a),
        "wksi": cv.get("wksi", "") or "0", "shell": cv.get("shell", "") or "0",
        "afs": cv.get("afs", ""), "src": cv.get("src", "") or "0",
        "egc": cv.get("egc", "") or "0",
        "sec_12b": sec_12b, "sec_12g": sec_12g, "sec_15d": sec_15d,
    })
    prov.append({
        "Accession Number": a, "CIK": c,
        "State": state, "state_source": ssrc, "header_state": rec["h_state"],
        "api_state": api_state.get(c, ""), "xbrl_state": xbrl_state_raw.get(a, ""),
        "State Incorporated": soi, "soi_source": isrc, "header_soi": rec["h_soi"],
        "api_soi": api_soi.get(c, ""), "xbrl_soi": xbrl_soi_raw.get(a, ""),
    })

# --- 5. Write + summary ------------------------------------------------------

df = pd.DataFrame(out, columns=COLUMNS) \
    .sort_values("Accession Number").reset_index(drop=True)
df.to_csv(out_path, index=False, encoding="utf-8", lineterminator="\n")
pd.DataFrame(prov).sort_values("Accession Number").to_csv(
    prov_path, index=False, encoding="utf-8", lineterminator="\n")
print("\nwrote %s (%d rows)" % (out_path, len(df)))
print("wrote %s" % prov_path)
print("\nState source:              ", dict(state_src))
print("State Incorporated source: ", dict(soi_src))
print("blank State Incorporated:   %d" % (df["State Incorporated"] == "").sum())
print("distinct CIKs:              %d" % df["CIK"].nunique())
print("\nflag counts (=1):  BDC %d | ABS %d | multi %d | wksi %d | shell %d | src %d | egc %d"
      % tuple((df[c] == "1").sum() for c in
              ["BDC", "ABS", "multi", "wksi", "shell", "src", "egc"]))
print("afs:", dict(Counter(df["afs"])))
print("registration: 12b %d | 12g %d | 15d %d  (sum %d == %d rows)"
      % ((df["sec_12b"] == "1").sum(), (df["sec_12g"] == "1").sum(),
         (df["sec_15d"] == "1").sum(),
         (df[["sec_12b", "sec_12g", "sec_15d"]] == "1").sum().sum(), len(df)))
