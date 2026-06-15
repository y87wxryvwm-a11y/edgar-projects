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
           "State Incorporated", "Accession Number"]

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
        p = {"sic": "", "state_of_incorp": "", "business_state": "", "mail_state": ""}
        cik = int(r["index_cik"].lstrip("0") or 0)
    recs.append({
        "cik": cik, "period": lib.fmt_date(parsed["period_of_report"]),
        "filed": r["date_filed"], "sic": p["sic"], "acc": acc,
        "form": r["form"], "txt": txt,
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

# --- 4. Resolve + provenance -------------------------------------------------

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
state_src, soi_src = Counter(), Counter()
for rec in recs:
    c, a = rec["cik"], rec["acc"]
    state, ssrc = resolve(rec["h_state"], api_state.get(c, ""), xbrl_state.get(a, ""))
    soi, isrc = resolve(rec["h_soi"], api_soi.get(c, ""), xbrl_soi.get(a, ""))
    state_src[ssrc] += 1
    soi_src[isrc] += 1
    out.append({
        "CIK": c, "Company Period": rec["period"], "Filing Date": rec["filed"],
        "SIC": rec["sic"], "State": state, "State Incorporated": soi,
        "Accession Number": a,
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
print("blank State:                %d" % (df["State"] == "").sum())
print("blank State Incorporated:   %d" % (df["State Incorporated"] == "").sum())
print("distinct CIKs:              %d" % df["CIK"].nunique())
print("top States:", dict(Counter(df.loc[df['State'] != '', 'State']).most_common(8)))
print("top States of Incorp:",
      dict(Counter(df.loc[df['State Incorporated'] != '', 'State Incorporated']).most_common(8)))
