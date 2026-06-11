"""Build the census population: every filing whose form type is exactly
10-K, 20-F, or 40-F in the year's four EDGAR quarterly indexes, one row per
filing (deduped by accession), with the SGML-header identity fields.

Filings whose header SIC is 6189 (Asset-Backed Securities) stay in the file
but are marked excluded_abs=True; downstream scripts work on the rest.

Output: population_{year}.csv in DATA_DIR.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
# -----------------------------------------------------------------------------

import os

import pandas as pd

try:
    from config import DATA_DIR, USER_AGENT
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT and DATA_DIR."
    )

import census_lib as lib

directory = DATA_DIR.replace("\\", "/")
index_cache = os.path.join(directory, "cache", "indexes")
header_cache = os.path.join(directory, "cache", "headers")
out_path = os.path.join(directory, "population_%d.csv" % year)

session = lib.make_session(USER_AGENT)

# --- 1. Quarterly indexes -> exact-form rows, deduped by accession -----------

rows = []
for q in (1, 2, 3, 4):
    text = lib.fetch_master_index(session, index_cache, year, q)
    quarter_rows = lib.parse_master_index(text)
    print("QTR%d: %d annual-filing rows" % (q, len(quarter_rows)), flush=True)
    rows.extend(quarter_rows)

# A combined filing is listed once per co-registrant CIK with the same
# accession; collapse to one filing, remembering every index CIK.
by_accession = {}
for r in rows:
    entry = by_accession.setdefault(
        r["accession"], dict(r, index_ciks=set(), txt_paths=[]))
    entry["index_ciks"].add(r["index_cik"])
    entry["txt_paths"].append(r["txt_path"])
print("unique filings: %d" % len(by_accession), flush=True)

# --- 2. SGML header per filing: canonical name / CIK / SIC -------------------

population = []
items = sorted(by_accession.values(), key=lambda r: r["accession"])
for i, r in enumerate(items, 1):
    header = lib.fetch_sgml_header(
        session, header_cache, sorted(r["txt_paths"])[0], r["accession"])
    parsed = lib.parse_sgml_header(header)
    filers = parsed["filers"]
    if filers and filers[0]["cik"].isdigit():
        primary = filers[0]
        cik = int(primary["cik"])
    else:
        # header didn't parse — fall back to the index row, flag for review
        primary = {"name": r["index_name"], "sic": "", "sic_desc": ""}
        cik = int(sorted(r["index_ciks"], key=int)[0])
    accession_nodash = r["accession"].replace("-", "")
    filer_ciks = ";".join(f["cik"] for f in filers)
    if not any(f["cik"] for f in filers):
        filer_ciks = str(cik)
    population.append({
        "accession": r["accession"],
        "form": r["form"],
        "date_filed": r["date_filed"],
        "period_of_report": parsed["period_of_report"],
        "cik": cik,
        "company_name": primary["name"] or r["index_name"],
        "sic": primary["sic"],
        "sic_desc": primary["sic_desc"],
        "n_filers": len(filers),
        "all_ciks": filer_ciks,
        "all_sics": ";".join(f["sic"] for f in filers),
        "filing_index_url": "https://www.sec.gov/Archives/edgar/data/%d/%s/%s-index.htm"
                            % (cik, accession_nodash, r["accession"]),
        "excluded_abs": any(f["sic"] == lib.ABS_SIC for f in filers),
        # describes the sic column (primary filer); co-filer SICs are in all_sics
        "sic_missing": not primary["sic"],
        "header_parse_failed": not filers,
        "header_truncated": header.startswith(lib.TRUNCATED_SENTINEL),
    })
    if i % 250 == 0 or i == len(items):
        print("[%d/%d] headers fetched" % (i, len(items)), flush=True)

# --- 3. Write + summary -------------------------------------------------------

df = pd.DataFrame(population).sort_values("accession").reset_index(drop=True)
df.to_csv(out_path, index=False, encoding="utf-8", lineterminator="\n")
print("\nwrote %s (%d filings)" % (out_path, len(df)))

print("\nper form:")
print(df.groupby("form").size().to_string())
print("\nABS-excluded per form:")
print(df[df["excluded_abs"]].groupby("form").size().to_string())
print("\nin scope (non-ABS): %d" % (~df["excluded_abs"]).sum())
print("multi-filer filings: %d" % (df["n_filers"] > 1).sum())
print("primary SIC missing: %d" % df["sic_missing"].sum())
print("header parse failed: %d" % df["header_parse_failed"].sum())
print("header truncated:    %d" % df["header_truncated"].sum())
