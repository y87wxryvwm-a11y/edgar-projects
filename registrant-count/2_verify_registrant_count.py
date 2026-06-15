"""Deterministic verification of registrant_count_<year>.csv. Offline; reads
only the cached indexes/headers and (optionally) the census population CSV.
Every check prints PASS/FAIL; the script raises at the end if any failed, so a
clean run is a green assertion suite.

What it proves:
  1. COMPLETENESS — the accession set is re-derived independently from the
     four cached quarterly master indexes (exact-form, deduped) and must equal
     the CSV's accession set exactly; per-form counts reported; one row per
     filing; no /A amendments.
  2. SHARED COLUMNS — CIK, SIC, Company Period, Filing Date, Accession match
     the census population_<year>.csv row-for-row (when configured), piggy-
     backing on that dataset's prior validation.
  3. STATE FIELDS — re-extracted by a second, independently written line-state-
     machine parser (not the regex one the build uses); the two must agree on
     every row. This is what guards the genuinely new columns.
  4. FORMAT — CIK int > 0; accession 10-2-6 digits; Filing Date in <year>;
     Company Period ISO-or-blank; State / State Incorporated short codes.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
in_filename = "registrant_count_2025.csv"
# -----------------------------------------------------------------------------

import os
import re

import pandas as pd

try:
    from config import DATA_DIR, USER_AGENT, SEED_CACHE_DIRS
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")
try:
    from config import CENSUS_POPULATION_CSV
except ImportError:
    CENSUS_POPULATION_CSV = ""

import registrant_lib as lib

directory = DATA_DIR.replace("\\", "/")
cache_dirs = [os.path.join(directory, "cache")] + \
    [d.replace("\\", "/") for d in SEED_CACHE_DIRS]
in_path = os.path.join(directory, in_filename)

df = pd.read_csv(in_path, dtype=str, keep_default_na=False)
print("loaded %s (%d rows)\n" % (in_path, len(df)))

results = []  # (name, ok, detail)


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))
    print("[%s] %s%s" % ("PASS" if ok else "FAIL", name,
                         ("  -- " + detail) if detail else ""))


# --- 1. COMPLETENESS: re-derive population straight from the indexes ----------

session = lib.make_session(USER_AGENT)
idx_rows = []
for q in (1, 2, 3, 4):
    text = lib.fetch_master_index(session, cache_dirs, year, q)
    idx_rows.extend(lib.parse_master_index(text))

idx_form = {}
for r in idx_rows:
    idx_form.setdefault(r["accession"], r["form"])
idx_acc = set(idx_form)

csv_acc = set(df["Accession Number"])
check("completeness: CSV accessions == index-derived accessions",
      csv_acc == idx_acc,
      "csv=%d index=%d missing=%d extra=%d" % (
          len(csv_acc), len(idx_acc),
          len(idx_acc - csv_acc), len(csv_acc - idx_acc)))
check("one row per filing (no duplicate accessions)",
      len(df) == df["Accession Number"].nunique(),
      "rows=%d unique=%d" % (len(df), df["Accession Number"].nunique()))

# per-form counts (informational + amendment guard)
from collections import Counter
form_counts = Counter(idx_form[a] for a in idx_acc)
print("    per-form (index):", dict(sorted(form_counts.items())))
check("no amendments (every form in {10-K,20-F,40-F})",
      set(form_counts) <= set(lib.ANNUAL_FORMS),
      "forms=%s" % sorted(form_counts))

# --- 2. SHARED COLUMNS vs census population ----------------------------------

if CENSUS_POPULATION_CSV and os.path.exists(CENSUS_POPULATION_CSV):
    cen = pd.read_csv(CENSUS_POPULATION_CSV, dtype=str, keep_default_na=False)
    check("census cross-check: same accession set",
          set(cen["accession"]) == csv_acc,
          "census=%d csv=%d" % (cen["accession"].nunique(), len(csv_acc)))
    m = df.merge(cen, left_on="Accession Number", right_on="accession",
                 how="inner", validate="one_to_one")
    cik_ok = (m["CIK"].astype(int) == m["cik"].astype(int)).all()
    sic_ok = (m["SIC"] == m["sic"]).all()
    date_ok = (m["Filing Date"] == m["date_filed"]).all()
    per_ok = (m["Company Period"] == m["period_of_report"].map(lib.fmt_date)).all()
    check("census cross-check: CIK matches", cik_ok,
          "mismatches=%d" % (m["CIK"].astype(int) != m["cik"].astype(int)).sum())
    check("census cross-check: SIC matches", sic_ok,
          "mismatches=%d" % (m["SIC"] != m["sic"]).sum())
    check("census cross-check: Filing Date matches", date_ok,
          "mismatches=%d" % (m["Filing Date"] != m["date_filed"]).sum())
    check("census cross-check: Company Period matches", per_ok,
          "mismatches=%d" % (m["Company Period"]
                             != m["period_of_report"].map(lib.fmt_date)).sum())
else:
    print("    (census cross-check skipped: CENSUS_POPULATION_CSV not set)")

# --- 3. STATE FIELDS --------------------------------------------------------
# (a) re-derive the HEADER values with a different parser (line state machine)
#     and confirm they reproduce the build's recorded header_state / header_soi;
# (b) confirm every published value is a faithful resolution: header value if
#     the header had one, else the EDGAR submissions value, else blank — with
#     API-sourced values re-read from the cached submissions JSON (not trusting
#     the build's own write);
# (c) provenance source flags are internally consistent.

prov_path = os.path.join(directory, in_filename.replace(".csv", "_provenance.csv"))
prov = pd.read_csv(prov_path, dtype=str, keep_default_na=False)
P = prov.set_index("Accession Number")


def alt_extract(header_text):
    """A deliberately different implementation from registrant_lib.parse_header:
    a line-by-line state machine over the FIRST filer block, tracking the
    current subsection. Returns (business_state, mail_state, state_of_incorp)."""
    seen_filer = 0
    subsec = None
    biz = mail = soi = ""
    for line in header_text.split("\n"):
        if line.rstrip() == "FILER:":
            seen_filer += 1
            subsec = None
            continue
        if seen_filer != 1:
            continue
        hm = re.match(r"^\t*([A-Z][A-Z &/]+):[ \t]*$", line)
        if hm:
            subsec = hm.group(1).strip()
            continue
        fm = re.match(r"^\t*([A-Z][A-Z &/]+):[ \t]*(\S.*?)[ \t]*$", line)
        if not fm:
            continue
        lbl, val = fm.group(1).strip(), fm.group(2).strip()
        if lbl == "STATE OF INCORPORATION" and subsec == "COMPANY DATA":
            soi = soi or val
        elif lbl == "STATE" and subsec == "BUSINESS ADDRESS":
            biz = biz or val
        elif lbl == "STATE" and subsec == "MAIL ADDRESS":
            mail = mail or val
    return biz, mail, soi


# (a) independent header re-derivation; (b) API-sourced values re-read from cache
hdr_state_bad, hdr_soi_bad, api_state_bad, api_soi_bad = [], [], [], []
for _, row in df.iterrows():
    acc = row["Accession Number"]
    p = P.loc[acc]
    biz, mail, soi = alt_extract(lib.fetch_sgml_header(session, cache_dirs, None, acc))
    if (biz or mail).upper() != p["header_state"]:
        hdr_state_bad.append((acc, (biz or mail).upper(), p["header_state"]))
    if soi.upper() != p["header_soi"]:
        hdr_soi_bad.append((acc, soi.upper(), p["header_soi"]))
    if p["state_source"] == "API":
        j = lib.fetch_submissions(session, cache_dirs, int(p["CIK"]))
        if lib.submissions_business_state(j) != row["State"]:
            api_state_bad.append((acc, lib.submissions_business_state(j), row["State"]))
    if p["soi_source"] == "API":
        j = lib.fetch_submissions(session, cache_dirs, int(p["CIK"]))
        if lib.submissions_state_of_incorp(j) != row["State Incorporated"]:
            api_soi_bad.append((acc, lib.submissions_state_of_incorp(j), row["State Incorporated"]))

check("HEADER State reproduced by an independent parser on every row",
      not hdr_state_bad, "mismatches=%d %s" % (len(hdr_state_bad), hdr_state_bad[:5]))
check("HEADER State Incorporated reproduced by an independent parser on every row",
      not hdr_soi_bad, "mismatches=%d %s" % (len(hdr_soi_bad), hdr_soi_bad[:5]))
check("API-sourced State equals the cached submissions value",
      not api_state_bad, "mismatches=%d %s" % (len(api_state_bad), api_state_bad[:5]))
check("API-sourced State Incorporated equals the cached submissions value",
      not api_soi_bad, "mismatches=%d %s" % (len(api_soi_bad), api_soi_bad[:5]))

# (c) resolution + provenance consistency
df_state = dict(zip(df["Accession Number"], df["State"]))
df_soi = dict(zip(df["Accession Number"], df["State Incorporated"]))

def resolved_ok(pub, src, hdr):
    if src == "HEADER":
        return pub == hdr and hdr != ""
    if src == "API":
        return hdr == "" and pub != ""
    if src in ("NONE", "XBRL_CONFLICT"):   # both publish blank
        return pub == "" and hdr == ""
    return False

res_state_bad = [a for a in df["Accession Number"]
                 if not resolved_ok(df_state[a], P.loc[a]["state_source"], P.loc[a]["header_state"])]
res_soi_bad = [a for a in df["Accession Number"]
               if not resolved_ok(df_soi[a], P.loc[a]["soi_source"], P.loc[a]["header_soi"])]
check("State / source / header_state are mutually consistent on every row",
      not res_state_bad, "bad=%d %s" % (len(res_state_bad), res_state_bad[:5]))
check("State Incorporated / source / header_soi are mutually consistent on every row",
      not res_soi_bad, "bad=%d %s" % (len(res_soi_bad), res_soi_bad[:5]))

# every XBRL_CONFLICT drop must rest on real evidence: an API value AND a
# differing as-filed XBRL value (recorded in provenance), never a spurious drop
if "soi_source" in prov and (prov["soi_source"] == "XBRL_CONFLICT").any():
    conf = prov[prov["soi_source"] == "XBRL_CONFLICT"]
    bad = conf[(conf["api_soi"] == "") | (conf["xbrl_soi"] == "")]
    check("every XBRL_CONFLICT drop has both an API value and a conflicting XBRL value",
          len(bad) == 0, "drops=%d unevidenced=%d" % (len(conf), len(bad)))

# --- 4. FORMAT ----------------------------------------------------------------

cik_int_ok = df["CIK"].str.fullmatch(r"\d+").all() and (df["CIK"].astype(int) > 0).all()
check("CIK is a positive integer", cik_int_ok)
acc_ok = df["Accession Number"].str.fullmatch(r"\d{10}-\d{2}-\d{6}").all()
check("Accession Number is NNNNNNNNNN-NN-NNNNNN", acc_ok)
date_ok = df["Filing Date"].str.fullmatch(r"%d-\d{2}-\d{2}" % year).all()
check("Filing Date is in %d" % year, date_ok)
per_ok = df["Company Period"].str.fullmatch(r"(\d{4}-\d{2}-\d{2})?").all()
check("Company Period is ISO date or blank", per_ok)
st_ok = df["State"].str.fullmatch(r"[A-Z0-9]{0,2}").all()
check("State is a 2-char EDGAR code or blank", st_ok,
      "bad=%s" % df.loc[~df["State"].str.fullmatch(r"[A-Z0-9]{0,2}"), "State"].unique()[:10])
soi_ok = df["State Incorporated"].str.fullmatch(r"[A-Z0-9]{0,2}").all()
check("State Incorporated is a 2-char EDGAR code or blank", soi_ok,
      "bad=%s" % df.loc[~df["State Incorporated"].str.fullmatch(r"[A-Z0-9]{0,2}"),
                        "State Incorporated"].unique()[:10])

# --- 5. NEW FLAG COLUMNS ------------------------------------------------------

for col in ["BDC", "ABS", "multi", "wksi", "shell", "src", "egc",
            "sec_12b", "sec_12g", "sec_15d"]:
    check("%s is 0/1" % col, df[col].isin(["0", "1"]).all(),
          "bad=%s" % df.loc[~df[col].isin(["0", "1"]), col].unique()[:5])
check("afs is NAF/AF/LAF or blank", df["afs"].isin(["", "NAF", "AF", "LAF"]).all(),
      "bad=%s" % df.loc[~df["afs"].isin(["", "NAF", "AF", "LAF"]), "afs"].unique()[:5])

regsum = (df[["sec_12b", "sec_12g", "sec_15d"]] == "1").sum(axis=1)
check("exactly one of sec_12b/12g/15d is 1 on every row", (regsum == 1).all(),
      "rows not summing to 1: %d" % (regsum != 1).sum())

text_bad = sum(not (u.startswith("https://www.sec.gov/Archives/edgar/data/")
                    and u.endswith(a + ".txt"))
               for u, a in zip(df["text_url"], df["Accession Number"]))
idx_bad = sum(not u.endswith(a + "-index.htm")
              for u, a in zip(df["filing_url"], df["Accession Number"]))
check("text_url is the full-submission .txt for the accession", text_bad == 0,
      "bad=%d" % text_bad)
check("filing_url is the filing index page for the accession", idx_bad == 0,
      "bad=%d" % idx_bad)

# independent re-derivation of BDC / ABS / multi straight from the cached headers
bdc_bad = abs_bad = multi_bad = 0
for _, row in df.iterrows():
    fl = lib.parse_header(lib.fetch_sgml_header(
        session, cache_dirs, None, row["Accession Number"]))["filers"]
    bdc = "1" if any((f.get("file_number", "") or "").strip().startswith("814")
                     for f in fl) else "0"
    abs_ = "1" if any(f.get("sic", "") == "6189" for f in fl) else "0"
    bdc_bad += (bdc != row["BDC"])
    abs_bad += (abs_ != row["ABS"])
    multi_bad += (("1" if len(fl) > 1 else "0") != row["multi"])
check("BDC reproduced from the header file numbers on every row", bdc_bad == 0,
      "mismatches=%d" % bdc_bad)
check("ABS reproduced from any filer's SIC=6189 on every row", abs_bad == 0,
      "mismatches=%d" % abs_bad)
check("multi reproduced from the header filer count on every row", multi_bad == 0,
      "mismatches=%d" % multi_bad)

# --- coverage stats (informational) ------------------------------------------

print("\ncoverage:")
print("  rows:                       %d" % len(df))
print("  distinct CIKs:              %d" % df["CIK"].nunique())
print("  blank State:                %d" % (df["State"] == "").sum())
print("  blank State Incorporated:   %d" % (df["State Incorporated"] == "").sum())
print("  top States:", dict(Counter(df.loc[df['State'] != '', 'State']).most_common(8)))
print("  top States of Incorp:",
      dict(Counter(df.loc[df['State Incorporated'] != '', 'State Incorporated']).most_common(8)))
print("  flags (=1): " + " ".join("%s=%d" % (c, (df[c] == "1").sum())
      for c in ["BDC", "ABS", "multi", "wksi", "shell", "src", "egc",
                "sec_12b", "sec_12g", "sec_15d"]))
print("  afs:", dict(Counter(df["afs"])))

# --- verdict ------------------------------------------------------------------

failed = [n for n, ok, _ in results if not ok]
print("\n%d/%d checks passed." % (len(results) - len(failed), len(results)))
if failed:
    raise SystemExit("FAILED: " + "; ".join(failed))
print("ALL CHECKS PASSED.")
