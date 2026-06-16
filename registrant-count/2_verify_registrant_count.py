"""Deterministic verification of registrant_count_<year>.csv. Offline; reads
only the cached indexes/headers (and optionally the census population CSV).
Every check prints PASS/FAIL; the script raises at the end if any failed.

The dataset is ONE ROW PER REGISTRANT CIK: a combined annual report (utilities,
etc.) is exploded so every FILER block becomes its own row with that CIK's own
company-specific columns, and each CIK is then represented by its LAST non-
amended annual report of the calendar year. So the row key is the CIK, not the
accession. What this proves:

  1. COVERAGE — re-derive every filer CIK from every 2025 annual filing's header
     (an independent line-state-machine parser, not the build's regex one); the
     CSV's CIK set must equal that universe exactly, one row per CIK.
  2. LATEST-FILING — each CIK's row must point to that CIK's latest-filed annual
     report among all its filings in the year (the dedup rule).
  3. PER-CIK FIELDS — SIC / State / State of Incorporation / BDC / ABS / multi on
     each row are re-derived from THAT CIK's own filer block in its accession and
     must match (State / State Inc. allowing the EDGAR-record fill).
  4. STATUS / FORMAT — every status flag filled (no blanks), domains, registration
     mutually exclusive, no fill manufactures an impossible LAF+SRC, URL shape.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
in_filename = "registrant_count_2025.csv"
# -----------------------------------------------------------------------------

import os
import re
from collections import Counter

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


# --- independent header parser: ALL filer blocks (line state machine) ---------

def alt_filers(header_text):
    """Deliberately different from registrant_lib.parse_header: a line state
    machine that walks every FILER block, tracking the current subsection.
    Returns a list of dicts {cik, sic, biz, mail, soi, filenum} in file order."""
    blocks, cur, subsec = [], None, None
    for line in header_text.split("\n"):
        if line.rstrip().rstrip(":") in ("FILER", "FILED BY") and line.strip().endswith(":") \
                and not line.startswith("\t\t"):
            cur = {"cik": "", "sic": "", "biz": "", "mail": "", "soi": "", "filenum": ""}
            blocks.append(cur)
            subsec = None
            continue
        if cur is None:
            continue
        hm = re.match(r"^\t*([A-Z][A-Z &/]+):[ \t]*$", line)
        if hm:
            subsec = hm.group(1).strip()
            continue
        fm = re.match(r"^\t*([A-Z][A-Z &/()0-9]+):[ \t]*(\S.*?)[ \t]*$", line)
        if not fm:
            continue
        lbl, val = fm.group(1).strip(), fm.group(2).strip()
        if lbl == "CENTRAL INDEX KEY":
            cur["cik"] = cur["cik"] or val.lstrip("0")
        elif lbl == "STANDARD INDUSTRIAL CLASSIFICATION":
            m = re.search(r"\[(\d{3,4})\]", val)
            if m and not cur["sic"]:
                cur["sic"] = m.group(1).zfill(4)
        elif lbl == "STATE OF INCORPORATION" and subsec == "COMPANY DATA":
            cur["soi"] = cur["soi"] or val
        elif lbl == "SEC FILE NUMBER" and subsec == "FILING VALUES":
            cur["filenum"] = cur["filenum"] or val
        elif lbl == "STATE" and subsec == "BUSINESS ADDRESS":
            cur["biz"] = cur["biz"] or val
        elif lbl == "STATE" and subsec == "MAIL ADDRESS":
            cur["mail"] = cur["mail"] or val
    return [b for b in blocks if b["cik"]]


# --- 1. Re-derive the registrant universe from the indexes + headers ----------

session = lib.make_session(USER_AGENT)
idx_form, idx_date = {}, {}
for q in (1, 2, 3, 4):
    for r in lib.parse_master_index(lib.fetch_master_index(session, cache_dirs, year, q)):
        idx_form.setdefault(r["accession"], r["form"])
        idx_date.setdefault(r["accession"], r["date_filed"])
idx_acc = set(idx_form)

# parse every filing's header once -> per-CIK filings + per-(acc,cik) filer block
cik_filings = {}            # cik(str) -> list of (date_filed, accession)
filer_at = {}               # (acc, cik) -> filer-block dict
nfilers = {}                # acc -> filer count
for i, acc in enumerate(sorted(idx_acc), 1):
    blocks = alt_filers(lib.fetch_sgml_header(session, cache_dirs, None, acc))
    nfilers[acc] = len(blocks)
    for b in blocks:
        cik_filings.setdefault(b["cik"], []).append((idx_date.get(acc, ""), acc))
        filer_at[(acc, b["cik"])] = b
    if i % 1500 == 0 or i == len(idx_acc):
        print("  [%d/%d] headers parsed (independent)" % (i, len(idx_acc)), flush=True)

expected_ciks = set(cik_filings)
csv_ciks = set(df["CIK"])
print()
check("no amendments (every form in {10-K,20-F,40-F})",
      set(idx_form.values()) <= set(lib.ANNUAL_FORMS), "forms=%s" % sorted(set(idx_form.values())))
check("one row per CIK (no duplicate CIKs)",
      len(df) == df["CIK"].nunique(), "rows=%d unique=%d" % (len(df), df["CIK"].nunique()))
check("coverage: CSV CIK set == every filer CIK across all 2025 filings",
      csv_ciks == expected_ciks,
      "csv=%d universe=%d missing=%d extra=%d" % (
          len(csv_ciks), len(expected_ciks),
          len(expected_ciks - csv_ciks), len(csv_ciks - expected_ciks)))
check("every row's accession is a real 2025 annual filing",
      set(df["Accession Number"]) <= idx_acc,
      "unknown=%d" % len(set(df["Accession Number"]) - idx_acc))

# --- 2. LATEST-FILING dedup rule ---------------------------------------------
# each CIK's row must point to the latest-filed (date, accession) among its filings
row_acc = dict(zip(df["CIK"], df["Accession Number"]))
late_bad = []
for cik, flist in cik_filings.items():
    want = max(flist)[1]            # latest (date_filed, accession)
    if row_acc.get(cik) != want:
        late_bad.append((cik, row_acc.get(cik), want))
check("each CIK's row is its LATEST-filed annual report of the year",
      not late_bad, "wrong=%d %s" % (len(late_bad), late_bad[:4]))

# --- 3. PER-CIK company-specific fields re-derived from that CIK's filer block -
prov_path = os.path.join(directory, in_filename.replace(".csv", "_provenance.csv"))
P = pd.read_csv(prov_path, dtype=str, keep_default_na=False).set_index("CIK")

sic_bad = state_bad = soi_bad = bdc_bad = abs_bad = multi_bad = 0
for _, r in df.iterrows():
    cik, acc = r["CIK"], r["Accession Number"]
    b = filer_at.get((acc, cik))
    if b is None:
        sic_bad += 1
        continue
    if (b["sic"] or "") != r["SIC"]:
        sic_bad += 1
    # State: header business->mail; blank may be EDGAR-record filled (prov source)
    hstate = (b["biz"] or b["mail"]).upper()
    src = P.loc[cik]["state_source"] if cik in P.index else ""
    if not (r["State"] == hstate or (hstate == "" and src in ("API", "NONE"))):
        state_bad += 1
    hsoi = b["soi"].upper()
    isrc = P.loc[cik]["soi_source"] if cik in P.index else ""
    if not (r["State Incorporated"] == hsoi
            or (hsoi == "" and isrc in ("API", "NONE", "XBRL_CONFLICT"))):
        soi_bad += 1
    if ("1" if b["filenum"].strip().startswith("814") else "0") != r["BDC"]:
        bdc_bad += 1
    if ("1" if b["sic"] == "6189" else "0") != r["ABS"]:
        abs_bad += 1
    if ("1" if nfilers.get(acc, 1) > 1 else "0") != r["multi"]:
        multi_bad += 1
check("SIC re-derived from each CIK's own filer block", sic_bad == 0, "mismatches=%d" % sic_bad)
check("State re-derived from each CIK's filer block (or EDGAR-record fill)",
      state_bad == 0, "mismatches=%d" % state_bad)
check("State Incorporated re-derived from each CIK's filer block (or fill)",
      soi_bad == 0, "mismatches=%d" % soi_bad)
check("BDC re-derived from each CIK's own SEC file number", bdc_bad == 0, "mismatches=%d" % bdc_bad)
check("ABS re-derived from each CIK's own SIC=6189", abs_bad == 0, "mismatches=%d" % abs_bad)
check("multi re-derived from the filing's filer count", multi_bad == 0, "mismatches=%d" % multi_bad)

# --- 4. FORMAT ----------------------------------------------------------------

check("CIK is a positive integer",
      df["CIK"].str.fullmatch(r"\d+").all() and (df["CIK"].astype(int) > 0).all())
check("Accession Number is NNNNNNNNNN-NN-NNNNNN",
      df["Accession Number"].str.fullmatch(r"\d{10}-\d{2}-\d{6}").all())
check("Filing Date is in %d" % year, df["Filing Date"].str.fullmatch(r"%d-\d{2}-\d{2}" % year).all())
check("Company Period is ISO date or blank", df["Company Period"].str.fullmatch(r"(\d{4}-\d{2}-\d{2})?").all())
check("State is a 2-char EDGAR code or blank", df["State"].str.fullmatch(r"[A-Z0-9]{0,2}").all())
check("State Incorporated is a 2-char EDGAR code or blank",
      df["State Incorporated"].str.fullmatch(r"[A-Z0-9]{0,2}").all())

# --- 5. STATUS FLAGS: domains, 100% filled, registration, impossible-combo ----

for col in ["BDC", "ABS", "multi", "wksi", "shell", "src", "egc",
            "sec_12b", "sec_12g", "sec_15d"]:
    check("%s is 0/1" % col, df[col].isin(["0", "1"]).all(),
          "bad=%s" % df.loc[~df[col].isin(["0", "1"]), col].unique()[:5])
check("afs is NAF/AF/LAF (never blank)", df["afs"].isin(["NAF", "AF", "LAF"]).all(),
      "bad=%s" % df.loc[~df["afs"].isin(["NAF", "AF", "LAF"]), "afs"].unique()[:5])

status_cols = ["wksi", "shell", "afs", "src", "egc", "sec_12b", "sec_12g", "sec_15d"]
blank = {c: int((df[c] == "").sum()) for c in status_cols}
check("no blanks in any status column (100% filled)", sum(blank.values()) == 0, str(blank))

regsum = (df[["sec_12b", "sec_12g", "sec_15d"]] == "1").sum(axis=1)
check("exactly one of sec_12b/12g/15d is 1 on every row", (regsum == 1).all(),
      "rows not summing to 1: %d" % (regsum != 1).sum())

fills_path = os.path.join(directory, in_filename.replace(".csv", "_fills.csv"))
if os.path.exists(fills_path):
    fl = pd.read_csv(fills_path, dtype=str, keep_default_na=False)
    method_cols = ["wksi_method", "shell_method", "afs_method", "src_method",
                   "egc_method", "reg_method"]
    check("every CIK row has a fill-method record", set(fl["CIK"]) == csv_ciks,
          "fills=%d csv=%d" % (fl["CIK"].nunique(), len(csv_ciks)))
    check("every status cell has a non-blank resolution method",
          all((fl[c] != "").all() for c in method_cols))
    j = df.merge(fl[["CIK", "src_method"]], on="CIK")
    laf_src = j[(j["afs"] == "LAF") & (j["src"] == "1")]
    manufactured = laf_src[~laf_src["src_method"].isin(["AS_FILED", "AGENT_READ"])]
    check("no fill MANUFACTURED an impossible LAF+SRC pair", len(manufactured) == 0,
          "manufactured=%d (as-filed/read LAF+SRC kept: %d)" % (len(manufactured), len(laf_src)))
    print("    method mix afs:", dict(Counter(fl["afs_method"])))
    print("    method mix src:", dict(Counter(fl["src_method"])))

text_bad = sum(not (u.startswith("https://www.sec.gov/Archives/edgar/data/") and u.endswith(a + ".txt"))
               for u, a in zip(df["text_url"], df["Accession Number"]))
idx_bad = sum(not u.endswith(a + "-index.htm")
              for u, a in zip(df["filing_url"], df["Accession Number"]))
check("text_url is the full-submission .txt for the accession", text_bad == 0, "bad=%d" % text_bad)
check("filing_url is the filing index page for the accession", idx_bad == 0, "bad=%d" % idx_bad)

# --- coverage stats (informational) ------------------------------------------

print("\ncoverage:")
print("  rows / distinct CIKs:       %d / %d" % (len(df), df["CIK"].nunique()))
print("  rows from combined filings: %d (multi=1)" % (df["multi"] == "1").sum())
print("  blank State / State Inc.:   %d / %d" % ((df["State"] == "").sum(), (df["State Incorporated"] == "").sum()))
print("  flags (=1): " + " ".join("%s=%d" % (c, (df[c] == "1").sum())
      for c in ["BDC", "ABS", "multi", "wksi", "shell", "src", "egc", "sec_12b", "sec_12g", "sec_15d"]))
print("  afs:", dict(Counter(df["afs"])))

# --- verdict ------------------------------------------------------------------

failed = [n for n, ok, _ in results if not ok]
print("\n%d/%d checks passed." % (len(results) - len(failed), len(results)))
if failed:
    raise SystemExit("FAILED: " + "; ".join(failed))
print("ALL CHECKS PASSED.")
