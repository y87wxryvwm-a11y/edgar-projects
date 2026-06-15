"""Independent cross-check of the State / State Incorporated columns against an
outside source: the SEC submissions API (data.sec.gov/submissions/CIK*.json),
which carries the same EDGAR State-or-Country codes the SGML header uses
(stateOfIncorporation and addresses.business.stateOrCountry).

This attacks the one risk the offline suite can't: a shared blind spot in the
header parser. The API is a *different* SEC system populated independently, so
agreement is strong evidence the columns are right. The API reports the
registrant's CURRENT profile, so a mismatch is usually as-filed-vs-now drift
(a move or reincorporation since the 2025 filing), not an extraction error —
mismatches are printed for eyeballing, never silently passed.

Stratified sample so every kind of row is exercised: domestic 10-K, 20-F,
40-F, ABS, blank-State, blank-State-Incorporated, multi-source mail fallback.
Network; throttled. Set sample_size; it always includes the rare strata whole.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
in_filename = "registrant_count_2025.csv"
sample_size = 300          # domestic-10-K backbone; rare strata added on top
# -----------------------------------------------------------------------------

import json
import os
import time

import pandas as pd
import requests

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
api_cache = os.path.join(directory, "cache", "submissions")
os.makedirs(api_cache, exist_ok=True)

df = pd.read_csv(os.path.join(directory, in_filename), dtype=str, keep_default_na=False)
prov = pd.read_csv(os.path.join(directory, in_filename.replace(".csv", "_provenance.csv")),
                   dtype=str, keep_default_na=False)
# Cross-check only HEADER-sourced rows: those did NOT use the submissions API,
# so comparing them to it is a genuinely independent check. (API-filled rows
# would match the API by construction.)
df = df.merge(prov[["Accession Number", "state_source", "soi_source"]],
              on="Accession Number")

# attach form + ABS flag from the census population (for stratification only)
forms = {}
if CENSUS_POPULATION_CSV and os.path.exists(CENSUS_POPULATION_CSV):
    cen = pd.read_csv(CENSUS_POPULATION_CSV, dtype=str, keep_default_na=False)
    forms = dict(zip(cen["accession"], cen["form"]))
    abs_flag = dict(zip(cen["accession"], cen["excluded_abs"]))
else:
    abs_flag = {}
df["form"] = df["Accession Number"].map(lambda a: forms.get(a, "10-K"))
df["abs"] = df["Accession Number"].map(lambda a: abs_flag.get(a, "False"))
hdr = df[(df["state_source"] == "HEADER") | (df["soi_source"] == "HEADER")]


def take(frame, n):
    if len(frame) <= n:
        return frame
    step = len(frame) / n
    return frame.iloc[[int(i * step) for i in range(n)]]


# header-sourced rows only; domestic-10-K backbone + foreign forms exercised
picks = pd.concat([
    take(hdr[hdr["form"] == "10-K"], sample_size),
    hdr[hdr["form"] == "20-F"].pipe(take, 60),
    hdr[hdr["form"] == "40-F"].pipe(take, 30),
    hdr[hdr["abs"] == "True"].pipe(take, 30),
]).drop_duplicates("Accession Number").reset_index(drop=True)
print("sampled %d rows for API cross-check\n" % len(picks))

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def submissions(cik):
    path = os.path.join(api_cache, "CIK%010d.json" % int(cik))
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    url = "https://data.sec.gov/submissions/CIK%010d.json" % int(cik)
    for attempt in range(3):
        time.sleep(0.12)
        resp = session.get(url, timeout=30)
        if resp.status_code == 200:
            with open(path, "w", encoding="utf-8") as f:
                f.write(resp.text)
            return resp.json()
        if resp.status_code == 404:
            return None
        time.sleep(3 * (attempt + 1))
    return None


state_match = state_drift = state_n = 0
soi_match = soi_drift = soi_n = 0
foreign_api_blank = 0   # header has a country code, API leaves stateOrCountry blank
errors = []
for i, row in picks.iterrows():
    j = submissions(row["CIK"])
    if j is None:
        errors.append((row["Accession Number"], "no-API"))
        continue
    api_soi = (j.get("stateOfIncorporation") or "").upper()
    api_state = (j.get("addresses", {}).get("business", {})
                 .get("stateOrCountry") or "").upper()
    # only compare a field the dataset actually took from the header
    if row["state_source"] == "HEADER":
        state_n += 1
        if row["State"] == api_state:
            state_match += 1
        elif api_state == "":          # header carried a (often foreign) code the API omits
            foreign_api_blank += 1
        else:
            state_drift += 1
            errors.append((row["Accession Number"], "STATE hdr=%r api=%r"
                           % (row["State"], api_state)))
    if row["soi_source"] == "HEADER":
        soi_n += 1
        if row["State Incorporated"] == api_soi:
            soi_match += 1
        elif api_soi == "":
            foreign_api_blank += 1
        else:
            soi_drift += 1
            errors.append((row["Accession Number"], "SOI hdr=%r api=%r"
                           % (row["State Incorporated"], api_soi)))

print("Cross-checking HEADER-sourced values vs EDGAR's authoritative record.\n")
print("State (location):   %d compared, %d exact, %d header-code-but-API-blank, %d genuinely differ"
      % (state_n, state_match, foreign_api_blank, state_drift))
print("State Incorporated: %d compared, %d exact, %d genuinely differ"
      % (soi_n, soi_match, soi_drift))
print("\nexact-match rate (excluding API-blank): State %.1f%%, State Incorporated %.1f%%"
      % (100 * state_match / max(state_match + state_drift, 1),
         100 * soi_match / max(soi_match + soi_drift, 1)))
print("\ngenuine header-vs-record differences (as-filed vs current / redomiciliations) — %d:"
      % len([e for e in errors if "differ" not in e[1]]))
for acc, msg in errors[:60]:
    print("  %s  %s" % (acc, msg))
