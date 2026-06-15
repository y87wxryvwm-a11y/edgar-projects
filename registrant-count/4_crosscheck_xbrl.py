"""Independent as-filed cross-check of State / State Incorporated against the
filing's OWN inline-XBRL dei tags (dei:EntityAddressStateOrProvince and
dei:EntityIncorporationStateCountryCode), parsed from the cached primary
documents. This is the XBRL verification source: it is read straight from the
filing, so unlike the submissions API it carries no current-vs-as-filed drift.

Caveat documented up front: the XBRL incorporation tag is NOISY. Filers tag it
inconsistently — a display name ("Delaware") or a raw code, EDGAR codes or ISO
codes (IL = Illinois in EDGAR but Israel in ISO), country level ("Canada")
where the header gives the province (Ontario). Several filings even tag
"Delaware" while the registrant is demonstrably CA-incorporated per both the
SGML header and EDGAR's own record. So this script REPORTS agreement and
itemizes conflicts for a human; it is not a pass/fail gate. The published
values come from the header (+ EDGAR's authoritative record), which is why they
are trusted over this tag.

The XBRL display name is decoded to an EDGAR code by a map derived empirically
from filings where the header code and the XBRL name co-occur (no hand-typed
country table). Internal whitespace in names is collapsed first.

Network only on a cold doc cache (reuses the census docs cache when present).
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
in_filename = "registrant_count_2025.csv"
sample_size = 1500   # 0 = every in-scope filing (slow: parses each doc)
# -----------------------------------------------------------------------------

import os
import re
from collections import Counter, defaultdict

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

df = pd.read_csv(os.path.join(directory, in_filename), dtype=str, keep_default_na=False)
prov = pd.read_csv(os.path.join(directory, in_filename.replace(".csv", "_provenance.csv")),
                   dtype=str, keep_default_na=False)
m = df.merge(prov[["Accession Number", "state_source", "soi_source",
                   "header_state", "header_soi"]], on="Accession Number")

forms, txtpaths = {}, {}
if CENSUS_POPULATION_CSV and os.path.exists(CENSUS_POPULATION_CSV):
    cen = pd.read_csv(CENSUS_POPULATION_CSV, dtype=str, keep_default_na=False)
    forms = dict(zip(cen["accession"], cen["form"]))
m["form"] = m["Accession Number"].map(lambda a: forms.get(a, "10-K"))


def even(frame, n):
    if n == 0 or len(frame) <= n:
        return frame
    step = len(frame) / n
    return frame.iloc[[int(i * step) for i in range(n)]]


sample = m if sample_size == 0 else pd.concat([
    even(m[m.form == "10-K"], int(sample_size * 0.8)),
    even(m[m.form == "20-F"], int(sample_size * 0.15)),
    even(m[m.form == "40-F"], int(sample_size * 0.05)),
    m[m.soi_source == "API"],   # always include every API-filled SOI row
]).drop_duplicates("Accession Number")
print("parsing XBRL for %d filings...\n" % len(sample), flush=True)

session = lib.make_session(USER_AGENT)
WS = re.compile(r"\s+")
rows = []
for i, (_, r) in enumerate(sample.iterrows(), 1):
    acc = r["Accession Number"]
    doc = lib.fetch_primary_document(session, cache_dirs, None, acc, r["form"])
    xs = xi = ""
    if doc and (b"EntityAddressStateOrProvince" in doc
                or b"EntityIncorporationStateCountryCode" in doc):
        xs = WS.sub(" ", lib.extract_dei_state(doc, lib.ADDR_STATE_LOCALNAME)).strip()
        xi = WS.sub(" ", lib.extract_dei_state(doc, lib.INCORP_LOCALNAME)).strip()
    rows.append((acc, r["State"], r["State Incorporated"],
                 r["header_state"], r["header_soi"], r["soi_source"], xs, xi))
    if i % 300 == 0 or i == len(sample):
        print("  [%d/%d]" % (i, len(sample)), flush=True)

# empirical name -> code from header/XBRL co-occurrence within the sample
pairs = defaultdict(Counter)
for _, pub_st, pub_soi, h_st, h_soi, _, xs, xi in rows:
    if h_st and xs:
        pairs[xs.upper()][h_st] += 1
    if h_soi and xi:
        pairs[xi.upper()][h_soi] += 1
name2code = {k: c.most_common(1)[0][0] for k, c in pairs.items()}


def decode(name):
    return name2code.get(WS.sub(" ", name).strip().upper(), "") if name else ""


# agreement where the HEADER had a value (the verification signal)
soi_ok = soi_diff = state_ok = state_diff = 0
soi_conflicts = Counter()
api_soi_ok = api_soi_unverifiable = 0
for acc, pub_st, pub_soi, h_st, h_soi, soi_src, xs, xi in rows:
    if h_soi and xi:
        if decode(xi) == h_soi:
            soi_ok += 1
        else:
            soi_diff += 1
            soi_conflicts[(h_soi, xi, decode(xi))] += 1
    if h_st and xs:
        state_ok += 1 if decode(xs) == h_st else 0
        state_diff += 0 if decode(xs) == h_st else 1
    # XBRL view of the API-filled SOI rows
    if soi_src == "API" and pub_soi:
        if xi and decode(xi) == pub_soi:
            api_soi_ok += 1
        else:
            api_soi_unverifiable += 1

print("\n=== header vs XBRL (where header has a value) ===")
print("State Incorporated: %d agree, %d differ (%.1f%% agree)"
      % (soi_ok, soi_diff, 100 * soi_ok / max(soi_ok + soi_diff, 1)))
print("State (location):   %d agree, %d differ (%.1f%% agree)"
      % (state_ok, state_diff, 100 * state_ok / max(state_ok + state_diff, 1)))
print("\ntop SOI conflict patterns (header_code, xbrl_name, decoded) — XBRL is the noisy side:")
for (h, xn, dec), n in soi_conflicts.most_common(15):
    print("  %3d  header=%-4s xbrl=%-22r decoded=%s" % (n, h, xn, dec))
print("\n=== XBRL view of API-filled State Incorporated ===")
print("of %d API-filled SOI rows in sample: %d corroborated by the filing's XBRL, "
      "%d the XBRL doesn't cleanly confirm (absent/noisy)"
      % (api_soi_ok + api_soi_unverifiable, api_soi_ok, api_soi_unverifiable))
