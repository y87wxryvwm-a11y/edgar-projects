"""Build neutral evidence packets for every filing the public-float XBRL
ladder couldn't settle — the input to the independent-read tiers.

Two files per filing under DATA_DIR/evidence_float/:

* {accession}.cover.txt   — the cover text region only (what a blind reader
  sees; no hint of what our extractor found)
* {accession}.context.txt — our extraction rows + every XBRL fact, for the
  adjudication tier only

Also samples the negative classes (NO_FLOAT_STATED / ZERO_FACT / EMPTY
10-Ks) so the no-disclosure dispositions get independently spot-checked,
and writes float_evidence_index_{year}.csv driving the read fan-out.

Pure compute over local caches — no network.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
negative_sample_per_class = 25
# -----------------------------------------------------------------------------

import gzip
import os
import random

import pandas as pd

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT and DATA_DIR."
    )

import float_extractor as fx

directory = DATA_DIR.replace("\\", "/")
text_cache = os.path.join(directory, "cache", "text")
ev_dir = os.path.join(directory, "evidence_float")
os.makedirs(ev_dir, exist_ok=True)

st = pd.read_csv(os.path.join(directory, "float_status_%d.csv" % year),
                 dtype=str, keep_default_na=False)
ext = pd.read_csv(os.path.join(directory, "float_extraction_%d.csv" % year),
                  dtype=str, keep_default_na=False)
facts = pd.read_csv(os.path.join(directory, "float_facts_%d.csv" % year),
                    dtype=str, keep_default_na=False)
api = pd.read_csv(os.path.join(directory, "float_api_facts_%d.csv" % year),
                  dtype=str, keep_default_na=False)
pop = pd.read_csv(os.path.join(directory, "population_%d.csv" % year),
                  dtype=str, keep_default_na=False).set_index("accession")
eby = {a: g for a, g in ext.groupby("accession")}
fby = {a: g for a, g in facts.groupby("accession")}
aby = {a: g for a, g in api.groupby("accession")}

UNRESOLVED = ["MISMATCH", "MISSED_BY_PROSE", "ROWS_OK_FACTS_UNMATCHED",
              "PROSE_SUPERSET", "SCALE_DISCREPANCY", "PROSE_ONLY"]
NEGATIVE = ["NO_FLOAT_STATED", "ZERO_FACT", "EMPTY"]

rows = []
for status in UNRESOLVED:
    for acc in st[st["status"] == status]["accession"]:
        rows.append({"accession": acc, "status": status, "tier": "unresolved"})

random.seed(year)
for status in NEGATIVE:
    cand = st[(st["status"] == status) & (st["form"] == "10-K")][
        "accession"].tolist()
    for acc in random.sample(cand, min(negative_sample_per_class, len(cand))):
        rows.append({"accession": acc, "status": status, "tier": "negative"})

idx = pd.DataFrame(rows)
print("evidence packets: %d unresolved + %d negative samples"
      % ((idx["tier"] == "unresolved").sum(), (idx["tier"] == "negative").sum()))

for r in idx.itertuples(index=False):
    acc = r.accession
    with gzip.open(os.path.join(text_cache, acc + ".txt.gz"),
                   "rt", encoding="utf-8") as fh:
        text = fh.read()
    cover = fx._cover_region(text, [])[:9000]
    p = pop.loc[acc]
    with open(os.path.join(ev_dir, acc + ".cover.txt"), "w",
              encoding="utf-8") as fh:
        fh.write("FORM: %s   COMPANY: %s   FILED: %s   FY-END: %s\n"
                 "--- COVER TEXT ---\n%s\n"
                 % (p["form"], p["company_name"], p["date_filed"],
                    p["period_of_report"], cover))
    lines = ["STATUS: %s" % r.status, "", "EXTRACTION ROWS:"]
    g = eby.get(acc)
    if g is not None:
        for _, e in g.iterrows():
            lines.append("  value=%s raw=%r as_of=%s label=%r xbrl=%s flags=%s"
                         % (e["value"], e["raw"], e["as_of"], e["label"],
                            e["xbrl"], e["flags"]))
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("INLINE XBRL dei:EntityPublicFloat FACTS:")
    g = fby.get(acc)
    if g is not None:
        for _, f in g.iterrows():
            lines.append("  value=%s instant=%s dims=%s unit=%s"
                         % (f["value"], f["instant"], f["dims"], f["unit"]))
    else:
        lines.append("  (none)")
    lines.append("SEC companyconcept API FACTS:")
    g = aby.get(acc)
    if g is not None:
        for _, f in g.iterrows():
            lines.append("  value=%s end=%s" % (f["value"], f["end"]))
    else:
        lines.append("  (none)")
    with open(os.path.join(ev_dir, acc + ".context.txt"), "w",
              encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

out = os.path.join(directory, "float_evidence_index_%d.csv" % year)
idx.sort_values(["tier", "status", "accession"]).to_csv(
    out, index=False, encoding="utf-8", lineterminator="\n")
print("wrote %s (%d packets) and %s/" % (out, len(idx), ev_dir))
