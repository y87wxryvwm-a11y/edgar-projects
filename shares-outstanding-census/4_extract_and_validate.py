"""Run the cover extractor over every in-scope filing's cached text and
validate each extracted row against the filer's own inline-XBRL facts.

Outputs (both in DATA_DIR):

* extraction_{year}.csv — one row per share class extracted from a cover,
  with its per-row XBRL verdict.
* filing_status_{year}.csv — one row per in-scope filing with the filing-
  level validation status that drives the improvement loop:
    VALIDATED       every prose row's number matches a tagged fact
    AGG_VALIDATED   prose class rows sum to the single tagged total
    MISMATCH        facts exist, at least one prose number matches nothing
    PROSE_ONLY      prose rows but the filer tagged no dei shares fact
    MISSED_BY_PROSE the filer tagged a fact but prose extraction found nothing
    EMPTY           no prose rows and no facts (candidate no-public-shares)

Pure compute over local caches — fast to re-run after every extractor change.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
# -----------------------------------------------------------------------------

import gzip
import os
import re

import pandas as pd

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT and DATA_DIR."
    )

import cover_extractor as cx

directory = DATA_DIR.replace("\\", "/")
text_cache = os.path.join(directory, "cache", "text")
pop_path = os.path.join(directory, "population_%d.csv" % year)
facts_path = os.path.join(directory, "ixbrl_facts_%d.csv" % year)
api_facts_path = os.path.join(directory, "xbrl_api_facts_%d.csv" % year)
extraction_path = os.path.join(directory, "extraction_%d.csv" % year)
status_path = os.path.join(directory, "filing_status_%d.csv" % year)

pop = pd.read_csv(pop_path, dtype=str, keep_default_na=False)
in_scope = pop[pop["excluded_abs"] == "False"].reset_index(drop=True)
facts = pd.read_csv(facts_path, dtype=str, keep_default_na=False)
facts["value"] = facts["value"].astype("int64")
facts_by_acc = {a: g for a, g in facts.groupby("accession")}

_CLASS_MEMBER_RE = re.compile(
    r"(?:Common)?Class([A-Z])\w*Member$|Class([A-Z])\b")
_PRETTY_MEMBER_RE = re.compile(r"(?<=[a-z])(?=[A-Z])")


def _pretty(member):
    return _PRETTY_MEMBER_RE.sub(" ", re.sub(r"Member$", "", member)).strip()


def fact_dims_maps(f):
    """Per-value class designator, class member name, and registrant from
    the filer's own dimension members — the authoritative attribution for
    ambiguous prose."""
    cls, cls_name, reg = {}, {}, {}
    for _, fr in f.iterrows():
        v = int(fr["value"])
        for dim in (fr["dims"] or "").split("|"):
            if dim.startswith("StatementClassOfStockAxis=") or \
                    dim.startswith("ClassesOfShareCapitalAxis="):
                member = dim.split("=", 1)[1].split(":")[-1]
                m = _CLASS_MEMBER_RE.search(member)
                if m:
                    cls.setdefault(v, (m.group(1) or m.group(2)))
                cls_name.setdefault(v, _pretty(member))
            elif dim.startswith("LegalEntityAxis="):
                member = dim.split("=", 1)[1].split(":")[-1]
                reg.setdefault(v, _pretty(member))
    return cls, cls_name, reg

# secondary XBRL source: SEC's companyconcept API (joined by accession);
# used only where the filing's own inline XBRL yielded no facts
api_by_acc = {}
if os.path.exists(api_facts_path):
    api = pd.read_csv(api_facts_path, dtype=str, keep_default_na=False)
    api["value"] = api["value"].astype("int64")
    api = api.rename(columns={"end": "instant"})
    api_by_acc = {a: g for a, g in api.groupby("accession")}
n = len(in_scope)
print("in-scope filings: %d, ixbrl facts: %d, api-covered filings: %d"
      % (n, len(facts), len(api_by_acc)), flush=True)

ext_rows, status_rows = [], []
for i, row in enumerate(in_scope.itertuples(index=False), 1):
    text_path = os.path.join(text_cache, row.accession + ".txt.gz")
    if not os.path.exists(text_path):
        status_rows.append({"accession": row.accession, "form": row.form,
                            "status": "NO_TEXT", "n_rows": 0, "n_facts": 0,
                            "filing_flags": "NO_TEXT"})
        continue
    with gzip.open(text_path, "rt", encoding="utf-8") as fh:
        text = fh.read()

    multi = int(row.n_filers) > 1
    rows, filing_flags = cx.extract_cover(
        text, row.form, row.period_of_report, multi_registrant=multi,
        filed_date=row.date_filed)
    if multi:
        filing_flags.append("MULTI_REGISTRANT")

    f = facts_by_acc.get(row.accession)
    xbrl_source = "inline"
    if f is None or len(f) == 0:
        f = api_by_acc.get(row.accession)
        xbrl_source = "api" if f is not None else ""
    all_fact_values = sorted(set(f["value"].tolist())) if f is not None else []
    # a tagged 0 is a no-public-shares signal, not a validatable count
    fact_values = [v for v in all_fact_values if v != 0]
    # latest instant per value; instants compared on the date part only
    fact_dates = ({v: inst[:10] for v, inst in
                   f.groupby("value")["instant"].max().items()}
                  if f is not None else {})

    n_match = 0
    for r in rows:
        v = r["value"]
        # a fractional prose count matches the filer's rounded integer tag —
        # the integer fact is thereby accounted for, the exact value stands
        if v not in fact_values and float(v) != int(v) and \
                (int(round(v)) in fact_values or int(v) in fact_values):
            r["flags"].append("XBRL_ROUNDED_MATCH")
            rounded = int(round(v)) if int(round(v)) in fact_values else int(v)
            fact_values = sorted((set(fact_values) - {rounded}) | {v})
        if r["value"] in fact_values:
            r["xbrl"] = "XBRL_MATCH"
            n_match += 1
            inst = fact_dates.get(r["value"], "")
            if r["as_of"] and inst and r["as_of"] != inst:
                # the tag's instant IS the filer's own as-of for this exact
                # count; a differing prose-derived date is a wrong grab
                # (typically the Q2 market-value date)
                if inst <= (pd.to_datetime(row.date_filed) + pd.Timedelta(days=14)).strftime("%Y-%m-%d"):
                    r["as_of"] = inst
                    r["flags"].append("DATE_FROM_XBRL_TAG")
                else:
                    r["flags"].append("DATE_DIFF_VS_XBRL")
            elif not r["as_of"] and inst:
                r["flags"].append("XBRL_HAS_DATE")
        elif fact_values:
            r["xbrl"] = "XBRL_MISMATCH"
            if r["value"] * 1000 in fact_values or \
                    (r["value"] % 1000 == 0 and r["value"] // 1000 in fact_values):
                r["flags"].append("THOUSANDS_DISCREPANCY")
        else:
            r["xbrl"] = "XBRL_ABSENT"
    matched_values = {r["value"] for r in rows if r["xbrl"] == "XBRL_MATCH"}

    # attribution repairs from the filer's own dimension members + tagged
    # instants: ambiguous duplicate labels, missing registrants, blank dates
    if f is not None and len(f) and "dims" in f.columns:
        cls_map, cls_name_map, reg_map = fact_dims_maps(f)
        seen_labels = {}
        for r in rows:
            seen_labels.setdefault(
                (r["label"].lower(), r["class_designator"]), []).append(r)
        ambiguous = {k for k, v in seen_labels.items()
                     if len({x["value"] for x in v}) > 1}
        for r in rows:
            v = int(r["value"]) if float(r["value"]) == int(r["value"]) else None
            key = (r["label"].lower(), r["class_designator"])
            mem_desig = cls_map.get(v, "")
            if mem_desig and (key in ambiguous or
                              (r["class_designator"] and
                               r["class_designator"] != mem_desig)):
                if r["class_designator"] != mem_desig:
                    r["label"] = re.sub(r"(?i)\bclass\s+[a-z0-9]{1,2}\b",
                                        "Class " + mem_desig, r["label"]) \
                        if re.search(r"(?i)\bclass\s+[a-z0-9]{1,2}\b",
                                     r["label"]) else r["label"]
                    r["class_designator"] = mem_desig
                    r["flags"].append("CLASS_FROM_XBRL")
            # duplicate labels with distinct tagged member names: append the
            # filer's own member name so the classes stay distinguishable
            if key in ambiguous and cls_name_map.get(v) and \
                    not cls_map.get(v) and \
                    cls_name_map[v].lower() not in r["label"].lower():
                grp_vals = [int(x["value"]) for x in seen_labels[key]
                            if float(x["value"]) == int(x["value"])]
                names = {cls_name_map.get(gv) for gv in grp_vals}
                if len(names) == len(grp_vals):
                    r["label"] = "%s (%s)" % (r["label"], cls_name_map[v])
                    r["flags"].append("CLASS_FROM_XBRL")
            if int(row.n_filers) > 1 and not r.get("registrant") and \
                    reg_map.get(v):
                r["registrant"] = reg_map[v]
                r["flags"].append("REGISTRANT_FROM_XBRL")
            if not r["as_of"] and r["xbrl"] == "XBRL_MATCH":
                inst = fact_dates.get(r["value"], "")
                if inst:
                    r["as_of"] = inst
                    r["flags"].append("DATE_FROM_XBRL_TAG")

    if rows and fact_values and n_match == len(rows) and \
            all(v in matched_values for v in fact_values):
        status = "VALIDATED"
    elif rows and len(fact_values) == 1 and len(rows) > 1 and \
            sum(r["value"] for r in rows if r["share_type"] not in
                ("preferred", "depositary")) == fact_values[0]:
        status = "AGG_VALIDATED"
        for r in rows:
            if r["xbrl"] == "XBRL_MISMATCH":
                r["xbrl"] = "XBRL_AGG_MATCH"
    elif rows and fact_values and all(v in matched_values for v in fact_values):
        # every tagged fact is confirmed; the extra prose rows are classes
        # the filer didn't tag (e.g. exchangeable shares) — tier-2 verifies
        status = "PROSE_SUPERSET"
        for r in rows:
            if r["xbrl"] == "XBRL_MISMATCH":
                r["xbrl"] = "XBRL_NOT_COVERED"
    elif rows and fact_values and n_match == len(rows) and \
            all(v == sum(matched_values) or
                v == sum(r["value"] for r in rows if r["share_type"] not in
                         ("preferred", "depositary"))
                for v in fact_values if v not in matched_values):
        # the only unmatched facts are the filer's tagged TOTAL of the very
        # rows we extracted (we drop cross-class total rows by design)
        status = "VALIDATED"
        filing_flags.append("FACTS_INCLUDE_TOTAL")
    elif rows and fact_values and n_match == len(rows):
        # every prose row is confirmed but the filer tagged MORE facts —
        # exactly how a missed share class hides; never auto-validated
        status = "ROWS_OK_FACTS_UNMATCHED"
    elif rows and fact_values:
        status = "MISMATCH"
    elif rows:
        status = "PROSE_ONLY"
    elif fact_values:
        status = "MISSED_BY_PROSE"
    elif all_fact_values:
        status = "ZERO_FACT"
    else:
        status = "EMPTY"

    for r in rows:
        v = r["value"]
        ext_rows.append({
            "accession": row.accession, "cik": row.cik,
            "company_name": row.company_name, "form": row.form,
            # fractional counts (rare, real) print exactly; ints stay ints
            "value": ("%d" % v) if float(v) == int(v) else ("%s" % v),
            "share_class_label": r["label"],
            "share_type": r["share_type"],
            "class_designator": r["class_designator"],
            "registrant": r["registrant"],
            "as_of": r["as_of"], "method": r["method"], "xbrl": r["xbrl"],
            "flags": ";".join(sorted(set(r["flags"]))),
            "filing_index_url": row.filing_index_url,
        })
    status_rows.append({
        "accession": row.accession, "form": row.form, "status": status,
        "n_rows": len(rows), "n_facts": len(fact_values),
        "xbrl_source": xbrl_source,
        "filing_flags": ";".join(sorted(set(filing_flags)))})

    if i % 500 == 0 or i == n:
        print("[%d/%d] extracted" % (i, n), flush=True)

ext = pd.DataFrame(ext_rows).sort_values(
    ["accession", "value"]).reset_index(drop=True)
ext.to_csv(extraction_path, index=False, encoding="utf-8", lineterminator="\n")
st = pd.DataFrame(status_rows).sort_values("accession").reset_index(drop=True)
st.to_csv(status_path, index=False, encoding="utf-8", lineterminator="\n")

print("\nwrote %s (%d class rows)" % (extraction_path, len(ext)))
print("wrote %s (%d filings)" % (status_path, len(st)))
print("\nfiling status x form:")
print(st.groupby(["status", "form"]).size().unstack(fill_value=0).to_string())
ok = st["status"].isin(["VALIDATED", "AGG_VALIDATED"]).sum()
print("\nvalidated: %d / %d (%.1f%%)" % (ok, len(st), 100.0 * ok / len(st)))
