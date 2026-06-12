"""Run the public-float extractor over every in-scope filing's cached text
and validate each extraction against the filer's own dei:EntityPublicFloat
facts (inline XBRL from script 8, companyconcept API from script 10 where
the document carries none).

Matching is precision-aware: an exact printed figure must equal the tagged
value; a scaled figure ("$5.6 billion") matches any tagged value within the
half-step of its printed precision (flagged ROUNDING_MATCH when not exact).

Outputs (both in DATA_DIR):

* float_extraction_{year}.csv — one row per extracted float value, with its
  per-row XBRL verdict.
* float_status_{year}.csv — one row per in-scope filing with the filing-
  level status driving the improvement loop:
    VALIDATED            every prose value matches a tagged fact, all
                         nonzero facts accounted for
    AGG_VALIDATED        prose component values sum to the single tagged fact
    ROWS_OK_FACTS_UNMATCHED  every prose value confirmed, extra facts remain
    MISMATCH             facts exist, at least one prose value matches nothing
    PROSE_ONLY           prose value but no usable fact
    MISSED_BY_PROSE      nonzero fact but prose extraction found nothing
    STATEMENT_VS_FACTS   cover says no float / N-A, filer tagged a nonzero one
    NO_FLOAT_STATED      cover says no float / N-A; tags agree or are absent
    ZERO_FACT            tagged 0, cover silent (no-float signal)
    NIL_FACT             explicit nil tag, cover silent
    EMPTY                no anchor, no statement, no facts — the expected
                         outcome for 20-F / 40-F covers

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

import float_extractor as fx

directory = DATA_DIR.replace("\\", "/")
text_cache = os.path.join(directory, "cache", "text")
pop_path = os.path.join(directory, "population_%d.csv" % year)
facts_path = os.path.join(directory, "float_facts_%d.csv" % year)
api_facts_path = os.path.join(directory, "float_api_facts_%d.csv" % year)
extraction_path = os.path.join(directory, "float_extraction_%d.csv" % year)
status_path = os.path.join(directory, "float_status_%d.csv" % year)

pop = pd.read_csv(pop_path, dtype=str, keep_default_na=False)
in_scope = pop[pop["excluded_abs"] == "False"].reset_index(drop=True)

facts = pd.read_csv(facts_path, dtype=str, keep_default_na=False)
facts_by_acc = {a: g for a, g in facts.groupby("accession")}

api_by_acc = {}
if os.path.exists(api_facts_path):
    api = pd.read_csv(api_facts_path, dtype=str, keep_default_na=False)
    api = api.rename(columns={"end": "instant"})
    api["dims"] = ""
    api["unit"] = "USD"
    api_by_acc = {a: g for a, g in api.groupby("accession")}

n = len(in_scope)
print("in-scope filings: %d, inline float facts: %d, api-covered: %d"
      % (n, len(facts), len(api_by_acc)), flush=True)


def fmt(v):
    return ("%d" % v) if float(v) == int(v) else ("%.2f" % v)


ext_rows, status_rows = [], []
for i, row in enumerate(in_scope.itertuples(index=False), 1):
    text_path = os.path.join(text_cache, row.accession + ".txt.gz")
    if not os.path.exists(text_path):
        status_rows.append({"accession": row.accession, "form": row.form,
                            "status": "NO_TEXT", "n_rows": 0, "n_facts": 0,
                            "n_zero_facts": 0, "n_nil_facts": 0,
                            "xbrl_source": "", "filing_flags": "NO_TEXT"})
        continue
    with gzip.open(text_path, "rt", encoding="utf-8") as fh:
        text = fh.read()

    multi = int(row.n_filers) > 1
    rows, filing_flags = fx.extract_float(
        text, row.form, row.period_of_report, multi_registrant=multi,
        filed_date=row.date_filed)
    if multi:
        filing_flags.append("MULTI_REGISTRANT")

    f = facts_by_acc.get(row.accession)
    xbrl_source = "inline"
    if f is None or len(f) == 0:
        f = api_by_acc.get(row.accession)
        xbrl_source = "api" if f is not None else ""

    por = str(row.period_of_report)
    por_iso = "%s-%s-%s" % (por[:4], por[4:6], por[6:8]) \
        if len(por) == 8 and por.isdigit() else por
    nonzero, zeros, nils = [], [], []
    fact_meta = {}  # value -> (instant, dims)
    if f is not None:
        for _, fr in f.iterrows():
            if fr["value"] == "":
                nils.append(fr)
                continue
            v = float(fr["value"])
            if fr.get("unit", "USD") not in ("USD", ""):
                filing_flags.append("NON_USD_FACT")
            # a tag whose instant predates the fiscal year by over ~18
            # months is a stale leftover from an earlier filing, not this
            # cover's disclosure
            inst = fr["instant"][:10]
            if v != 0 and inst and len(por_iso) == 10 and \
                    (pd.Timestamp(por_iso) - pd.Timestamp(inst)).days > 540:
                filing_flags.append("STALE_FACT_IGNORED")
                continue
            if v == 0:
                zeros.append(fr)
            else:
                nonzero.append(v)
                prev = fact_meta.get(v)
                # keep the latest instant per value
                if prev is None or inst > prev[0]:
                    fact_meta[v] = (inst, fr["dims"])
    nonzero = sorted(set(nonzero))

    def closest_match(v, tol):
        # exact figures still match within $1 — taggers truncate printed
        # cents ("$1,234,533,446.84" tagged 1234533446)
        eps = tol if tol > 0 else 1.0
        best = None
        for fv in nonzero:
            d = abs(fv - v)
            if d <= eps and (best is None or d < abs(best - v)):
                best = fv
        return best

    def scale_match(v, tol):
        """The tagging defect family: identical mantissa, the tag's scale
        or decimal point off by a power of ten ('$1,578,803,860' tagged
        1578803860000; '$24,613,330.5' tagged 246133305)."""
        eps = tol if tol > 0 else 0.005
        for k in (10.0, 100.0, 1e3, 1e6, 1e9):
            for fv in nonzero:
                if abs(fv - v * k) <= eps * k:
                    return fv
                if abs(fv * k - v) <= eps:
                    return fv
        return None

    filed_plus = (pd.to_datetime(row.date_filed) +
                  pd.Timedelta(days=14)).strftime("%Y-%m-%d")
    matched_facts = set()
    n_match, n_scale = 0, 0
    for r in rows:
        r["value_xbrl"] = ""
        if r["value"] == 0:
            if zeros:
                r["xbrl"] = "XBRL_MATCH"
                r["value_xbrl"] = 0.0
                n_match += 1
            elif nonzero:
                r["xbrl"] = "XBRL_MISMATCH"
            else:
                r["xbrl"] = "XBRL_ABSENT"
            continue
        fv = closest_match(r["value"], r["tol"])
        # boilerplate-slip detection: a full-precision mantissa followed by
        # a spurious scale word ("$8,474,510 million") whose BARE digits the
        # filer's own tag confirms — the digits are the value, the scale
        # word is the slip (observed on several covers; one is internally
        # provable: the cover's own shares x price equals the bare digits)
        if fv is None and r["tol"] > 0:
            mm = re.search(r"(\d{1,3}(?:,\d{3})+(?:\.\d+)?)", r["raw"])
            if mm:
                mant = float(mm.group(1).replace(",", ""))
                if mant >= 1e6 and any(abs(x - mant) <= 1.0
                                       for x in nonzero):
                    r["value"] = mant
                    r["tol"] = 0.0
                    r["flags"].append("SCALE_WORD_CONTRADICTED_BY_TAG")
                    fv = closest_match(mant, 0.0)
        sv = None if fv is not None else scale_match(r["value"], r["tol"])
        if fv is not None or sv is not None:
            if fv is not None:
                r["xbrl"] = "XBRL_MATCH"
                if abs(fv - r["value"]) > 0.005:
                    r["flags"].append("ROUNDING_MATCH")
            else:
                # mantissa agreement, scale disagreement: the printed cover
                # number is the disclosure of record; the tag is recorded as
                # filed and the filing goes to the reads tier, not auto-OK
                fv = sv
                r["xbrl"] = "XBRL_SCALE_DISCREPANCY"
                n_scale += 1
            n_match += 1
            matched_facts.add(fv)
            r["value_xbrl"] = fv
            inst, dims = fact_meta.get(fv, ("", ""))
            if r["as_of"] and inst and r["as_of"] != inst:
                # the tag's instant IS the filer's own as-of for this exact
                # value; a differing prose-derived date is a wrong grab
                if inst <= filed_plus:
                    r["as_of"] = inst
                    r["flags"].append("DATE_FROM_XBRL_TAG")
                else:
                    r["flags"].append("DATE_DIFF_VS_XBRL")
            elif not r["as_of"] and inst:
                r["as_of"] = inst
                r["flags"] = [fl for fl in r["flags"]
                              if fl != "NO_DATE_STATED"]
                r["flags"].append("DATE_FROM_XBRL_TAG")
            def member_words(d):
                # CamelCase member -> words, handling upper runs:
                # "CubesmartLPAndSubsidiaries" -> "Cubesmart LP And
                # Subsidiaries", "CCOHoldings" -> "CCO Holdings"
                member = re.sub(r"Member$", "", d.split("=", 1)[1].split(":")[-1])
                member = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", member)
                return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", member)

            for d in (dims or "").split("|"):
                if d.startswith("LegalEntityAxis=") and not r["label"]:
                    r["label"] = member_words(d)
                    r["flags"].append("REGISTRANT_FROM_XBRL")
                elif d.startswith("StatementClassOfStockAxis=") and \
                        not r["label"]:
                    # the filer's own class member names the row — the
                    # authoritative attribution for unlabeled prose
                    r["label"] = member_words(d)
                    r["flags"].append("CLASS_FROM_XBRL")
        elif nonzero:
            r["xbrl"] = "XBRL_MISMATCH"
        else:
            r["xbrl"] = "XBRL_ABSENT"

    # the same disclosure read twice (two anchors whose dates converged
    # once the tag instant was adopted) is one row
    deduped, seen_keys = [], set()
    for r in rows:
        key = (r["value"], r["as_of"], r["label"])
        if key in seen_keys:
            n_match -= 1 if r["xbrl"] in ("XBRL_MATCH",
                                          "XBRL_SCALE_DISCREPANCY") else 0
            continue
        seen_keys.add(key)
        deduped.append(r)
    rows = deduped

    statement = next((fl for fl in filing_flags
                      if fl.startswith("NO_FLOAT_STATEMENT:")), "")

    # facts dimensioned as subsequent events or ranges are voluntary extras
    # (post-period updates, max/min bounds); a fact equal to the sum of the
    # matched rows is the filer's tagged TOTAL of the very components we
    # extracted — none of these condemn completeness
    condemning = [v for v in nonzero
                  if "SubsequentEvent" not in fact_meta.get(v, ("", ""))[1]
                  and "RangeAxis" not in fact_meta.get(v, ("", ""))[1]]

    if rows and nonzero:
        unmatched_facts = [v for v in condemning if v not in matched_facts]
        msum = sum(r["value"] for r in rows if r["xbrl"] == "XBRL_MATCH")
        msum_tol = max(max((r["tol"] for r in rows), default=0.0), 1.0) * \
            max(len(rows), 1)
        if unmatched_facts and msum > 0:
            still = [v for v in unmatched_facts if abs(v - msum) > msum_tol]
            if len(still) < len(unmatched_facts):
                filing_flags.append("FACTS_INCLUDE_TOTAL")
            unmatched_facts = still
        row_sum = sum(r["value"] for r in rows)
        sum_tol = max((r["tol"] for r in rows), default=0) or 0.005
        if n_match == len(rows) and not unmatched_facts:
            status = "SCALE_DISCREPANCY" if n_scale else "VALIDATED"
        elif len(nonzero) == 1 and len(rows) > 1 and \
                not matched_facts and \
                all(r["value"] >= 1000 for r in rows) and \
                abs(row_sum - nonzero[0]) <= sum_tol * len(rows):
            status = "AGG_VALIDATED"
            for r in rows:
                if r["xbrl"] == "XBRL_MISMATCH":
                    r["xbrl"] = "XBRL_AGG_MATCH"
        elif not unmatched_facts and n_match > 0 and \
                any("TOTAL_OF_COMPONENTS" in r["flags"] and
                    r["xbrl"] == "XBRL_MATCH" for r in rows) and \
                all(r["xbrl"] != "XBRL_MISMATCH" or
                    "COMPONENT" in r["flags"] for r in rows):
            # the filer tagged only the total; the prose components sum to
            # it exactly, so the sum corroborates every component
            status = "VALIDATED"
            filing_flags.append("COMPONENTS_UNTAGGED")
            for r in rows:
                if r["xbrl"] == "XBRL_MISMATCH":
                    r["xbrl"] = "COMPONENT_OF_MATCHED_TOTAL"
        elif not unmatched_facts and n_match > 0:
            # every tagged fact is confirmed; the extra prose rows are
            # values the filer didn't tag (a second registrant's float) —
            # tier-2 verifies them
            status = "PROSE_SUPERSET"
            for r in rows:
                if r["xbrl"] == "XBRL_MISMATCH":
                    r["xbrl"] = "XBRL_NOT_COVERED"
        elif n_match == len(rows):
            status = "ROWS_OK_FACTS_UNMATCHED"
        else:
            status = "MISMATCH"
    elif rows and zeros and all(r["value"] == 0 for r in rows):
        status = "VALIDATED"
    elif rows:
        status = "PROSE_ONLY"
    elif statement:
        status = "STATEMENT_VS_FACTS" if nonzero else "NO_FLOAT_STATED"
        if zeros and not nonzero:
            filing_flags.append("ZERO_FACT_AGREES")
    elif nonzero:
        status = "MISSED_BY_PROSE"
    elif zeros:
        status = "ZERO_FACT"
    elif nils:
        status = "NIL_FACT"
    else:
        status = "EMPTY"

    for r in rows:
        ext_rows.append({
            "accession": row.accession, "cik": row.cik,
            "company_name": row.company_name, "form": row.form,
            "value": fmt(r["value"]),
            "value_xbrl": fmt(r["value_xbrl"])
                          if r.get("value_xbrl", "") != "" else "",
            "tol": fmt(r["tol"]), "raw": r["raw"], "label": r["label"],
            "as_of": r["as_of"], "method": r["method"], "xbrl": r["xbrl"],
            "flags": ";".join(sorted(set(r["flags"]))),
            "filing_index_url": row.filing_index_url,
        })
    status_rows.append({
        "accession": row.accession, "form": row.form, "status": status,
        "n_rows": len(rows), "n_facts": len(nonzero),
        "n_zero_facts": len(zeros), "n_nil_facts": len(nils),
        "xbrl_source": xbrl_source,
        "filing_flags": ";".join(sorted(set(filing_flags)))})

    if i % 500 == 0 or i == n:
        print("[%d/%d] extracted" % (i, n), flush=True)

ext = pd.DataFrame(ext_rows).sort_values(
    ["accession", "value"]).reset_index(drop=True)
ext.to_csv(extraction_path, index=False, encoding="utf-8", lineterminator="\n")
st = pd.DataFrame(status_rows).sort_values("accession").reset_index(drop=True)
st.to_csv(status_path, index=False, encoding="utf-8", lineterminator="\n")

print("\nwrote %s (%d float rows)" % (extraction_path, len(ext)))
print("wrote %s (%d filings)" % (status_path, len(st)))
print("\nfiling status x form:")
print(st.groupby(["status", "form"]).size().unstack(fill_value=0).to_string())
relevant = st[~st["status"].isin(["EMPTY", "NO_FLOAT_STATED", "ZERO_FACT",
                                  "NIL_FACT"])]
ok = st["status"].isin(["VALIDATED", "AGG_VALIDATED"]).sum()
denom = len(relevant) + st["status"].isin(["NO_FLOAT_STATED", "ZERO_FACT",
                                           "NIL_FACT"]).sum()
print("\nvalidated: %d; float-relevant filings (non-EMPTY): %d"
      % (ok, denom))
