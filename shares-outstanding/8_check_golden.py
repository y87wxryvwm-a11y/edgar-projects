"""8_check_golden.py — score the extractor against the golden ground-truth table.

After a parser change and a re-extraction, this compares the extractor's output
(validation_input_*.jsonl) against golden_*.json (built by 7_build_golden.py) and
reports, for every filing, PASS or a specific failure (missed number, extra
number, wrong date, wrong type, false positive/negative). The goal — 100% — is
reached when every filing PASSes. Writes golden_failures_*.json (the worklist of
filings still wrong) and prints a summary by failure kind and form.
"""

import os
import csv
import json

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000
NUM_TOL = 0.002          # 0.2% — absorbs "5,822 million" cover rounding only
CHECK_DATE = True
CHECK_TYPE = True        # treat common/ordinary as interchangeable
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")

directory = DATA_DIR.replace("\\", "/")
val_jsonl = os.path.join(directory, f"validation_input_{YEAR}_n{SAMPLE_SIZE}.jsonl")
golden_json = os.path.join(directory, f"golden_{YEAR}_n{SAMPLE_SIZE}.json")
fail_json = os.path.join(directory, f"golden_failures_{YEAR}_n{SAMPLE_SIZE}.json")

EQUITY = {"common", "ordinary"}


def _int(x):
    if isinstance(x, (int, float)):
        return int(x)
    try:
        return int(str(x).replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def pair_numbers(aud, ext, tol=NUM_TOL):
    ext_left = list(ext)
    pairs, aud_only = [], []
    for a in aud:
        if a in ext_left:
            pairs.append((a, a)); ext_left.remove(a); continue
        hit = next((e for e in ext_left if e and abs(a - e) <= tol * max(a, e)), None)
        if hit is not None:
            pairs.append((a, hit)); ext_left.remove(hit)
        else:
            aud_only.append(a)
    return pairs, aud_only, ext_left


def check(gold, ext_entries):
    g_classes = gold.get("classes", [])
    g_nums = [c["number"] for c in g_classes if c.get("number")]
    e_nums = [e["shares"] for e in ext_entries if e.get("shares")]

    if gold.get("verdict") == "TRUE_NEGATIVE":
        return ("PASS", "") if not e_nums else ("FAIL_FALSE_POS", f"ext={e_nums} truth=none")
    # HAS_SHARES
    if not e_nums:
        return "FAIL_MISS", f"truth={g_nums} ext=none"
    pairs, g_only, e_only = pair_numbers(sorted(g_nums), sorted(e_nums))
    if g_only:
        return "FAIL_MISS_NUMBER", f"missing={g_only} ext={e_nums}"
    if e_only:
        return "FAIL_EXTRA_NUMBER", f"extra={e_only} truth={g_nums}"
    # numbers match; check date + type
    e_date = {e["shares"]: e.get("as_of_date", "") for e in ext_entries if e.get("shares")}
    e_type = {e["shares"]: e.get("share_type", "") for e in ext_entries if e.get("shares")}
    g_date = {c["number"]: c.get("as_of_date", "") for c in g_classes if c.get("number")}
    g_type = {c["number"]: c.get("share_type", "") for c in g_classes if c.get("number")}
    if CHECK_DATE:
        for a, e in pairs:
            if g_date.get(a) and e_date.get(e) and g_date[a] != e_date[e]:
                return "FAIL_DATE", f"{e}: ext={e_date[e]} truth={g_date[a]}"
    if CHECK_TYPE:
        for a, e in pairs:
            gt, et = g_type.get(a, ""), e_type.get(e, "")
            if gt and et and gt != et and not ({gt, et} <= EQUITY):
                return "FAIL_TYPE", f"{e}: ext={et} truth={gt}"
    return "PASS", ""


def main():
    golden = json.load(open(golden_json, encoding="utf-8"))
    claims = {}
    with open(val_jsonl, encoding="utf-8") as f:
        for ln in f:
            r = json.loads(ln)
            claims[r["accession"]] = r

    counts, fails = {}, []
    for acc, gold in golden.items():
        claim = claims.get(acc)
        if claim is None:
            status, detail = "NO_EXTRACTION", "filing missing from extraction output"
        elif gold.get("source") == "UNRESOLVED":
            status, detail = "UNRESOLVED_GOLDEN", ""
        else:
            status, detail = check(gold, claim.get("script_entries", []))
        counts[status] = counts.get(status, 0) + 1
        if status != "PASS":
            fails.append({"accession": acc, "form": claim.get("form", "") if claim else "",
                          "company": claim.get("company", "") if claim else "",
                          "status": status, "detail": detail,
                          "flags": ";".join(claim.get("flags", [])) if claim else "",
                          "golden_source": gold.get("source", ""),
                          "ext": " | ".join(f"{e.get('shares')}/{e.get('share_type')}/{e.get('as_of_date')}"
                                            for e in (claim.get("script_entries", []) if claim else [])),
                          "truth": " | ".join(f"{c.get('number')}/{c.get('share_type')}/{c.get('as_of_date')}"
                                              for c in gold.get("classes", []))})

    with open(fail_json, "w", encoding="utf-8") as f:
        json.dump(fails, f, indent=1)

    n = len(golden)
    p = counts.get("PASS", 0)
    print(f"=== GOLDEN CHECK (n={n}) ===")
    for k in sorted(counts, key=lambda x: -counts[x]):
        print(f"  {k:20} {counts[k]:>5}")
    print(f"  {'-'*26}")
    print(f"  PASS RATE  {p}/{n}  ({p/n:.1%})")
    if fails:
        import collections
        byform = collections.Counter(x["form"] for x in fails)
        print(f"  failures by form: {dict(byform)}")
    print(f"Wrote {fail_json}  ({len(fails)} failures)")


if __name__ == "__main__":
    main()
