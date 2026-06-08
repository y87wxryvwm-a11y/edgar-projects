"""5_reconcile_audit.py — compare the independent audit against the extractor.

The audit sub-agents read each filing's neutral evidence packet and decided the
ground truth themselves (number / class / date, or "true negative"), writing
their verdicts under audit/<ROUND>/results/. This script reconciles those
verdicts against what the extractor actually produced (validation_input_*.jsonl)
— entirely in code, so the comparison is exact and reproducible.

It writes a reconciliation CSV (one row per filing) and a disagreements JSON
(the filings whose extraction the audit did not confirm) — the worklist for the
adjudication / parser-fix round. The XBRL_MATCH flag is carried through as a
third witness: when the extractor disagrees with the auditor but agrees with
SEC's structured dei fact, the auditor is the more likely party to have misread.
"""

import os
import csv
import glob
import json

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000
ROUND = "round1"
NUM_TOL = 0.002          # 0.2% — absorbs "5,822 million" cover rounding only
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")

directory = DATA_DIR.replace("\\", "/")
val_jsonl = os.path.join(directory, f"validation_input_{YEAR}_n{SAMPLE_SIZE}.jsonl")
result_dir = os.path.join(directory, "audit", ROUND, "results")
recon_csv = os.path.join(directory, f"reconciliation_{YEAR}_n{SAMPLE_SIZE}_{ROUND}.csv")
disagree_json = os.path.join(directory, f"disagreements_{YEAR}_n{SAMPLE_SIZE}_{ROUND}.json")

EQUITY = {"common", "ordinary"}          # treated as interchangeable primary-equity labels


def _coerce_int(x):
    """Auditor numbers may arrive as int or as a string ('1,228,504,232')."""
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    try:
        return int(str(x).replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def _aud_class_numbers(aud_classes):
    """List of (int_number, class_dict) for classes with a usable number."""
    out = []
    for c in aud_classes:
        n = _coerce_int(c.get("number"))
        if n:
            out.append((n, c))
    return out


def load_claims():
    claims = {}
    with open(val_jsonl, encoding="utf-8") as f:
        for ln in f:
            r = json.loads(ln)
            claims[r["accession"]] = r
    return claims


def load_audits():
    audits = {}
    for path in glob.glob(os.path.join(result_dir, "*.json")):
        with open(path, encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception as e:
                print(f"  !! unparseable {os.path.basename(path)}: {e}")
                continue
        for rec in data.get("results", []):
            audits[rec["accession"]] = rec
    return audits


def pair_numbers(aud, ext, tol=NUM_TOL):
    """Greedily pair auditor numbers to extractor numbers (exact, then within
    tol). Returns (pairs, aud_only, ext_only, used_rounding)."""
    ext_left = list(ext)
    pairs, aud_only, rounded = [], [], False
    for a in aud:
        if a in ext_left:
            pairs.append((a, a)); ext_left.remove(a); continue
        hit = next((e for e in ext_left if e and abs(a - e) <= tol * max(a, e)), None)
        if hit is not None:
            pairs.append((a, hit)); ext_left.remove(hit); rounded = True
        else:
            aud_only.append(a)
    return pairs, aud_only, ext_left, rounded


def reconcile(claim, audit):
    """Return (status, detail) for one filing."""
    ext_entries = claim.get("script_entries", [])
    ext_nums = [e["shares"] for e in ext_entries if e.get("shares")]
    ext_dates = {e["shares"]: e.get("as_of_date", "") for e in ext_entries if e.get("shares")}
    ext_types = {e["shares"]: e.get("share_type", "") for e in ext_entries if e.get("shares")}
    flags = claim.get("flags", [])
    xbrl_ok = "XBRL_MATCH" in flags

    if audit is None:
        return "NO_AUDIT", "no auditor verdict found"

    verdict = audit.get("verdict", "")
    if verdict == "UNDETERMINABLE":
        return "NEEDS_REVIEW", audit.get("notes", "")[:200]

    aud_pairs = _aud_class_numbers(audit.get("classes", []))
    aud_nums = [n for n, _ in aud_pairs]

    if verdict == "TRUE_NEGATIVE":
        if not ext_nums:
            return "AGREE_TRUE_NEG", ""
        return "DISAGREE_FALSE_POS", f"auditor=none ext={ext_nums}"

    # verdict == HAS_SHARES
    if not ext_nums:
        return "DISAGREE_MISS", f"auditor={aud_nums} ext=none xbrl_ok={xbrl_ok}"

    pairs, aud_only, ext_only, rounded = pair_numbers(sorted(aud_nums), sorted(ext_nums))
    if aud_only:
        return "DISAGREE_NUMBER", (f"auditor_only={aud_only} ext={ext_nums} "
                                   f"xbrl_ok={xbrl_ok} | {audit.get('notes','')[:120]}")
    if ext_only:
        return "DISAGREE_EXTRA", f"ext_only={ext_only} auditor={aud_nums} xbrl_ok={xbrl_ok}"

    # numbers all matched — check dates and types on the matched pairs
    date_problems, type_problems = [], []
    aud_dates = {n: c.get("as_of_date", "") for n, c in aud_pairs}
    aud_types = {n: c.get("share_type", "") for n, c in aud_pairs}
    for a, e in pairs:
        ad, ed = aud_dates.get(a, ""), ext_dates.get(e, "")
        if ad and ed and ad != ed:
            date_problems.append(f"{e}:{ed}!={ad}")
        at, et = aud_types.get(a, ""), ext_types.get(e, "")
        if at and et and at != et and not ({at, et} <= EQUITY):
            type_problems.append(f"{e}:{et}!={at}")
    if date_problems:
        return "DISAGREE_DATE", "; ".join(date_problems)
    if type_problems:
        return "DISAGREE_TYPE", "; ".join(type_problems)
    return ("AGREE_ROUNDED" if rounded else "AGREE"), ""


def fmt_classes(items, num_key, type_key, date_key):
    return " | ".join(
        f"{it.get(num_key)}/{it.get(type_key,'')}/{it.get(date_key,'')}" for it in items)


def main():
    claims = load_claims()
    audits = load_audits()
    print(f"claims: {len(claims)}  audits: {len(audits)}")

    rows, disagreements = [], []
    counts = {}
    for acc, claim in claims.items():
        audit = audits.get(acc)
        status, detail = reconcile(claim, audit)
        counts[status] = counts.get(status, 0) + 1
        row = {
            "accession": acc, "company": claim.get("company", ""),
            "form": claim.get("form", ""), "status": status, "detail": detail,
            "flags": ";".join(claim.get("flags", [])),
            "ext": fmt_classes(claim.get("script_entries", []), "shares", "share_type", "as_of_date"),
            "aud": fmt_classes(audit.get("classes", []) if audit else [], "number", "share_type", "as_of_date"),
        }
        rows.append(row)
        if status.startswith("DISAGREE") or status in ("NEEDS_REVIEW", "NO_AUDIT"):
            disagreements.append({**row, "cik": claim.get("cik", ""),
                                  "txt_url": claim.get("txt_url", ""),
                                  "aud_notes": audit.get("notes", "") if audit else ""})

    with open(recon_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["accession", "company", "form", "status",
                                          "detail", "flags", "ext", "aud"])
        w.writeheader()
        w.writerows(rows)
    with open(disagree_json, "w", encoding="utf-8") as f:
        json.dump(disagreements, f, indent=2)

    n = len(claims)
    agree = sum(v for k, v in counts.items() if k.startswith("AGREE"))
    print(f"\n=== RECONCILIATION (n={n}, round={ROUND}) ===")
    for k in sorted(counts, key=lambda x: -counts[x]):
        print(f"  {k:22} {counts[k]:>5}")
    print(f"  {'-'*28}")
    print(f"  CONFIRMED (any AGREE)  {agree:>5}  ({agree/n:.1%})")
    print(f"  NEEDS WORK             {n-agree:>5}")
    print(f"\nWrote {recon_csv}")
    print(f"Wrote {disagree_json}  ({len(disagreements)} filings)")


if __name__ == "__main__":
    main()
