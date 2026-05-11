"""Attach a predicted CIK to each row of iss_only_v2 by matching company_name
against ciks_combined_unique. Exact normalized match wins; otherwise take the
best rapidfuzz score. All rows get a best-available guess plus a score, so
borderline cases can be eyeballed in the output.

Requires: pip install rapidfuzz
"""

import os
import re

import pandas as pd
from rapidfuzz import fuzz, process

# ---- EDIT THIS --------------------------------------------------------------
save_directory = "/Users/avilae/claude-code/projects/edgar-projects/iss-factset-reconciliation"
iss_filename = "iss_only_v2.csv"
cik_filename = "ciks_combined_unique.csv"
output_filename = "iss_only_v2_with_predicted_cik.csv"

iss_name_col = "company_name"
cik_name_col = "company_name"
cik_col = "CIK"
# -----------------------------------------------------------------------------

iss_path = os.path.join(save_directory, iss_filename)
cik_path = os.path.join(save_directory, cik_filename)
output_path = os.path.join(save_directory, output_filename)


SUFFIX_TOKENS = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "COMPANY", "CO",
    "LTD", "LIMITED", "LLC", "LP", "LLP", "PLC", "HOLDINGS", "HOLDING",
    "GROUP", "TRUST", "NV", "SA", "AG", "PARTNERS", "CLASS", "THE",
}


def normalize(name: str) -> str:
    """Aggressive canonicalization so cosmetic variations collide."""
    if pd.isna(name):
        return ""
    s = str(name).upper()
    s = re.sub(r"\(.*?\)", " ", s)          # drop parenthetical tickers/notes
    s = s.replace("&", " AND ")
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)      # strip punctuation
    s = re.sub(r"\s+", " ", s).strip()
    tokens = [t for t in s.split(" ") if t and t not in SUFFIX_TOKENS]
    return " ".join(tokens)


def main() -> None:
    iss = pd.read_csv(iss_path)
    cik_df = pd.read_csv(cik_path, dtype={cik_col: str})

    cik_df = cik_df[[cik_name_col, cik_col]].dropna(subset=[cik_name_col]).copy()
    cik_df["_norm"] = cik_df[cik_name_col].map(normalize)
    cik_df = cik_df[cik_df["_norm"] != ""]

    # Exact-lookup dict on normalized name. If a normalized name maps to
    # multiple CIKs we keep the first and flag how many others existed.
    exact_lookup: dict[str, tuple[str, str]] = {}
    exact_collisions: dict[str, int] = {}
    for _, row in cik_df.iterrows():
        key = row["_norm"]
        if key in exact_lookup:
            exact_collisions[key] = exact_collisions.get(key, 1) + 1
        else:
            exact_lookup[key] = (row[cik_col], row[cik_name_col])

    # Fuzzy-pool: unique normalized names, with back-pointer to CIK + original.
    fuzzy_choices = list(exact_lookup.keys())

    iss["_norm"] = iss[iss_name_col].map(normalize)

    predicted_cik = []
    match_pct = []
    matched_company_name = []
    match_method = []

    for norm in iss["_norm"]:
        if norm == "":
            predicted_cik.append("")
            match_pct.append(0.0)
            matched_company_name.append("")
            match_method.append("no_input")
            continue

        if norm in exact_lookup:
            cik_val, orig = exact_lookup[norm]
            predicted_cik.append(cik_val)
            match_pct.append(100.0)
            matched_company_name.append(orig)
            match_method.append("exact")
            continue

        best = process.extractOne(norm, fuzzy_choices, scorer=fuzz.token_set_ratio)
        if best is None:
            predicted_cik.append("")
            match_pct.append(0.0)
            matched_company_name.append("")
            match_method.append("no_match")
            continue

        best_norm, score, _ = best
        cik_val, orig = exact_lookup[best_norm]
        predicted_cik.append(cik_val)
        match_pct.append(round(score, 2))
        matched_company_name.append(orig)
        match_method.append("fuzzy")

    iss["matched_company_name"] = matched_company_name
    iss["predicted_cik"] = predicted_cik
    iss["match_pct"] = match_pct
    iss["match_method"] = match_method
    iss = iss.drop(columns="_norm")

    exact_n = sum(1 for m in match_method if m == "exact")
    fuzzy_n = sum(1 for m in match_method if m == "fuzzy")
    below_92 = sum(1 for m, p in zip(match_method, match_pct)
                   if m == "fuzzy" and p < 92)
    print(f"Input rows:            {len(iss):,}")
    print(f"Exact matches:         {exact_n:,}")
    print(f"Fuzzy matches:         {fuzzy_n:,}")
    print(f"  of which < 92%:      {below_92:,}  (manual review)")
    if exact_collisions:
        print(f"Normalized-name collisions in CIK file: "
              f"{len(exact_collisions)} (kept first CIK each)")

    iss.to_csv(output_path, index=False)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
