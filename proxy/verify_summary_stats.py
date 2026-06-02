"""Verify proxy_summary_stats.csv against an independent recomputation.

Re-computes each sub-stat from synthetic_proxy.csv using different pandas idioms
(value_counts / describe) than compute_summary_stats.py, then checks the value
in every row of the long, window-grouped output file.
"""

import os
import re

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
data_filename = "synthetic_proxy.csv"
stats_filename = "proxy_summary_stats.csv"
# -----------------------------------------------------------------------------

# directory is read from config.py (git-ignored) so each machine keeps its own
# data-folder path and a `git pull` never overwrites it. Copy config.example.py
# to config.py in this folder and set DATA_DIR.
try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py in this "
        "folder and set DATA_DIR to your proxy data folder."
    )

directory = DATA_DIR.replace("\\", "/")
df = pd.read_csv(os.path.join(directory, data_filename))
out = pd.read_csv(os.path.join(directory, stats_filename))


def pct(x, t):
    return round(100 * x / t, 2) if t else float("nan")


def independent(sub):
    """base_label -> expected value, computed an independent way."""
    n = sub.shape[0]
    res_vc = sub["myproposal_result"].value_counts(dropna=False)
    pc_vc = sub["Proxy Category"].value_counts(dropna=False)
    pt_vc = sub["Proponent Type Code"].value_counts(dropna=True)
    ppr_vc = sub["Proxy Proposal Result"].value_counts(dropna=False)
    pt_nonnull = int(sub["Proponent Type Code"].dropna().shape[0])
    # Votes For As % Votes Cast is only meaningful for voted proposals; non-voted
    # rows carry a spurious 0 in the source data and are excluded (matches
    # compute_summary_stats.py).
    voted_votes = sub.loc[sub["myproposal_result"] == "1voted", "Votes For As % Votes Cast"]
    votes_desc = voted_votes.describe()

    voted = int(res_vc.get("1voted", 0))
    omitted = int(res_vc.get("2omitted", 0))
    withdrawn = int(res_vc.get("3withdrawn", 0))
    sp500 = int(sub["Index Mtg SP50"].sum())
    cgov = int(pc_vc.get("Corporate Governance", 0))
    social = int(pc_vc.get("Social/Environmental Issues", 0))
    individual = int(pt_vc.get("INDIVIDUAL", 0))
    institutional = int(pt_vc.drop(labels=["INDIVIDUAL"], errors="ignore").sum())
    passed = int(ppr_vc.get("Pass", 0))

    # Concepts 12-16, recomputed via drop_duplicates / groupby / crosstab idioms.
    unique_ciks = int(sub["cik"].drop_duplicates().shape[0])
    inv_mask = (
        sub["Factset Industry Desc"].str.strip().str.lower()
        .isin(["investment managers", "investment trusts/mutual funds"])
    )
    investment_ciks = int(len(set(sub.loc[inv_mask, "cik"])))
    sought = int(sub.groupby("mynoaction_sought").size().get(1, 0))
    granted = int(sub.groupby("mynoaction_granted").size().get(1, 0))
    granted_not_voted = int(
        pd.crosstab(sub["mynoaction_granted"], sub["myproposal_result"] == "1voted")
        .reindex(index=[1], columns=[False], fill_value=0)
        .iloc[0, 0]
    )

    return {
        "1.1": n,
        "2.1": voted, "2.2": pct(voted, n),
        "3.1": omitted, "3.2": pct(omitted, n),
        "4.1": withdrawn, "4.2": pct(withdrawn, n),
        "5.1": sp500, "5.2": pct(sp500, n),
        "6.1": cgov, "6.2": pct(cgov, n),
        "7.1": social, "7.2": pct(social, n),
        "8.1": individual, "8.2": pct(individual, pt_nonnull),
        "9.1": institutional, "9.2": pct(institutional, pt_nonnull),
        "10.1": round(float(votes_desc["mean"]), 2), "10.2": float(votes_desc["50%"]),
        "11.1": passed, "11.2": pct(passed, n),
        "12.1": unique_ciks,
        "13.1": investment_ciks,
        "14.1": sought, "14.2": pct(sought, n),
        "15.1": granted, "15.2": pct(granted, sought),
        "16.1": granted_not_voted, "16.2": pct(granted_not_voted, granted),
    }


expected_by_window = {
    "a": independent(df.loc[df["year"] == 2025]),
    "b": independent(df.loc[df["year"].between(2022, 2025)]),
}


def approx(expected, actual):
    return round(float(expected), 2) == round(float(actual), 2)


print(f"{'STATUS':6s} {'STAT':8s} {'EXPECTED':12s} ACTUAL")
print("-" * 50)
results = []
for _, row in out.iterrows():
    m = re.fullmatch(r"(\d+\.\d+)([ab])", str(row["stat_number"]).strip())
    base, letter = m.group(1), m.group(2)
    expected = expected_by_window[letter][base]
    actual = row["value"]
    ok = approx(expected, actual)
    results.append(ok)
    status = "PASS" if ok else "FAIL"
    print(f"{status:6s} {row['stat_number']:8s} {str(expected):12s} {actual}")

passed = sum(results)
total = len(results)
print()
print(f"{passed}/{total} stats verified")
if passed != total:
    print("VERIFICATION FAILED")
