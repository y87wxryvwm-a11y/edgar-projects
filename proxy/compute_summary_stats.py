"""Compute proxy-proposal summary statistics defined in proxy/data_ask.txt.

Stats use the addressing scheme <concept>.<part><window>:
  concept = 1..16  (proposal category / measure)
  part    = .1, .2  (count then percent; or mean then median for concept 10)
  window  = a (year == 2025), b (2022 <= year <= 2025)

Output: proxy_summary_stats.csv -- one row per sub-stat with columns
filter, stat_number (e.g. "2.1a"), description, value. Every window-a row is
emitted before every window-b row, and concepts run 1..16 in numeric order so
10.x..16.x follow 9.x (rather than string-sorting where "10" precedes "2").
"""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
data_filename = "synthetic_proxy.csv"
output_filename = "proxy_summary_stats.csv"
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
data_path = os.path.join(directory, data_filename)
output_path = os.path.join(directory, output_filename)

df = pd.read_csv(data_path)


def pct(x, total):
    return round(100 * x / total, 2) if total else float("nan")


def stat_rows(sub):
    """Return [(base_label, description, value), ...] in numeric concept order.

    base_label is the stat number without the window letter, e.g. "2.1".
    """
    n = len(sub)
    voted = int((sub["myproposal_result"] == "1voted").sum())
    omitted = int((sub["myproposal_result"] == "2omitted").sum())
    withdrawn = int((sub["myproposal_result"] == "3withdrawn").sum())
    sp500 = int((sub["Index Mtg SP50"] == 1).sum())
    cgov = int((sub["Proxy Category"] == "Corporate Governance").sum())
    social = int((sub["Proxy Category"] == "Social/Environmental Issues").sum())
    pt = sub["Proponent Type Code"]
    pt_nonnull = int(pt.notna().sum())
    individual = int((pt == "INDIVIDUAL").sum())
    institutional = pt_nonnull - individual
    # Votes For As % Votes Cast is only meaningful for proposals that actually
    # went to a vote. Non-voted rows (omitted/withdrawn) carry a spurious 0 in the
    # source data, so restrict the mean/median to myproposal_result == 1voted.
    votes = sub.loc[sub["myproposal_result"] == "1voted", "Votes For As % Votes Cast"]
    votes_mean = round(float(votes.mean()), 2)
    votes_median = float(votes.median())
    passed = int((sub["Proxy Proposal Result"] == "Pass").sum())
    unique_ciks = int(sub["cik"].nunique())
    # Match industry names case-insensitively: the canonical labels use title case
    # ("Investment Managers") while this list is lowercase, so normalize first.
    desc_norm = sub["Factset Industry Desc"].str.strip().str.lower()
    investment_industries = ["investment managers", "investment trusts/mutual funds"]
    investment_ciks = int(sub.loc[desc_norm.isin(investment_industries), "cik"].nunique())
    sought = int((sub["mynoaction_sought"] == 1).sum())
    granted = int((sub["mynoaction_granted"] == 1).sum())
    granted_not_voted = int(
        ((sub["mynoaction_granted"] == 1) & (sub["myproposal_result"] != "1voted")).sum()
    )

    return [
        ("1.1", "Total count of proposals", n),
        ("2.1", "Voted on (myproposal_result == 1voted)", voted),
        ("2.2", "Voted on, as % of total (stat 1.1)", pct(voted, n)),
        ("3.1", "Omitted after no-action letter (myproposal_result == 2omitted)", omitted),
        ("3.2", "Omitted, as % of total (stat 1.1)", pct(omitted, n)),
        ("4.1", "Withdrawn by proponent (myproposal_result == 3withdrawn)", withdrawn),
        ("4.2", "Withdrawn, as % of total (stat 1.1)", pct(withdrawn, n)),
        ("5.1", "S&P 500 meetings (Index Mtg SP50 == 1)", sp500),
        ("5.2", "S&P 500 meetings, as % of total (stat 1.1)", pct(sp500, n)),
        ("6.1", "Corporate governance (Proxy Category == Corporate Governance)", cgov),
        ("6.2", "Corporate governance, as % of total (stat 1.1)", pct(cgov, n)),
        ("7.1", "Social/environmental (Proxy Category == Social/Environmental Issues)", social),
        ("7.2", "Social/environmental, as % of total (stat 1.1)", pct(social, n)),
        ("8.1", "Individual proponents (Proponent Type Code == INDIVIDUAL)", individual),
        ("8.2", "Individual proponents, as % of non-missing Proponent Type Code", pct(individual, pt_nonnull)),
        ("9.1", "Institutional proponents (Proponent Type Code not INDIVIDUAL and not missing)", institutional),
        ("9.2", "Institutional proponents, as % of non-missing Proponent Type Code", pct(institutional, pt_nonnull)),
        ("10.1", "Average of Votes For As % Votes Cast", votes_mean),
        ("10.2", "Median of Votes For As % Votes Cast", votes_median),
        ("11.1", "Proxy Proposal Result == Pass", passed),
        ("11.2", "Pass, as % of total (stat 1.1)", pct(passed, n)),
        ("12.1", "Count of unique cik values", unique_ciks),
        ("13.1", "Count of unique cik values where Factset Industry Desc is 'investment managers' or 'investment trusts/mutual funds' (subset of stat 12.1)", investment_ciks),
        ("14.1", "Count where mynoaction_sought == 1", sought),
        ("14.2", "Stat 14.1 as % of total (stat 1.1)", pct(sought, n)),
        ("15.1", "Count where mynoaction_granted == 1", granted),
        ("15.2", "Stat 15.1 as % of stat 14.1 (no-action granted as % of sought)", pct(granted, sought)),
        ("16.1", "Count where mynoaction_granted == 1 and myproposal_result != 1voted", granted_not_voted),
        ("16.2", "Stat 16.1 as % of stat 15.1", pct(granted_not_voted, granted)),
    ]


# Window-a rows are emitted before window-b rows so the two blocks stay grouped.
windows = [
    ("a", "year_2025", df.loc[df["year"] == 2025]),
    ("b", "year_2022_2025", df.loc[df["year"].between(2022, 2025)]),
]

rows = []
for letter, label, sub in windows:
    for base, desc, value in stat_rows(sub):
        rows.append((label, f"{base}{letter}", desc, value))

out = pd.DataFrame(rows, columns=["filter", "stat_number", "description", "value"])
out.to_csv(output_path, index=False)
print(f"Wrote {len(out)} stat rows to {output_path}")
