"""Compute proxy-proposal summary statistics defined in proxy/data_ask.txt.

For each filter window (year == 2025; 2022 <= year <= 2025) produces the 17
stats requested in data_ask.txt and writes them to proxy_summary_stats.csv
with one row per (filter, stat) and the human-readable description alongside
the value.
"""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
directory = r"/Users/avilae/claude-code/projects/edgar-projects/proxy"
data_filename = "synthetic_proxy.csv"
output_filename = "proxy_summary_stats.csv"
# -----------------------------------------------------------------------------

directory = directory.replace("\\", "/")
data_path = os.path.join(directory, data_filename)
output_path = os.path.join(directory, output_filename)

df = pd.read_csv(data_path)


def pct(x, total):
    return round(100 * x / total, 2) if total else float("nan")


def stats_block(sub, filter_label):
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

    return [
        (filter_label, 1, "Total count of proposals", n),
        (filter_label, 2, "Count voted on (myproposal_result == 1voted)", voted),
        (filter_label, 3, "Stat 2 as a percent of Stat 1", pct(voted, n)),
        (filter_label, 4, "Count omitted following no-action letter (myproposal_result == 2omitted)", omitted),
        (filter_label, 5, "Stat 4 as a percent of Stat 1", pct(omitted, n)),
        (filter_label, 6, "Count withdrawn by proponent (myproposal_result == 3withdrawn)", withdrawn),
        (filter_label, 7, "Stat 6 as a percent of Stat 1", pct(withdrawn, n)),
        (filter_label, 8, "Count at S&P 500 meetings (Index Mtg SP50 == 1)", sp500),
        (filter_label, 9, "Stat 8 as a percent of Stat 1", pct(sp500, n)),
        (filter_label, 10, "Count related to corporate governance (Proxy Category == Corporate Governance)", cgov),
        (filter_label, 11, "Stat 10 as a percent of Stat 1", pct(cgov, n)),
        (filter_label, 12, "Count related to social/environmental issues (Proxy Category == Social/Environmental Issues)", social),
        (filter_label, 13, "Stat 12 as a percent of Stat 1", pct(social, n)),
        (filter_label, 14, "Count submitted by individual proponents (Proponent Type Code == INDIVIDUAL)", individual),
        (filter_label, 15, "Stat 14 as a percent of non-missing Proponent Type Code rows", pct(individual, pt_nonnull)),
        (filter_label, 16, "Count submitted by institutional proponents (Proponent Type Code not INDIVIDUAL and not missing)", institutional),
        (filter_label, 17, "Stat 16 as a percent of non-missing Proponent Type Code rows", pct(institutional, pt_nonnull)),
    ]


rows = []
rows.extend(stats_block(df.loc[df["year"] == 2025], "year_2025"))
rows.extend(stats_block(df.loc[df["year"].between(2022, 2025)], "year_2022_2025"))

out = pd.DataFrame(rows, columns=["filter", "stat_num", "description", "value"])
out.to_csv(output_path, index=False)
print(f"Wrote {len(out)} stats to {output_path}")
