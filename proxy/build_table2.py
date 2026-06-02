"""Build Table 2 -- Shareholder proposal voting support.

For each proposal group (all proposals, then by topic, then by proponent type)
reports three voting-support measures, every cell an integer percentage. All
three are computed over proposals that actually went to a vote
(myproposal_result == 1voted); omitted/withdrawn rows carry a spurious 0 in the
source data and are excluded.
  - Votes cast in favor (average): mean of Votes For As % Votes Cast
  - Votes cast in favor (median): median of Votes For As % Votes Cast
  - Proposals with majority support: share with Votes For As % Votes Cast > 50

One table per population: A = year == 2025, B = 2022 <= year <= 2025.
Outputs table2_2025.csv and table2_2022_2025.csv (and prints both as markdown).
"""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
data_filename = "synthetic_proxy.csv"
majority_threshold = 50  # "majority support" = Votes For As % Votes Cast > this
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

VOTES = "Votes For As % Votes Cast"

COLUMNS = [
    "Votes cast in favor (average)",
    "Votes cast in favor (median)",
    "Proposals with majority support",
]

# (section, row label, mask). "" section = the top line (no section header).
ROW_DEFS = [
    ("", "All Proposals", lambda s: pd.Series(True, index=s.index)),
    ("Proposal Topic", "Governance", lambda s: s["Proxy Category"] == "Corporate Governance"),
    ("Proposal Topic", "Environmental and Social", lambda s: s["Proxy Category"] == "Social/Environmental Issues"),
    ("Proponent Type", "Institution", lambda s: s["Proponent Type Code"].notna() & (s["Proponent Type Code"] != "INDIVIDUAL")),
    ("Proponent Type", "Individual", lambda s: s["Proponent Type Code"] == "INDIVIDUAL"),
]


def pctfmt(x):
    return f"{x:.0f}%"


def metrics(sub):
    # Voting-support measures apply only to proposals that went to a vote.
    # Non-voted rows (omitted/withdrawn) carry a spurious 0 in the source data,
    # so restrict to myproposal_result == 1voted before averaging/thresholding.
    v = sub.loc[sub["myproposal_result"] == "1voted", VOTES]
    avg = v.mean()
    median = v.median()
    majority = 100 * (v > majority_threshold).mean()
    return [pctfmt(avg), pctfmt(median), pctfmt(majority)]


def missing_footnote(sub):
    n = len(sub)
    m = int(sub["Proponent Type Code"].isna().sum())
    pct = 100 * m / n if n else 0
    return (f"Note: Proponent Type Code is missing for {m} of {n} proposals "
            f"({pct:.1f}%); these are excluded from the Institution and Individual "
            f"rows. It is the only variable with missing values.")


def build_table(sub):
    rows = [[""] + COLUMNS]
    last_section = None
    for section, label, fn in ROW_DEFS:
        if section and section != last_section:
            rows.append([section] + [""] * len(COLUMNS))
            last_section = section
        rows.append([label] + metrics(sub.loc[fn(sub)]))
    return rows


def to_markdown(rows):
    header, body = rows[0], rows[1:]
    lines = ["| " + " | ".join(header) + " |",
             "| " + " | ".join(["---"] * len(header)) + " |"]
    lines += ["| " + " | ".join(str(x) for x in r) + " |" for r in body]
    return "\n".join(lines)


populations = [
    ("2025", "table2_2025.csv", df.loc[df["year"] == 2025]),
    ("2022-2025", "table2_2022_2025.csv", df.loc[df["year"].between(2022, 2025)]),
]

for tag, fname, sub in populations:
    rows = build_table(sub)
    note = missing_footnote(sub)
    body = rows[1:] + [[""] * len(rows[0]), [note] + [""] * (len(rows[0]) - 1)]
    pd.DataFrame(body, columns=rows[0]).to_csv(os.path.join(directory, fname), index=False)
    print(f"\n=== Table 2 ({tag}) -- Shareholder proposal voting support ===")
    print(to_markdown(rows))
    print(f"\n{note}")
