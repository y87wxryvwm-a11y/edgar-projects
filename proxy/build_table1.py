"""Build Table 1 -- Shareholder proposal submissions by status.

Cross-tabulates proposal status (Voted On / Omitted / Withdrawn / Total) against
company size, proposal topic, and proponent type. Each cell shows the count and
that count as a percentage of the column's top-line "Number". One table per
population: A = year == 2025, B = 2022 <= year <= 2025.

Outputs table1_2025.csv and table1_2022_2025.csv (and prints both as markdown).
"""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
directory = r"/Users/avilae/claude-code/projects/edgar-projects/proxy"
data_filename = "synthetic_proxy.csv"
pct_decimals = 1            # decimal places on the percentages
show_number_row_pct = False  # True -> print "(100.0%)" next to the Number row
# -----------------------------------------------------------------------------

directory = directory.replace("\\", "/")
df = pd.read_csv(os.path.join(directory, data_filename))

# Status masks -> the table columns. Total is every row in the population.
STATUS_COLUMNS = [
    ("Voted On", lambda s: s["myproposal_result"] == "1voted"),
    ("Omitted", lambda s: s["myproposal_result"] == "2omitted"),
    ("Withdrawn", lambda s: s["myproposal_result"] == "3withdrawn"),
    ("Total", lambda s: pd.Series(True, index=s.index)),
]

# Breakdown masks -> the table rows, grouped into sections.
ROW_DEFS = [
    ("Company Size", "S&P 500", lambda s: s["Index Mtg SP50"] == 1),
    ("Company Size", "All other", lambda s: s["Index Mtg SP50"] == 0),
    ("Proposal Topic", "Governance", lambda s: s["Proxy Category"] == "Corporate Governance"),
    ("Proposal Topic", "Environmental & Social", lambda s: s["Proxy Category"] == "Social/Environmental Issues"),
    ("Proponent Type", "Institution", lambda s: s["Proponent Type Code"].notna() & (s["Proponent Type Code"] != "INDIVIDUAL")),
    ("Proponent Type", "Individual", lambda s: s["Proponent Type Code"] == "INDIVIDUAL"),
]


def fmt(count, base):
    pct = (100 * count / base) if base else float("nan")
    return f"{count} ({pct:.{pct_decimals}f}%)"


def build_table(sub):
    """Return a list of rows (first row is the header)."""
    status_masks = [(name, fn(sub)) for name, fn in STATUS_COLUMNS]
    bases = {name: int(mask.sum()) for name, mask in status_masks}

    rows = [["Proposal Status"] + [name for name, _ in status_masks]]

    # Top line: the column denominators.
    number_row = ["Number"]
    for name, _ in status_masks:
        c = bases[name]
        number_row.append(fmt(c, c) if show_number_row_pct else str(c))
    rows.append(number_row)

    # Breakdown rows, one section header per section.
    last_section = None
    for section, label, fn in ROW_DEFS:
        if section != last_section:
            rows.append([section] + [""] * len(status_masks))
            last_section = section
        row_mask = fn(sub)
        cells = [label]
        for name, status_mask in status_masks:
            c = int((row_mask & status_mask).sum())
            cells.append(fmt(c, bases[name]))
        rows.append(cells)
    return rows


def missing_footnote(sub):
    n = len(sub)
    m = int(sub["Proponent Type Code"].isna().sum())
    pct = 100 * m / n if n else 0
    return (f"Note: Proponent Type Code is missing for {m} of {n} proposals "
            f"({pct:.1f}%); these are excluded from the Institution and Individual "
            f"rows. It is the only variable with missing values.")


def to_markdown(rows):
    header, body = rows[0], rows[1:]
    lines = ["| " + " | ".join(header) + " |",
             "| " + " | ".join(["---"] * len(header)) + " |"]
    lines += ["| " + " | ".join(str(x) for x in r) + " |" for r in body]
    return "\n".join(lines)


populations = [
    ("2025", "table1_2025.csv", df.loc[df["year"] == 2025]),
    ("2022-2025", "table1_2022_2025.csv", df.loc[df["year"].between(2022, 2025)]),
]

for tag, fname, sub in populations:
    rows = build_table(sub)
    note = missing_footnote(sub)
    body = rows[1:] + [[""] * len(rows[0]), [note] + [""] * (len(rows[0]) - 1)]
    pd.DataFrame(body, columns=rows[0]).to_csv(os.path.join(directory, fname), index=False)
    print(f"\n=== Table 1 ({tag}) -- Shareholder proposal submissions by status ===")
    print(to_markdown(rows))
    print(f"\n{note}")
