"""Line B -- Environmental & Social vs Governance, over time.

Four themed figures, full history (whatever year range the data spans), each a
small panel of year-axis line charts with Governance and Environmental-and-Social
plotted as two overlaid series:

  1_volume_by_topic.png     proposal count per year; each topic's share of all proposals
  2_outcomes_by_topic.png   share voted / omitted / withdrawn per year, by topic
  3_noaction_funnel.png     no-action sought rate; grant rate | sought; effective exclusion
  4_support_by_topic.png    mean & median Votes For (voted rows); majority-pass rate

Every panel marks two regime changes as vertical reference lines: SEC Staff Legal
Bulletin 14L (Nov 2021), which narrowed companies' ability to exclude E&S
proposals, and the 2020 Rule 14a-8 amendments (effective the 2022 season). The
hunt is for a structural break around 2021-2022 -- E&S surviving to a vote more
often after SLB 14L, and E&S support sliding through the 2022-2024 "backlash".

Definitions (topic, voted, omitted, withdrawn, majority support) are copied
verbatim from build_table1.py / build_table2.py / build_company_size.py so the
cuts stay consistent across every deliverable.

NOTE: on the synthetic data every column is drawn independently of the others, so
every series is flat and the two topics move in parallel by construction -- this
run validates the plotting plumbing; real signal awaits the reconciled ISS+FactSet
data.
"""

import os

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # render to file, no display needed (Spyder-safe)
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import PercentFormatter

# ---- EDIT THIS --------------------------------------------------------------
data_filename = "synthetic_proxy.csv"
sp500_only = False       # False = whole population (the Line B default: topic
                         #         trends across every proposal, full history).
                         # True  = restrict to S&P 500 (Index Mtg SP50 == 1).
majority_threshold = 50  # "Passed" = Votes For As % Votes Cast > this (voted rows)
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
outdir = os.path.join(directory, "topic_trends")  # charts + README live here
os.makedirs(outdir, exist_ok=True)
df = pd.read_csv(os.path.join(directory, data_filename))

VOTES = "Votes For As % Votes Cast"

# ---- definitions copied verbatim from the table / company-size builders -----
# (kept character-identical so the cuts stay consistent across deliverables)
TOPIC_PAIR = [
    ("Governance", lambda s: s["Proxy Category"] == "Corporate Governance"),
    ("Environmental and Social", lambda s: s["Proxy Category"] == "Social/Environmental Issues"),
]


def is_voted(s):
    return s["myproposal_result"] == "1voted"


def is_omitted(s):
    return s["myproposal_result"] == "2omitted"


def is_withdrawn(s):
    return s["myproposal_result"] == "3withdrawn"


def is_passed(s):
    # "Passed" = majority support, only meaningful on voted rows. Non-voted rows
    # (omitted/withdrawn) carry a spurious 0 in the source data, so they are
    # excluded (same restriction as build_table2.py / build_company_size.py).
    return is_voted(s) & (s[VOTES] > majority_threshold)


# ---- fixed visual identities so every panel reads the same way --------------
TOPIC_COLOR = {"Governance": "#1f77b4", "Environmental and Social": "#2ca02c"}
# (label, x-position, color, linestyle). Two near-simultaneous 2021-2022 regime
# changes -- kept at distinct positions AND distinct styles so they never merge
# into one ambiguous mark, even in the narrow 3-panel figures.
SLB_14L = ("SLB 14L (Nov 2021)", 2021.8, "#d62728", "--")             # crimson dashed
AMEND_2020 = ("2020 14a-8 amend. (2022 season)", 2022.2, "#7f7f7f", ":")  # gray dotted

# ---- universe ---------------------------------------------------------------
universe = df[df["Index Mtg SP50"] == 1].copy() if sp500_only else df.copy()
universe_label = "S&P 500" if sp500_only else "all companies"
years = sorted(int(y) for y in universe["year"].dropna().unique())

# Pass-definition divergence (Votes For > t vs Proxy Proposal Result == "Pass"),
# printed to the console only -- not shown on the charts.
voted_all = universe.loc[is_voted(universe)]
agree = ((voted_all[VOTES] > majority_threshold) == (voted_all["Proxy Proposal Result"] == "Pass")).mean()
divergence = 100 * (1 - float(agree)) if len(voted_all) else 0.0


# ---- per-(topic, year) series builders --------------------------------------
def _by_year(sub):
    """Count rows of `sub` per year, reindexed over the full year axis (0-filled)."""
    return sub.groupby("year").size().reindex(years, fill_value=0)


def topic_count():
    """Proposal count per year, per topic."""
    return {label: _by_year(universe.loc[mask(universe)]) for label, mask in TOPIC_PAIR}


def topic_share():
    """Each topic's share of ALL proposals that year (denominator = every category,
    so a rising E&S share is visible against the whole population)."""
    total = _by_year(universe).replace(0, np.nan)
    return {label: _by_year(universe.loc[mask(universe)]) / total for label, mask in TOPIC_PAIR}


def topic_rate(num_fn, den_fn=None):
    """Per topic, a per-year rate: count(num) / count(den). den defaults to the
    topic's full count that year. NaN where the denominator is 0 (drawn as a gap)."""
    out = {}
    for label, mask in TOPIC_PAIR:
        t = universe.loc[mask(universe)]
        num = _by_year(t.loc[num_fn(t)])
        den = _by_year(t) if den_fn is None else _by_year(t.loc[den_fn(t)])
        out[label] = num / den.replace(0, np.nan)
    return out


def topic_support(stat):
    """Per topic, the per-year mean/median of Votes For on voted rows only."""
    out = {}
    for label, mask in TOPIC_PAIR:
        t = universe.loc[mask(universe) & is_voted(universe)]
        out[label] = t.groupby("year")[VOTES].agg(stat).reindex(years)
    return out


# ---- plotting ---------------------------------------------------------------
def draw_reflines(ax):
    for _, x, color, ls in (SLB_14L, AMEND_2020):
        ax.axvline(x, color=color, ls=ls, lw=1.5, alpha=0.9, zorder=1)


def plot_panel(ax, series_by_topic, title, ylabel, pct=False, pct100=False):
    for label, _ in TOPIC_PAIR:
        s = series_by_topic[label]
        ax.plot(years, s.reindex(years).values, marker="o", ms=3.5, lw=1.6,
                color=TOPIC_COLOR[label], label=label, zorder=3)
    draw_reflines(ax)
    ax.set_title(title, fontsize=10)
    ax.set_xlabel("Year", fontsize=8)
    ax.set_ylabel(ylabel, fontsize=8)
    ax.set_xticks(years)
    ax.tick_params(axis="x", labelrotation=45, labelsize=7)
    ax.tick_params(axis="y", labelsize=7)
    if pct:           # fractions in 0..1 (shares, rates)
        ax.yaxis.set_major_formatter(PercentFormatter(xmax=1))
    elif pct100:      # already on a 0..100 scale (Votes For as % of votes cast)
        ax.yaxis.set_major_formatter(PercentFormatter(xmax=100))
    ax.margins(y=0.12)
    ax.set_ylim(bottom=0)  # honest zero baseline on every panel; top stays autoscaled
    ax.grid(True, axis="y", alpha=0.3)


# one shared legend (both topics + both reference lines) for every figure
LEGEND_HANDLES = (
    [Line2D([0], [0], color=TOPIC_COLOR[l], marker="o", ms=4, lw=1.6, label=l) for l, _ in TOPIC_PAIR]
    + [Line2D([0], [0], color=c, ls=ls, lw=1.5, label=name) for name, _, c, ls in (SLB_14L, AMEND_2020)]
)

written = []


def finish(fig, suptitle, foot, name):
    name = f"{len(written) + 1}_{name}"  # number the output files 1..4
    fig.suptitle(f"{suptitle} -- {universe_label}, {years[0]}-{years[-1]}", fontsize=12)
    fig.legend(handles=LEGEND_HANDLES, loc="lower center", ncol=4, fontsize=8,
               frameon=False, bbox_to_anchor=(0.5, 0.075))
    fig.text(0.01, 0.01, foot, fontsize=7, va="bottom")  # foot may be two lines
    fig.tight_layout(rect=[0, 0.15, 1, 0.95])
    fig.savefig(os.path.join(outdir, name), dpi=120)
    plt.close(fig)
    written.append(name)


SYN_NOTE = ("Synthetic data: columns are independent, so series are flat and the "
            "two topics track in parallel by construction (plumbing check only).")

# 1. Volume -- count, then share of all proposals
fig, axes = plt.subplots(1, 2, figsize=(13, 5.2))
plot_panel(axes[0], topic_count(), "Proposals per year", "Proposals")
plot_panel(axes[1], topic_share(), "Share of all proposals", "Share of all proposals", pct=True)
finish(fig, "Volume by topic", SYN_NOTE, "volume_by_topic.png")

# 2. Outcomes -- voted / omitted / withdrawn share (of that topic's proposals that year)
fig, axes = plt.subplots(1, 3, figsize=(15.5, 5.2))
plot_panel(axes[0], topic_rate(is_voted), "Voted on", "Share of topic", pct=True)
plot_panel(axes[1], topic_rate(is_omitted), "Omitted", "Share of topic", pct=True)
plot_panel(axes[2], topic_rate(is_withdrawn), "Withdrawn", "Share of topic", pct=True)
finish(fig, "Outcomes by topic", "Share = of that topic's proposals that year.\n" + SYN_NOTE,
       "outcomes_by_topic.png")

# 3. No-action funnel -- the SLB 14L story
sought = lambda s: s["mynoaction_sought"] == 1
granted = lambda s: s["mynoaction_granted"] == 1
granted_and_sought = lambda s: (s["mynoaction_granted"] == 1) & (s["mynoaction_sought"] == 1)
effective_excl = lambda s: (s["mynoaction_granted"] == 1) & (s["myproposal_result"] != "1voted")
fig, axes = plt.subplots(1, 3, figsize=(15.5, 5.2))
plot_panel(axes[0], topic_rate(sought), "No-action sought", "Share of topic", pct=True)
plot_panel(axes[1], topic_rate(granted_and_sought, sought), "Grant rate (given sought)", "Share of sought", pct=True)
plot_panel(axes[2], topic_rate(effective_excl), "Effective exclusion", "Share of topic", pct=True)
finish(fig, "No-action funnel by topic",
       "Sought & effective-exclusion = share of topic; grant rate = share of sought; "
       "effective exclusion = granted and not voted.\n" + SYN_NOTE,
       "noaction_funnel.png")

# 4. Support -- mean & median Votes For on voted rows, plus majority-pass rate
fig, axes = plt.subplots(1, 3, figsize=(15.5, 5.2))
plot_panel(axes[0], topic_support("mean"), "Mean Votes For", "Votes For (%)", pct100=True)
plot_panel(axes[1], topic_support("median"), "Median Votes For", "Votes For (%)", pct100=True)
plot_panel(axes[2], topic_rate(is_passed, is_voted), f"Majority-pass rate (> {majority_threshold}%)",
           "Share of voted", pct=True)
finish(fig, "Support by topic",
       f"Voted rows only; majority support = Votes For > {majority_threshold}%; diverges from "
       f"Proxy Proposal Result=='Pass' on {divergence:.1f}% of voted rows.\n{SYN_NOTE}",
       "support_by_topic.png")


README = """# Topic Trends Over Time (Line B)

## 1. What these charts show
Governance vs Environmental-and-Social shareholder proposals across the full year
range, plotted as two overlaid series per metric. The question: did the two topic
families move on different trajectories, and is there a structural break around
2021-2022 -- when **SEC Staff Legal Bulletin 14L** (Nov 2021) narrowed companies'
ability to exclude E&S proposals, and the **2020 Rule 14a-8 amendments** took
effect (2022 season)? Both regime changes are marked as vertical reference lines on
every panel.

Files (number = filename prefix):
1. `1_volume_by_topic.png` -- proposals per year; each topic's share of all proposals
2. `2_outcomes_by_topic.png` -- share voted / omitted / withdrawn per year, by topic
3. `3_noaction_funnel.png` -- no-action sought rate; grant rate (given sought); effective exclusion
4. `4_support_by_topic.png` -- mean & median Votes For (voted rows); majority-pass rate

Synthetic data -> every series is flat and the two topics track in parallel by
construction. Plumbing only; real signal awaits the reconciled ISS+FactSet data.

## 2. Variables and how they're used
| Variable | Role |
|---|---|
| `Proxy Category` | **Topic.** Governance = `Corporate Governance`; E&S = `Social/Environmental Issues`. Other categories count toward the "share of all proposals" denominator only. |
| `year` | **X-axis**, full range in the data. |
| `myproposal_result` | **Outcomes.** Voted = `1voted`; Omitted = `2omitted`; Withdrawn = `3withdrawn`. |
| `mynoaction_sought` / `mynoaction_granted` | **No-action funnel.** Sought rate (of topic); grant rate (granted among sought); effective exclusion (granted **and** not voted, of topic). |
| `Votes For As % Votes Cast` | **Support.** Mean & median on voted rows; majority support = `> 50`. |
| `Proxy Proposal Result` | Divergence check vs the `> 50` pass definition (footnoted, not plotted). |
| `Index Mtg SP50` | Universe toggle (`sp500_only`); default off -> whole population. |
"""

with open(os.path.join(outdir, "README.md"), "w") as f:
    f.write(README)

print(f"Universe: {universe_label} ({len(universe)} proposals, {years[0]}-{years[-1]})")
print(f"Pass-definition divergence on voted rows: {divergence:.1f}%")
print(f"Wrote {len(written)} PNGs + README.md to {outdir}:")
for name in written:
    print(f"  {name}")
