"""Line B -- Environmental & Social vs Governance, over time.

Four themed figures, full history (whatever year range the data spans), each a
small panel of year-axis line charts with Governance and Environmental-and-Social
plotted as two overlaid series:

  1_volume_by_topic.png     proposal count per year; each topic's share of all proposals
  2_outcomes_by_topic.png   share voted / omitted / withdrawn per year, by topic
  3_noaction_funnel.png     no-action sought rate; grant rate (given sought); effective exclusion
  4_support_by_topic.png    mean & median Votes For (voted rows); majority-pass rate

Definitions (topic, voted, omitted, withdrawn, majority support) are copied
verbatim from build_table1.py / build_table2.py / build_company_size.py so the
cuts stay consistent across every deliverable. See the generated README.md for the
exact variable and formula behind every panel.
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
def plot_panel(ax, series_by_topic, title, ylabel, pct=False, pct100=False):
    for label, _ in TOPIC_PAIR:
        s = series_by_topic[label]
        ax.plot(years, s.reindex(years).values, marker="o", ms=3.5, lw=1.6,
                color=TOPIC_COLOR[label], label=label, zorder=3)
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


# one shared legend (the two topic series) for every figure
LEGEND_HANDLES = [
    Line2D([0], [0], color=TOPIC_COLOR[l], marker="o", ms=4, lw=1.6, label=l) for l, _ in TOPIC_PAIR
]

written = []


def finish(fig, suptitle, foot, name):
    name = f"{len(written) + 1}_{name}"  # number the output files 1..4
    fig.suptitle(f"{suptitle} -- {universe_label}, {years[0]}-{years[-1]}", fontsize=12)
    fig.legend(handles=LEGEND_HANDLES, loc="lower center", ncol=2, fontsize=8,
               frameon=False, bbox_to_anchor=(0.5, 0.075))
    fig.text(0.01, 0.01, foot, fontsize=7, va="bottom")  # foot may be two lines
    fig.tight_layout(rect=[0, 0.15, 1, 0.95])
    fig.savefig(os.path.join(outdir, name), dpi=120)
    plt.close(fig)
    written.append(name)


# 1. Volume -- count, then share of all proposals
fig, axes = plt.subplots(1, 2, figsize=(13, 5.2))
plot_panel(axes[0], topic_count(), "Proposals per year", "Proposals")
plot_panel(axes[1], topic_share(), "Share of all proposals", "Share of all proposals", pct=True)
finish(fig, "Volume by topic", "", "volume_by_topic.png")

# 2. Outcomes -- voted / omitted / withdrawn share (of that topic's proposals that year)
fig, axes = plt.subplots(1, 3, figsize=(15.5, 5.2))
plot_panel(axes[0], topic_rate(is_voted), "Voted on", "Share of topic", pct=True)
plot_panel(axes[1], topic_rate(is_omitted), "Omitted", "Share of topic", pct=True)
plot_panel(axes[2], topic_rate(is_withdrawn), "Withdrawn", "Share of topic", pct=True)
finish(fig, "Outcomes by topic", "Share = of that topic's proposals that year.",
       "outcomes_by_topic.png")

# 3. No-action funnel
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
       "effective exclusion = granted and not voted.",
       "noaction_funnel.png")

# 4. Support -- mean & median Votes For on voted rows, plus majority-pass rate
fig, axes = plt.subplots(1, 3, figsize=(15.5, 5.2))
plot_panel(axes[0], topic_support("mean"), "Mean Votes For", "Votes For (%)", pct100=True)
plot_panel(axes[1], topic_support("median"), "Median Votes For", "Votes For (%)", pct100=True)
plot_panel(axes[2], topic_rate(is_passed, is_voted), f"Majority-pass rate (> {majority_threshold}%)",
           "Share of voted", pct=True)
finish(fig, "Support by topic",
       f"Voted rows only; majority support = Votes For > {majority_threshold}%; diverges from "
       f"Proxy Proposal Result=='Pass' on {divergence:.1f}% of voted rows.",
       "support_by_topic.png")


README = """# Topic Trends Over Time (Line B)

Governance vs Environmental-and-Social shareholder proposals over time. Each figure
is a year-axis line chart with one line per topic; four figures, one metric family
each.

## Conventions that apply to every figure

- **Topic (the two lines).** Each line is a subset of rows by `Proxy Category`:
  - **Governance** = rows where `Proxy Category == "Corporate Governance"`
  - **Environmental and Social** = rows where `Proxy Category == "Social/Environmental Issues"`
  Other categories (Compensation, etc.) are dropped, EXCEPT in figure 1's right panel
  whose denominator is "all proposals" and counts every category.
- **X-axis.** `year` -- every year present in the data.
- **Universe.** All rows. Set `sp500_only = True` to restrict to `Index Mtg SP50 == 1`.
- Every y-value below is computed **per topic, per year**. Rate panels start at 0 and
  show a gap in any topic-year whose denominator is 0.

## 1. `1_volume_by_topic.png` -- Volume
| Panel | y-value (per topic, per year) | Columns used |
|---|---|---|
| Proposals per year | count of the topic's rows that year | `Proxy Category`, `year` |
| Share of all proposals | (topic rows that year) / (ALL rows that year, every category) | `Proxy Category`, `year` |

## 2. `2_outcomes_by_topic.png` -- Outcomes
Denominator of all three panels = the topic's row count that year. Numerator =
topic rows that year where:
| Panel | Numerator condition | Columns used |
|---|---|---|
| Voted on | `myproposal_result == "1voted"` | `Proxy Category`, `year`, `myproposal_result` |
| Omitted | `myproposal_result == "2omitted"` | `Proxy Category`, `year`, `myproposal_result` |
| Withdrawn | `myproposal_result == "3withdrawn"` | `Proxy Category`, `year`, `myproposal_result` |

## 3. `3_noaction_funnel.png` -- No-action funnel
| Panel | y-value = numerator / denominator (per topic, per year) | Columns used |
|---|---|---|
| No-action sought | (rows with `mynoaction_sought == 1`) / (all topic rows that year) | `Proxy Category`, `year`, `mynoaction_sought` |
| Grant rate (given sought) | (rows with `mynoaction_granted == 1` AND `mynoaction_sought == 1`) / (rows with `mynoaction_sought == 1`) | `Proxy Category`, `year`, `mynoaction_sought`, `mynoaction_granted` |
| Effective exclusion | (rows with `mynoaction_granted == 1` AND `myproposal_result != "1voted"`) / (all topic rows that year) | `Proxy Category`, `year`, `mynoaction_granted`, `myproposal_result` |

## 4. `4_support_by_topic.png` -- Support
Only voted rows (`myproposal_result == "1voted"`) enter this figure. The `> 50`
majority-support cutoff is the `majority_threshold` knob (default 50).
| Panel | y-value (per topic, per year, over voted rows) | Columns used |
|---|---|---|
| Mean Votes For | mean of `Votes For As % Votes Cast` | `Proxy Category`, `year`, `myproposal_result`, `Votes For As % Votes Cast` |
| Median Votes For | median of `Votes For As % Votes Cast` | `Proxy Category`, `year`, `myproposal_result`, `Votes For As % Votes Cast` |
| Majority-pass rate (> 50%) | (voted rows with `Votes For As % Votes Cast > 50`) / (all voted rows) | `Proxy Category`, `year`, `myproposal_result`, `Votes For As % Votes Cast` |

The support figure's footnote reports how often the `> 50%` pass flag disagrees with
`Proxy Proposal Result == "Pass"` -- a cross-check only; `Proxy Proposal Result` is
not plotted.
"""

with open(os.path.join(outdir, "README.md"), "w") as f:
    f.write(README)

print(f"Universe: {universe_label} ({len(universe)} proposals, {years[0]}-{years[-1]})")
print(f"Pass-definition divergence on voted rows: {divergence:.1f}%")
print(f"Wrote {len(written)} PNGs + README.md to {outdir}:")
for name in written:
    print(f"  {name}")
