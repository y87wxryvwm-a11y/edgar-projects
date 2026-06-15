"""Company-size effect charts (within the S&P 500).

For each market-cap bin, plots three grouped bars -- Received >= Voted >= Passed
proposal counts. Bins are market-cap quartiles computed on the company universe
over full history (one cap per company), so prolific mega-caps don't distort the
cut points. Per window (2025; 2022-2025) it writes four numbered figures:

  {n}_size_all_{window}.png          all proposals, quartiles (single panel)
  {n}_size_all_{window}_deciles.png  all proposals, deciles (single panel)
  {n}_size_gov_es_{window}.png       Governance | Environmental and Social (side by side)
  {n}_size_ind_inst_{window}.png     all proposals: Individual | Institution (side by side)

Definitions (topic, proponent, voted, majority support) are copied verbatim from
build_table1.py / build_table2.py so the cuts stay consistent with the tables.
"""

import os

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # render to file, no display needed (Spyder-safe)
import matplotlib.pyplot as plt

# ---- EDIT THIS --------------------------------------------------------------
data_filename = "synthetic_proxy.csv"
sp500_only = True        # True  = restrict to S&P 500 (Index Mtg SP50 == 1); the
                         #         size axis is then market cap WITHIN the S&P 500.
                         # False = whole universe (S&P 500 as one cut among others).
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
outdir = os.path.join(directory, "company_size_charts")  # charts + README live here
os.makedirs(outdir, exist_ok=True)
df = pd.read_csv(os.path.join(directory, data_filename))

MARKETCAP = "Market Cap ($ mil)"
VOTES = "Votes For As % Votes Cast"
QUARTILES, DECILES = 4, 10

# ---- definitions copied verbatim from build_table1.py / build_table2.py -----
# (kept character-identical so the cuts stay consistent with the tables)
TOPIC_PAIR = [
    ("Governance", lambda s: s["Proxy Category"] == "Corporate Governance"),
    ("Environmental and Social", lambda s: s["Proxy Category"] == "Social/Environmental Issues"),
]
PROPONENTS = [
    ("Individual", lambda s: s["Proponent Type Code"] == "INDIVIDUAL"),
    ("Institution", lambda s: s["Proponent Type Code"].notna() & (s["Proponent Type Code"] != "INDIVIDUAL")),
]


def is_voted(s):
    return s["myproposal_result"] == "1voted"


def is_passed(s):
    # "Passed" = majority support, only meaningful on voted rows. Non-voted rows
    # (omitted/withdrawn) carry a spurious 0 in the source data, so they are
    # excluded (same restriction as build_table2.py).
    return (s["myproposal_result"] == "1voted") & (s[VOTES] > majority_threshold)


WINDOWS = [
    ("2025", lambda y: y == 2025),
    ("2022_2025", lambda y: (y >= 2022) & (y <= 2025)),
]

# ---- universe ---------------------------------------------------------------
universe = df[df["Index Mtg SP50"] == 1].copy() if sp500_only else df.copy()
universe_label = "S&P 500" if sp500_only else "all companies"

# Pass-definition divergence (Votes For > t vs Proxy Proposal Result == "Pass"),
# printed to the console only -- not shown on the charts.
voted_all = universe.loc[is_voted(universe)]
agree = ((voted_all[VOTES] > majority_threshold) == (voted_all["Proxy Proposal Result"] == "Pass")).mean()
divergence = 100 * (1 - float(agree)) if len(voted_all) else 0.0


def make_bins(nbins):
    """Assign every proposal to a market-cap bin. Edges are equal-frequency over
    the company universe (one cap per company = median of its rows, full history),
    so mega-caps with many proposals are counted once. Returns the binned frame, the
    edges, and the count of proposals dropped for a missing cap."""
    comp_cap = universe.dropna(subset=[MARKETCAP]).groupby("cik")[MARKETCAP].median()
    bin_idx, edges = pd.qcut(comp_cap, q=nbins, labels=False, retbins=True, duplicates="drop")
    got = len(edges) - 1
    if got < nbins:
        # duplicate cap values collapsed some quantile edges (a point mass -- e.g.
        # many companies share a median cap). Use however many bins qcut formed.
        print(f"  note: requested {nbins} bins, formed {got} (duplicate cap edges).")
    assert got >= 2, f"qcut formed only {got} bin(s); cap column is near-constant."
    cik_bin = pd.Series(np.asarray(bin_idx), index=comp_cap.index)
    b = universe.copy()
    b["cap_bin"] = b["cik"].map(cik_bin)
    n_excluded = int(b["cap_bin"].isna().sum())
    b = b.dropna(subset=["cap_bin"]).copy()
    b["cap_bin"] = b["cap_bin"].astype(int)
    return b, edges, n_excluded


def range_labels(edges):
    # x-axis tick labels: the $B range each bin covers (axis carries the unit).
    return [f"{edges[i] / 1000:,.1f}-{edges[i + 1] / 1000:,.1f}" for i in range(len(edges) - 1)]


def footnote(n_cap, n_prop=None):
    # only missing-value notes, kept terse.
    parts = []
    if n_cap:
        parts.append(f"{n_cap} missing market cap")
    if n_prop:
        parts.append(f"{n_prop} missing proponent type")
    return ("Excluded: " + "; ".join(parts) + ".") if parts else ""


def bin_counts(sub, nbins):
    idx = range(nbins)
    rec = sub.groupby("cap_bin").size().reindex(idx, fill_value=0)
    vot = sub.loc[is_voted(sub)].groupby("cap_bin").size().reindex(idx, fill_value=0)
    pas = sub.loc[is_passed(sub)].groupby("cap_bin").size().reindex(idx, fill_value=0)
    return pd.DataFrame({"received": rec, "voted": vot, "passed": pas})


def plot_counts(ax, c, labels, title):
    # annotate each bar with its value so near-zero bars (e.g. a "Passed" bar in a
    # low-support bin) stay legible instead of looking like no data.
    x = np.arange(len(labels))
    w = 0.27
    fs = 6 if len(labels) <= QUARTILES else 5
    for off, col, lab in ((-w, "received", "Received"), (0, "voted", "Voted"), (w, "passed", "Passed")):
        container = ax.bar(x + off, c[col], w, label=lab)
        ax.bar_label(container, fmt="%d", fontsize=fs, padding=1)
    rot, lfs = (30, 8) if len(labels) <= QUARTILES else (40, 7)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=rot, ha="right", fontsize=lfs)
    ax.set_xlabel("Market cap ($B)", fontsize=8)
    ax.set_title(title, fontsize=10)
    ax.set_ylabel("Proposals")
    ax.margins(y=0.15)  # headroom for the value labels
    ax.legend(fontsize=8)


written = []


def save(fig, name):
    name = f"{len(written) + 1}_{name}"  # number the output files 1..8
    fig.savefig(os.path.join(outdir, name), dpi=120)
    plt.close(fig)
    written.append(name)


# quartile + decile binnings (computed once; decile feeds the fine all-proposals chart)
bq, eq, xq = make_bins(QUARTILES)
bd, ed, xd = make_bins(DECILES)
rlq, rld = range_labels(eq), range_labels(ed)

for wtag, wmask in WINDOWS:
    wlabel = wtag.replace("_", "-")
    winq = bq.loc[wmask(bq["year"])]
    wind = bd.loc[wmask(bd["year"])]

    # 1. all proposals -- quartiles (single panel)
    fig, ax = plt.subplots(figsize=(8, 6))
    plot_counts(ax, bin_counts(winq, len(rlq)), rlq,
                f"Company size: all proposals -- {universe_label}, {wlabel}\n"
                f"received / voted / passed by market-cap quartiles")
    fig.text(0.01, 0.01, footnote(xq), fontsize=7, va="bottom")
    fig.tight_layout(rect=[0, 0.05, 1, 1])
    save(fig, f"size_all_{wtag}.png")

    # 2. all proposals -- deciles (single panel)
    fig, ax = plt.subplots(figsize=(12, 6.5))
    plot_counts(ax, bin_counts(wind, len(rld)), rld,
                f"Company size: all proposals -- {universe_label}, {wlabel}\n"
                f"received / voted / passed by market-cap deciles")
    fig.text(0.01, 0.01, footnote(xd), fontsize=7, va="bottom")
    fig.tight_layout(rect=[0, 0.05, 1, 1])
    save(fig, f"size_all_{wtag}_deciles.png")

    # 3. by topic -- Governance | Environmental and Social (shared, identical y-axis)
    counts = [bin_counts(winq.loc[mask(winq)], len(rlq)) for _, mask in TOPIC_PAIR]
    top = max(c["received"].max() for c in counts) * 1.18  # one shared top + label headroom
    fig, axes = plt.subplots(1, 2, figsize=(13, 6), sharey=True, squeeze=False)
    for j, (label, _) in enumerate(TOPIC_PAIR):
        plot_counts(axes[0][j], counts[j], rlq, label)
    for j in (0, 1):
        axes[0][j].set_ylim(0, top)             # force an identical y-axis on both panels
        axes[0][j].tick_params(labelleft=True)  # show the same y ticks on both
    fig.suptitle(f"Company size: by topic -- {universe_label}, {wlabel}, market-cap quartiles", fontsize=12)
    fig.text(0.01, 0.01, footnote(xq), fontsize=7, va="bottom")
    fig.tight_layout(rect=[0, 0.04, 1, 0.95])
    save(fig, f"size_gov_es_{wtag}.png")

    # 4. all proposals by proponent -- Individual | Institution (shared, identical y-axis)
    n_missing_prop = int(winq["Proponent Type Code"].isna().sum())
    counts = [bin_counts(winq.loc[mask(winq)], len(rlq)) for _, mask in PROPONENTS]
    top = max(c["received"].max() for c in counts) * 1.18
    fig, axes = plt.subplots(1, 2, figsize=(13, 6), sharey=True, squeeze=False)
    for j, (label, _) in enumerate(PROPONENTS):
        plot_counts(axes[0][j], counts[j], rlq, label)
    for j in (0, 1):
        axes[0][j].set_ylim(0, top)
        axes[0][j].tick_params(labelleft=True)
    fig.suptitle(f"Company size: all proposals by proponent -- {universe_label}, {wlabel}, "
                 f"market-cap quartiles", fontsize=12)
    fig.text(0.01, 0.01, footnote(xq, n_missing_prop), fontsize=7, va="bottom")
    fig.tight_layout(rect=[0, 0.04, 1, 0.95])
    save(fig, f"size_ind_inst_{wtag}.png")


README = """# Company-Size Effect

## 1. What these charts show
Within the S&P 500, how shareholder-proposal **volume** and **success** scale with
company **market cap**. Proposals are split into market-cap bins (x-axis = each
bin's cap range in $B); every bin shows Received >= Voted >= Passed.

Files (number = filename prefix):
1. `1_size_all_2025.png` -- all proposals, quartiles, 2025
2. `2_size_all_2025_deciles.png` -- all proposals, deciles, 2025
3. `3_size_gov_es_2025.png` -- Governance | Environmental and Social, 2025
4. `4_size_ind_inst_2025.png` -- all proposals: Individual | Institution, 2025
5. `5_size_all_2022_2025.png` -- all proposals, quartiles, 2022-2025
6. `6_size_all_2022_2025_deciles.png` -- all proposals, deciles, 2022-2025
7. `7_size_gov_es_2022_2025.png` -- Governance | Environmental and Social, 2022-2025
8. `8_size_ind_inst_2022_2025.png` -- all proposals: Individual | Institution, 2022-2025

## 2. Variables and how they're used
| Variable | Role |
|---|---|
| `Market Cap ($ mil)` | **Bins (x-axis range).** One cap per company (median), full-history, equal-frequency quartiles/deciles -- mega-caps counted once so they don't skew the cut points. |
| `Index Mtg SP50` | **Universe.** Only S&P 500 (`== 1`). |
| `myproposal_result` | **Received** = all; **Voted** = `1voted`. |
| `Votes For As % Votes Cast` | **Passed** = voted **and** `> 50`. |
| `Proxy Category` | **Topic.** Governance = `Corporate Governance`; E&S = `Social/Environmental Issues`. |
| `Proponent Type Code` | **Proponent.** Individual = `INDIVIDUAL`; Institution = other non-null. Missing -> excluded (footnoted). |
| `year` | **Window.** `2025`, or `2022-2025`. |
| `cik` | Company key for the per-company cap and bins. |
"""

with open(os.path.join(outdir, "README.md"), "w") as f:
    f.write(README)

print(f"Universe: {universe_label} ({len(universe)} proposals)")
print(f"Pass-definition divergence on voted rows: {divergence:.1f}%")
print(f"Wrote {len(written)} PNGs + README.md to {outdir}:")
for name in written:
    print(f"  {name}")
