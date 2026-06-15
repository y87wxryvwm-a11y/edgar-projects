"""Build a synthetic proxy-proposal dataset whose distributional summary
reproduces every cell in proxy/dataset_shape.csv.

Output: synthetic_proxy.csv in the directory below (8,461 rows x 13 columns).
"""

import os

import numpy as np
import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
seed = 42
output_filename = "synthetic_proxy.csv"
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
output_path = os.path.join(directory, output_filename)

N = 8461
rng = np.random.default_rng(seed)


def build_symbol():
    return [f"PROP{i:05d}" for i in range(1, N + 1)]


def build_cik():
    n_unique = 1280
    ciks = rng.choice(np.arange(1000, 10_000_000), size=n_unique, replace=False)
    counts = np.ones(n_unique, dtype=int)
    extra = N - n_unique
    weights = 1.0 / np.arange(1, n_unique + 1)
    weights /= weights.sum()
    counts += rng.multinomial(extra, weights)
    values = np.repeat(ciks, counts)
    rng.shuffle(values)
    return values.astype(np.int64)


def build_year():
    # Hand-tuned to hit mean=2020, median=2020, all 11 years present,
    # and pinned counts for 2022-2025 from the spec.
    counts = {
        2015: 759, 2016: 800, 2017: 811, 2018: 821, 2019: 866,
        2020: 600, 2021: 600,
        2022: 742, 2023: 781, 2024: 896, 2025: 785,
    }
    assert sum(counts.values()) == N
    assert sum(y * c for y, c in counts.items()) == 2020 * N
    # Median lock: cumulative through 2019 = 4057 (<=4230), through 2020 = 4657 (>=4231)
    values = np.repeat(list(counts.keys()), list(counts.values()))
    rng.shuffle(values)
    return values.astype(np.int64)


def build_factset_industry():
    pinned = {"investment managers": 98, "investment trusts/mutual funds": 134}
    n_other = 123 - len(pinned)
    other_names = [f"industry_{i:03d}" for i in range(1, n_other + 1)]
    other_total = N - sum(pinned.values())
    weights = 1.0 / np.arange(1, n_other + 1)
    weights /= weights.sum()
    other_counts = np.ones(n_other, dtype=int)
    other_counts += rng.multinomial(other_total - n_other, weights)
    all_names = list(pinned.keys()) + other_names
    all_counts = list(pinned.values()) + other_counts.tolist()
    values = np.repeat(all_names, all_counts)
    rng.shuffle(values)
    return values


def build_binary(ones_count):
    zeros = N - ones_count
    values = np.concatenate([
        np.ones(ones_count, dtype=np.int64),
        np.zeros(zeros, dtype=np.int64),
    ])
    rng.shuffle(values)
    return values


def build_proponent_type():
    pinned_count = 3778
    n_missing = 670
    other_names = [
        "PUBLIC PENSION", "RELIGIOUS", "UNION", "MUTUAL FUND", "FOUNDATION",
        "INSTITUTIONAL", "STATE PENSION", "HEDGE FUND", "NON-PROFIT", "ASSET MANAGER",
    ]
    other_total = N - n_missing - pinned_count
    weights = 1.0 / np.arange(1, len(other_names) + 1)
    weights /= weights.sum()
    other_counts = np.ones(len(other_names), dtype=int)
    other_counts += rng.multinomial(other_total - len(other_names), weights)

    parts = ["INDIVIDUAL"] * pinned_count
    for name, c in zip(other_names, other_counts):
        parts.extend([name] * int(c))
    parts.extend([np.nan] * n_missing)
    values = np.array(parts, dtype=object)
    rng.shuffle(values)
    return values


def build_proxy_category():
    pinned = {"Corporate Governance": 4356, "Social/Environmental Issues": 3813}
    other_names = ["Compensation", "Capital Structure", "Anti-Takeover", "Audit", "M&A"]
    other_total = N - sum(pinned.values())
    n_other = len(other_names)
    base = other_total // n_other
    counts = [base] * n_other
    for i in range(other_total - sum(counts)):
        counts[i] += 1
    all_names = list(pinned.keys()) + other_names
    all_counts = list(pinned.values()) + counts
    values = np.repeat(all_names, all_counts)
    rng.shuffle(values)
    return values


def build_myproposal_result():
    counts = {"1voted": 5907, "2omitted": 1442, "3withdrawn": 1112}
    values = np.repeat(list(counts.keys()), list(counts.values()))
    rng.shuffle(values)
    return values


def build_proxy_proposal_result():
    counts = {"Pass": 785, "Fail": 6500, "Omitted": 600, "Withdrawn": 576}
    assert sum(counts.values()) == N
    values = np.repeat(list(counts.keys()), list(counts.values()))
    rng.shuffle(values)
    return values


def build_votes():
    counts = np.ones(101, dtype=int)
    # Lock median at 14 (position 4231 of sorted 8461-length column).
    counts[14] += 4216

    # Spread the remaining 4144 rows across buckets 15..100 with a right-tailed
    # weighting, then fine-tune via single-row adjacent shifts to hit sum=174,973.
    v = np.arange(15, 101)
    weights = np.exp(-(v - 15) / 15.0)
    weights /= weights.sum()
    counts[15:101] += rng.multinomial(4144, weights)

    target_sum = 174_973
    diff = target_sum - int((counts * np.arange(101)).sum())
    while diff != 0:
        if diff > 0:
            for vv in range(15, 100):
                if counts[vv] > 1:
                    counts[vv] -= 1
                    counts[vv + 1] += 1
                    diff -= 1
                    break
        else:
            for vv in range(100, 15, -1):
                if counts[vv] > 1:
                    counts[vv] -= 1
                    counts[vv - 1] += 1
                    diff += 1
                    break

    assert counts.sum() == N
    assert (counts > 0).all()
    assert int((counts * np.arange(101)).sum()) == target_sum
    values = np.repeat(np.arange(101), counts)
    rng.shuffle(values)
    return values.astype(np.int64)


def build_market_cap():
    """Market Cap ($ mil), pinned exactly to dataset_shape.csv:
    n_missing=127, n_unique=4124 (non-missing), min=0, max=3_765_000,
    mean=162_500, median=41_730. Drawn independently of every other column (zero
    cross-variable structure).

    Construction: a deterministic multiset that pins all six moments. The two
    central order-statistics are forced to MED (median), the extremes are LO/HI,
    distinctness is fixed at 4124 by the chosen support, and the exact sum (= mean
    x count) is landed with a coarse count knob on K plus a single solved-for
    'balancer' value B. NaNs are appended last so the column is float64.
    """
    n_missing = 127
    M = N - n_missing                 # 8334 non-missing (even)
    UNIQ = 4124
    LO, MED, HI = 0, 41_730, 3_765_000
    target_sum = 162_500 * M          # exact integer mean -> exact sum

    n_below = 1500                                   # distinct values 0..1499 (< MED)
    below_vals = np.arange(0, n_below, dtype=np.int64)
    K = 3_000_000                                    # coarse sum knob (distinct, < HI)
    n_above = UNIQ - 1 - n_below                     # distinct > MED (incl HI, K, balancer B)
    n_above_filler = n_above - 3
    above_filler = MED + 1 + np.arange(n_above_filler, dtype=np.int64)
    assert above_filler[-1] < K < HI

    below_total = M // 2 - 1                         # 4166 -> central two are MED
    below_counts = np.ones(n_below, dtype=np.int64)
    below_counts[0] += below_total - n_below         # pad extra onto 0 (adds nothing to sum)
    below_sum = int((below_vals * below_counts).sum())

    med_count = 2
    above_total = M - below_total - med_count        # 4166
    above_extra = above_total - n_above
    vmin = MED + 1
    filler_sum = int(above_filler.sum())

    # sum-excluding-B as a function of cK (coarse counts on K); remaining extra
    # counts land on vmin. Then B = target_sum - sum_excl_B closes the sum exactly.
    base_excl_B = below_sum + MED * med_count + HI + K + filler_sum
    lo_bound, hi_bound = target_sum - HI, target_sum - MED
    chosen = None
    for cK in range(above_extra + 1):
        s = base_excl_B + K * cK + (above_extra - cK) * vmin
        B = target_sum - s
        if lo_bound < s < hi_bound and B != K and B != HI and B > MED + n_above_filler:
            chosen = (cK, B)
            break
    assert chosen is not None, "no feasible cK for Market Cap sum"
    cK, B = chosen

    above_vals = np.concatenate([above_filler, [HI, K, B]]).astype(np.int64)
    above_counts = np.ones(n_above, dtype=np.int64)
    above_counts[n_above_filler + 1] += cK           # K
    above_counts[0] += (above_extra - cK)            # vmin == above_filler[0]
    assert above_counts.sum() == above_total and (above_counts > 0).all()

    vals = np.concatenate([
        np.repeat(below_vals, below_counts),
        np.repeat([MED], [med_count]),
        np.repeat(above_vals, above_counts),
    ]).astype(np.int64)

    s = np.sort(vals)
    assert vals.shape[0] == M
    assert s[0] == LO and s[-1] == HI
    assert (s[M // 2 - 1] + s[M // 2]) / 2 == MED
    assert len(np.unique(vals)) == UNIQ
    assert int(vals.sum()) == target_sum

    arr = np.concatenate([vals.astype(float), np.full(n_missing, np.nan)])
    rng.shuffle(arr)
    return arr


columns = {
    "Symbol": build_symbol(),
    "cik": build_cik(),
    "year": build_year(),
    "Factset Industry Desc": build_factset_industry(),
    "Index Mtg SP50": build_binary(6331),
    "Proponent Type Code": build_proponent_type(),
    "Proxy Category": build_proxy_category(),
    "mynoaction_granted": build_binary(1450),
    "mynoaction_sought": build_binary(2927),
    "myproposal_result": build_myproposal_result(),
    "Proxy Proposal Result": build_proxy_proposal_result(),
    "Votes For As % Votes Cast": build_votes(),
    "Market Cap ($ mil)": build_market_cap(),
}

df = pd.DataFrame(columns)
df.to_csv(output_path, index=False)
print(f"Wrote {len(df)} rows to {output_path}")
