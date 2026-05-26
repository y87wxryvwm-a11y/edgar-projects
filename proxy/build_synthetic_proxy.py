"""Build a synthetic proxy-proposal dataset whose distributional summary
reproduces every cell in proxy/dataset_shape.csv.

Output: synthetic_proxy.csv in the directory below (8,461 rows x 12 columns).
"""

import os

import numpy as np
import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
directory = r"/Users/avilae/claude-code/projects/edgar-projects/proxy"
seed = 42
output_filename = "synthetic_proxy.csv"
# -----------------------------------------------------------------------------

directory = directory.replace("\\", "/")
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
}

df = pd.DataFrame(columns)
df.to_csv(output_path, index=False)
print(f"Wrote {len(df)} rows to {output_path}")
