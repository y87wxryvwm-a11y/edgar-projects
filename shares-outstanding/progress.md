# progress — shares-outstanding

## Status: 1,000-filing run done & independently audited; extractor at 861/1000 (86.1%) vs the golden set, 0 regressions — fix-loop in progress

The goal: run the pipeline on a random stratified sample of **1,000** annual
filings (500 10-K / 400 20-F / 100 40-F, filed 2025) and iterate the extractor
until every filing's `(number, share class, as-of date)` is correct, **verified by
an independent adversarial audit**. We are not yet at 100%; the path there is the
golden-check loop below and the remaining work is enumerated.

---

## What is DONE

### 1. Fast, local iteration (caching)
Three on-disk caches under `.cache/` (git-ignored) make the fix loop cheap:
- `docs/` — primary documents by accession (no re-download)
- `xbrl/` — `dei` facts by CIK
- `clean/` — the **cleaned text** by accession (the BeautifulSoup reduction, the
  slowest step, which never changes when the *parser* changes)

With the clean cache warm, **re-extracting all 1,000 filings is ~2 s, no network** —
so a parser change → re-extract → score cycle is seconds, not minutes.

### 2. The 1,000-filing run
`1_…` sampled (seed 20260607), `2_…` extracted, `3_…` wrote a neutral evidence
packet per filing. Re-run any of these after editing their `# ---- EDIT THIS ----`
block (`SAMPLE_SIZE = 1000`).

### 3. Independent adversarial audit (two rounds) → a golden ground-truth table
- **Round 1 (blind):** 125 sub-agents each read the *neutral* evidence for ~8
  filings and decided the truth themselves, blind to the extractor's output.
  `5_reconcile_audit.py` compared (in code) → **820/1000 agreed, 180 disagreements.**
- **Round 2 (adjudication):** 45 sub-agents adjudicated every disagreement against
  the **full** cover (`dump_cover.py`), each producing a definitive ruling + a
  machine-readable `root_cause`. Notably, **12 disagreements were the round-1
  auditor's error, not the extractor's** — caught because round 2 reads the real
  cover, not the packet.
- **Golden table** `golden_2025_n1000.json` (built by `7_build_golden.py`) =
  round-1 agreements + round-2 definitive rulings = trusted truth for all 1,000.
  `8_check_golden.py` scores any re-extraction against it and writes the worklist
  `golden_failures_2025_n1000.json`.

### 4. Parser fix loop (driven by the golden check) — baseline 831/1000 → 861/1000 (0 regressions)
All fixes are in `shares_lib.py`, each verified against the golden set to avoid
regressions (regressions = a `round1_agree` row that newly fails; keep at 0):
- **Doc cap 6 MB → 50 MB + corrected streaming.** Large inline-XBRL bank 20-Fs
  (KB Financial, Barclays) carry a multi-MB XBRL header before the readable cover;
  the old 6 MB cap truncated them to empty. Also fixed a streaming bug that could
  return a half-streamed primary document.
- **European space-separated thousands** (`5 605 850 345` → Nokia's 5.6 B). Only
  clean 3-digit groupings are merged (a space after a decimal is left alone).
- **Decimal scale-words** (`45.0 million`).
- **Grand-total exclusion**: `… consisting of / comprising / being the sum of …`
  drops the redundant total, keeps the component classes; **plus** the
  `X shares, including A Class A … and B Class B` form (total dropped, classes kept).
- **Over-extraction skips**: issued-vs-outstanding (keep outstanding),
  `excluding / of which / form of` subsets, treasury, warrants/options,
  non-affiliate market-value figures, buyback/repurchase mentions.
- **Type**: trust / LP units classified `other`, not `common`.

---

## What is LEFT (and why) — the remaining golden-check failures

Run `python 8_check_golden.py` for the live list. Categories (≈ counts as of the
last full check; will shift as fixes land):

- **DATE (~15)** — 10-K covers where the recent practicable `As of <date>` should
  win over the fiscal year-end (iBio, Barfresh, Hudson Global), and a few 20-Fs
  with an inline cover date that should override the period fallback (Cellebrite).
  *Fix:* bias `_nearest_date` harder toward the date adjacent to the count, and
  don't let the 20-F period fallback overwrite an inline cover date.
- **TYPE (~24)** — residual `share_type` buckets: ADS filers labelled `ordinary`
  vs `depositary`, and unlabelled foreign covers that should default `ordinary`.
  *Fix:* refine `classify_share_type` and the unlabeled default per form.
- **EXTRA / MISS_NUMBER / MISS (long tail)** — the hard idiosyncratic cases:
  - **Multi-class decimal lists** with one trailing "outstanding" (HINES-style NAV
    REITs: Hines, Apollo Realty, EQT Exeter): early classes fall outside the
    ±230-char proximity window. *Fix:* a dedicated finder for
    `N [scale] shares of … Class X common stock` lists in the cover.
  - **Combined multi-registrant 10-Ks** (Exelon, CubeSmart): the cover lists each
    co-registrant's count; we want only the **primary** registrant's. *Fix:* keep
    the count whose registrant matches the filer (or the first listed / XBRL-matched).
  - A handful of one-off cover phrasings.

**Why not 100% yet:** the *systematic* patterns are largely handled; what remains
is a long tail of idiosyncratic cover wordings, each needing a targeted,
regression-checked rule. The golden check makes this safe and measurable.

---

## How to PICK UP tomorrow (the fast loop)

```
cd shares-outstanding
# 1. inspect the current failures (and confirm 0 regressions: golden_source==round1_agree)
python 8_check_golden.py

# 2. look at a specific cover (full, untruncated, exactly what the parser sees)
python dump_cover.py <accession>

# 3. edit a rule in shares_lib.py, then re-extract (clean cache warm => ~2 s, no network)
python 2_extract.py

# 4. re-score; repeat until PASS = 1000
python 8_check_golden.py
```

- The worklist is `DATA_DIR/golden_failures_2025_n1000.json` (per-filing: status,
  `ext` vs `truth`, flags).
- A failing row whose `golden_source == round1_agree` is a **regression** you just
  introduced — fix it before moving on.
- When `PASS = 1000`: run a **final independent confirmation** — re-run
  `audit_workflows/audit_round1.workflow.js` on the filings whose extraction
  changed, plus a fresh random sample, to confirm the golden table itself holds.

---

## Files added this run

Runner/analysis scripts (committed): `4_build_audit_batches.py`,
`5_reconcile_audit.py`, `6_build_round2_batches.py`, `7_build_golden.py`,
`8_check_golden.py`, `dump_cover.py`; the agent drivers in `audit_workflows/`.
Engine changes in `shares_lib.py` (caches + parser fixes above).

Generated artifacts live in `DATA_DIR` (git-ignored): `golden_2025_n1000.json`,
`reconciliation_*`, `disagreements_*`, `golden_failures_*`,
`audit/round1/` & `audit/round2/` (batches + agent results),
`validation_input_*`, `extraction_results_*`, `evidence/`.
