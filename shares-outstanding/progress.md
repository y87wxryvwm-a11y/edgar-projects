# progress — shares-outstanding

## Status: CYCLE 2 IN FLIGHT — 20-F/40-F joining the relevant universe + class-label standard added (paused mid-cycle)

Cycle 1 (10-K universe) closed at 410/410 with a confirmed golden table (see
below). Cycle 2 (requested 2026-06-10) is underway and **paused**; resume here:

### Cycle 2 scope
1. **20-F / 40-F are now RELEVANT** (no more NOT_RELEVANT_FORM rule): they get
   the same two-pass agent classification (ABS/UNITS/DEBT_ONLY still apply) and
   the same 100%-pass standard.
2. **Class-label standard**: the golden check now also compares the
   Class/Series designator (`CHECK_LABEL`, FAIL_LABEL; `class_designator()` in
   shares_lib — "Class AX common stock" → "AX"). Found because Hines'
   multi-class rows shipped as bare "common stock".
3. Final CSV format: `share_class` = bare designator ("AX"/"JX"/"")
   and a `filing_url` column — NOT YET IMPLEMENTED in 11_build_final.py.

### Cycle 2 state at pause
- Foreign relevance round 1 running/landed in `DATA_DIR/relevance/round1/{A,B}`
  as `batch_050`–`batch_099` (A complete; if B is short of 100 files, relaunch
  relevance_round1.workflow.js for the missing ids only). Then:
  `10_reconcile_relevance.py` (the NOT_RELEVANT_FORM rule must be REMOVED from
  it first — it still stamps non-10-Ks by rule) → round-2 adjudication →
  rebuilt relevance json.
- Extractor label fixes landed (multi-char designators "AX"/"I-S"/"A-III",
  forward label grab tolerates "outstanding"/"o f"/possessive forms).
  Golden check: 893/1000 overall, 399/410 relevant on the cycle-1 universe.
- **KEY FINDING**: remaining FAIL_LABEL rows are largely GOLDEN errors —
  `7_build_golden.py` (lines ~88-89) copied `round1_agree` labels from the OLD
  extractor's class_label, so they fossilize old neighbor-grab bugs (e.g.
  golden labels Baidu's 524,340,320 "Class A" when the cover says Class B; the
  new extraction is right). Labels were never audited in round 1.
  → Next: a label-adjudication round (sonnet, full cover) over every
  FAIL_LABEL row; patch golden labels per ruling (document source), keep
  extractor fixes for genuine extraction misses (e.g. LexinFintech 20-F anchor
  path label).
- Then: diagnose + fix the ~75 relevant 20-F/40-F golden failures (same
  sonnet-diagnose → fix-loop → 0 regressions), update 11_build_final
  (designator + filing_url columns, include relevant foreign rows), re-run the
  blind confirmation audit on changed + fresh sample, rebuild final CSV.

### Cycle 1 (closed): relevant 10-K universe at 100% (410/410), relevance audited, final dataset built

The study's relevant universe is **corporate 10-K registrants with public
equity shares**. Everything else in the 1,000-filing sample (500 10-K /
400 20-F / 100 40-F, filed 2025) is marked **not relevant** with an audited
category:

| category | n | how decided |
|---|---|---|
| RELEVANT | 410 | two blind agent reads per 10-K + adjudication of disagreements |
| NOT_RELEVANT_FORM | 500 | 20-F / 40-F excluded by rule |
| NOT_RELEVANT_ABS | 58 | securitization vehicles (CMBS/auto trusts, funding LLCs, repackaging trusts) |
| NOT_RELEVANT_UNITS | 26 | non-corporate registrants with units (MLPs, fund LPs/LLCs, crypto/commodity ETF trusts) |
| NOT_RELEVANT_DEBT_ONLY | 6 | no public equity (parent-held subs, FHLBank member stock) |

Every **relevant** filing's `(number, share class, as-of date)` matches the
adversarially-audited golden table: **410/410 PASS, 0 regressions**
(overall extractor: 913/1000 — the 87 remaining failures are all in
not-relevant filings and out of scope). Final dataset:
`DATA_DIR/final_2025_n1000.csv` (one row per class for relevant filings,
one row with the category for not-relevant ones).

## How we got here (this run)

1. **Relevance classification** — `9_build_relevance_batches.py` dumped all
   500 10-K covers offline; two blind agent passes (A: direct, B:
   checklist-first) classified each; `10_reconcile_relevance.py` compared in
   code (482/500 agreed), 18 disagreements adjudicated by fresh agents →
   `relevance_2025_n1000.json`. Cross-check: all 60 TRUE_NEGATIVE 10-Ks in the
   golden table landed in not-relevant categories — zero conflicts between the
   two independent audits.
2. **Diagnosis fan-out** — one agent per relevant golden failure (46) returned
   the exact cover sentence, the failing parser mechanism, and the smallest
   generalizable rule.
3. **Fix loop** — fixes applied in 4 batches in `shares_lib.py`, each
   re-scored against the golden table (warm cache ≈ 2 s/cycle), regressions
   fixed before moving on. Highlights: market-value-date penalties and
   whitespace-tolerant "as of" bias in `_nearest_date`; sentence-bounded scan
   window extension (±230 → ≤±500); comma-split/mid-word artifact repair;
   float/denominator/issued-half/subset guards; latest-date and
   multi-registrant post-filters; "51 million" and "1 Class B share" finders.
4. **Final dataset** — `11_build_final.py` merges sample + relevance +
   extraction + golden status.
5. **Final confirmation** — fresh blind audit (separate agents, evidence
   packets) of the 46 changed extractions + 60 randomly re-sampled relevant
   passes: 100/106 direct agreement; the 6 disagreements were adjudicated
   against the full cover and **all resolved for the shipped extraction**
   (auditor misreads: footnote preferred counts, pre-split figures, float
   numbers; one cover-page-standard call on Bit Digital's note-only preferred).
   Golden table unchanged. Artifacts in `DATA_DIR/audit/confirm/`.

## Agent-cost rule (hard)

All audit/classification fleets run on **haiku** (bulk reads) or **sonnet**
(adjudication/diagnosis) — set explicitly in every workflow driver. Never run
a fleet on the main-loop model tier.

## Pickup loop

```
cd shares-outstanding
python 8_check_golden.py        # relevant-only pass rate must stay 410/410
python dump_cover.py <accession>
# edit shares_lib.py, then:
python 2_extract.py             # ~2 s with warm clean cache
python 11_build_final.py        # rebuild the final CSV
```

Worklist: `DATA_DIR/golden_failures_2025_n1000.json` (each row carries its
relevance category — only RELEVANT rows matter). A failing row with
`golden_source == round1_agree` is a regression you just introduced.

## Files

Committed: `1_…`–`11_…` runner scripts, `shares_lib.py`, `dump_cover.py`,
`validate_helper.py`, agent drivers in `audit_workflows/` (audit round 1/2,
relevance round 1/2, failure diagnosis). Generated artifacts live in
`DATA_DIR` (git-ignored): `final_2025_n1000.csv`, `relevance_2025_n1000.json`,
`golden_2025_n1000.json`, `golden_failures_*`, `audit/`, `relevance/`,
`evidence/`, `extraction_results_*`, `validation_input_*`.
