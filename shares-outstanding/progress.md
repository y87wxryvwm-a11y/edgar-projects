# progress — shares-outstanding

## Status: CYCLE 2 CLOSED — full relevant universe (10-K + 20-F + 40-F) at 100% (898/898), label standard enforced, confirmed by blind audit

Cycle 1 closed the 10-K universe at 410/410. Cycle 2 (2026-06-10) added the
foreign annual forms to the relevant universe, added the class-label standard,
fixed the final CSV format, and re-ran the whole verification stack to the
same bar.

### The relevant universe now

| category | n | how decided |
|---|---|---|
| RELEVANT | 898 (410 10-K + 488 20-F/40-F) | two blind agent reads per filing + adjudication of disagreements |
| NOT_RELEVANT_ABS | 58 | securitization vehicles (CMBS/auto trusts, funding LLCs, repackaging trusts) |
| NOT_RELEVANT_UNITS | 36 | non-corporate registrants with units (MLPs, fund LPs/LLCs, crypto/commodity ETF trusts) |
| NOT_RELEVANT_DEBT_ONLY | 8 | no public equity (parent-held subs, FHLBank member stock) |

Every relevant filing's `(number, class designator, share type, as-of date)`
matches the adversarially-audited golden table: **898/898 PASS, 0
regressions** (overall extractor 979/1000; the 21 remaining failures are all
in not-relevant filings and out of scope). Final dataset:
`DATA_DIR/final_2025_n1000.csv` — one row per share class for relevant
filings; `share_class` is the bare designator ("AX", "JX") or the lowercased
label when the class has none; every row carries a `filing_url` to the EDGAR
index.

## Cycle 2 — what was done

1. **Foreign relevance** — the NOT_RELEVANT_FORM rule was removed; all 500
   20-F/40-F covers were dumped and classified by two blind haiku passes +
   sonnet adjudication (`9_…`/`10_…`, batches 050–099) → 488 joined the
   relevant universe; ABS/UNITS/DEBT_ONLY assignments cross-checked clean.
2. **Label standard** — `8_check_golden.py` now compares the Class/Series
   designator per paired number (`CHECK_LABEL` → FAIL_LABEL;
   `class_designator()`: "Class AX common stock" → "AX"). Root cause of the
   Hines miss: golden `round1_agree` labels were fossils copied from the old
   extractor; a label-adjudication round (sonnet, full cover; 3-voter panels
   for two single-adjudicator errors) corrected golden with
   `+label_adjudication`/`+label_panel` provenance (`12_apply_label_rulings.py`).
3. **Fix loop** — 66 foreign golden failures diagnosed (one sonnet agent each,
   `diagnose_failures.workflow.js`) and fixed in 3 batches + stragglers.
   Highlights: 20-F/40-F anchor tolerances (issued-phrasing, "each of"
   omitted, despaced artifacts); two-pass kept-number scan with no magnitude
   floor + citation/par-value guards; layout-aware label binding
   (and-connective adjacency, sentence-bounded pre-attachment, designator
   override only for far labels); grand-total/subset/respectively/warrant/
   treasury guard refinements; sentinel-only "date of this report" binding in
   BOTH extraction paths with the Primega deferral restricted to outside the
   anchor listing; decimal-safe clause cuts; comma-glued dates
   ("March 28,2025"); comma-list voting class names ("Subordinate, Restricted
   and Limited Voting Shares"); 12(b) designator-definition type lookup
   (Televisa Series "D" → preferred); day-first dates ("20 March, 2025").
4. **Golden panels** — two end-state disputes ruled by unanimous 3-voter
   sonnet panels (`DATA_DIR/audit/panels/golden_panel_rulings_cycle2.json`):
   Hub Cyber harmonized to the period-close count (3,553,818 @ 2024-12-31,
   consistent with the WEBUY/Phoenix/Scage convention); Glass House Brands
   dual-class voting shares typed common (consistent with Canada Goose et al.).
5. **Final confirmation** — fresh blind audit (13 haiku batches over neutral
   evidence packets) of the 66 changed extractions + 60 random relevant passes
   (40 foreign / 20 10-K): 117/126 direct agreement; all 9 disagreements
   sonnet-adjudicated — 8 for the shipped extraction (auditor decoy errors:
   balance-sheet thousands counts, rounded fractional shares, CPO/unit/
   investment-share taxonomy, dual-date convention, one truncated evidence
   packet), 1 both-wrong (JFB: number right, date missing → comma-glued-date
   fix + the golden date check tightened so an empty extracted date no longer
   passes a dated golden row). Golden table unchanged. Artifacts in
   `DATA_DIR/audit/confirm2/`.
6. **CSV review gate + label-quality pass** — an independent reviewer of the
   rendered CSV found fallback-label junk and, critically, duplicate
   same-designator rows: the signature of label fossils shared by BOTH the
   extractor and golden (`round1_agree` labels copied from the old extractor —
   no disagreement, so never adjudicated; e.g. Donegal's Class B row labeled
   "Class A", Trulieve's Multiple Voting Shares labeled "Shares"). Fixes: the
   golden label check now compares designator + voting-tier qualifier set
   (`label_sig` — subordinate/multiple/super/proportionate/non-voting/special/
   LT-n; limited/restricted only in voting names), which surfaced 9 fossil
   filings; extractor learned voting-tier class names, paired-respectively
   binding ("Class A … and Class B … was N1 and N2, respectively"), the
   registrant-phrased anchor, digit-led labels ("25p ordinary shares") and
   mid-word split repair for ordinary/common; the 9 goldens were re-ruled by
   sonnet label adjudication (`DATA_DIR/audit/labels2/`, Cresco's voting-tier
   types harmonized to the panel convention) and applied with provenance.
   Final CSV re-review: **PASS** (no fragments, no duplicates, designators
   verified against bank-series prospectuses). 898/898 holds under the
   stricter check.

### Conventions the dataset encodes (decided by adjudication/panel, applied corpus-wide)

- **Dual-date covers (20-F/40-F)**: the period-close count is the answer; a
  later practicable-date supplement is excluded. When the per-class breakdown
  exists ONLY "as of the date of this annual report", the class counts carry
  the filing date and a period-dated aggregate equal to their sum is dropped.
- **share_type**: common/ordinary = registrant common equity, any voting tier
  (incl. Canadian multiple/subordinate voting); preferred/preference =
  preferred; ADS/ADR = depositary; units, CPOs, unit bundles, investment
  shares, special/founder shares = other. A bare-designator cover label takes
  its type from the 12(b) registered-securities definition when one exists.
- **Fractional counts** ("5,635,788.8 shares") are truncated, not rounded.
- Balance-sheet (often thousands), authorized, treasury, weighted-average,
  float and holders-of-record numbers are never the answer.

## Cycle 1 (closed): 10-K universe at 100% (410/410)

Two blind agent reads per 10-K + adjudication built the golden table; a
46-fix loop took the extractor to 410/410; a 106-filing blind confirmation
audit (100/106 direct, 6 adjudicated for the extraction) closed it.
Artifacts in `DATA_DIR/audit/` (round1/round2/confirm).

## Agent-cost rule (hard)

All audit/classification fleets run on **haiku** (bulk reads) or **sonnet**
(adjudication/diagnosis) — set explicitly in every workflow driver. Never run
a fleet on the main-loop model tier.

## Pickup loop

```
cd shares-outstanding
python 8_check_golden.py        # relevant-only pass rate must stay 898/898
python dump_cover.py <accession>
# edit shares_lib.py, then:
python 2_extract.py             # ~2 s with warm clean cache
python 11_build_final.py        # rebuild the final CSV
```

Worklist: `DATA_DIR/golden_failures_2025_n1000.json` (each row carries its
relevance category — only RELEVANT rows matter; all 21 current failures are
not-relevant). A newly failing RELEVANT row is a regression you just
introduced.

## Files

Committed: `1_…`–`12_…` runner scripts, `shares_lib.py`, `dump_cover.py`,
`validate_helper.py`, agent drivers in `audit_workflows/` (audit round 1/2,
relevance round 1/2, failure diagnosis, label adjudication). Generated
artifacts live in `DATA_DIR` (git-ignored): `final_2025_n1000.csv`,
`relevance_2025_n1000.json`, `golden_2025_n1000.json`, `golden_failures_*`,
`audit/` (round1, round2, confirm, confirm2, labels, panels), `relevance/`,
`evidence/`, `extraction_results_*`, `validation_input_*`.
