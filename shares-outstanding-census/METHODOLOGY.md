# How the 2025 shares-outstanding census was built

A plain-English account of how every number in `shares_outstanding_2025.csv`
was produced and verified. The companion table `filing_coverage_2025.csv`
accounts for every filing in the population, so completeness can be checked
from the outside.

## The population

Every filing whose form type is exactly **10-K, 20-F, or 40-F** in SEC
EDGAR's four quarterly indexes for calendar 2025 — 7,650 filings. No
amendments (10-K/A etc.). Asset-backed-securities issuers are excluded
automatically by the SIC code in each filing's own SGML header (6189):
825 filings, listed in the coverage table, leaving **6,825 in scope**.

## The number

The cover-page disclosure of shares issued and outstanding, per share class —
the figure 10-K covers state "as of the latest practicable date" and
20-F/40-F covers state as of fiscal period close. Nothing is taken from
balance sheets or notes. Cross-class totals are reported only when the cover
prints no per-class components. Counts the cover itself excludes (treasury,
authorized, the market-value share basis, holder breakdowns) are never rows.
A cover that explicitly discloses zero for a class keeps that row; a cover
that discloses no count at all (debt-only subsidiaries, trusts) appears in
the coverage table as `NO_SHARE_COUNT_DISCLOSED`.

## How a number earns its place (the validation ladder)

Every row carries a `validation` tag stating exactly which rung it cleared:

1. **`XBRL_MATCH` (7,910 rows — 94.1%)** — the prose count equals the filer's own
   inline-XBRL `dei:EntityCommonStockSharesOutstanding` tag in the same
   document (or, where the document carries no parseable tags, SEC's
   companyconcept API record for the same accession). Two independent
   expressions of the number by the filer, in agreement.
2. **`XBRL_AGG_MATCH` (85 rows)** — per-class prose rows whose sum equals
   the filer's single tagged total.
3. **`READS_SONNET_CONFIRMED` (231 rows)** — no usable XBRL: the extraction
   was confirmed by independent machine readers (two blind passes, plus a
   stronger-model adjudication wherever any disagreement existed).
4. **`OVERRIDE_VERIFIED` (184 rows)** — the extractor's output was wrong or
   incomplete; the rows come from `overrides.py`, the committed, per-filing
   record of hand-verified readings. Every entry carries its provenance
   (which independent readings agreed, and the final adjudicator). The
   hardest 53 filings — including every case where a reading contradicted
   the filer's own XBRL — were ruled by a top-tier model panel.

Ambiguous class attributions were resolved with the filer's own XBRL
dimension members (`CLASS_FROM_XBRL` / `REGISTRANT_FROM_XBRL` in
`quality_flags`) — the filer's tagging, not our guess.

## The extractor is general, the overrides are explicit

Every parsing rule in `cover_extractor.py` describes a property of how
filings are written (documented in-line: decoy traps, scale phrases, glue
repairs, table shapes). No rule exists for one specific filing. Filings the
general rules cannot read correctly are not patched in the parser — they are
listed in `overrides.py` with their verified rows and provenance. That split
keeps the methodology honest: the code shows what generalizes; the override
table shows exactly where, and on whose authority, human-style judgment
entered.

## Reproducibility

The dataset is a deterministic function of (year, EDGAR archives, this
repository). Scripts 1–5 fetch and cache the inputs (filings are immutable
once filed); scripts 4 and 7 are pure computation; `overrides.py` is code.
No sub-agent is consulted at build time. Rebuilding on another machine
produces byte-identical CSVs (verified: repeated full rebuilds hash equal). Two independent
Opus-tier audits (full + delta) spot-checked 75+ rows against the filings
themselves; every count matched; their label/date findings were fixed and
re-verified.

## Known limits

- 1 row carries no as-of date (the cover prints none and no tagged instant
  exists); they are flagged.
- Counts are reported as filed, including filer-side oddities verified
  against the filings (one Cayman issuer genuinely discloses 73.1 trillion
  hyper-diluted shares; several wound-down classes genuinely disclose 0).
- The as-of date of a 10-K count is the filer's "latest practicable date,"
  which can precede filing by days or, for delinquent filers, much longer;
  20-F/40-F dates are fiscal period close by rule.
