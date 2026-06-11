# Progress log

## 2026-06-10 — project start

Scope locked with Evan (four explicit decisions):

1. Population = filings **filed in calendar 2025** (the four quarterly
   indexes), not fiscal-year 2025.
2. **Originals only** — form type must be exactly 10-K / 20-F / 40-F; no /A
   amendments.
3. Non-ABS filings with no share count (unit MLPs, fund LPs, debt-only
   issuers) are **kept with an explicit status flag**, never silently dropped.
4. The number comes from the **cover page only** — no balance-sheet fallback.

Validation ladder agreed: scrape-vs-XBRL agreement is presumed correct;
sub-agent reads close the gap by improving the extractor (general rules only)
until <1% unresolved; the remainder is hand-entered in committed code with
adversarial verification. Sub-agent output never enters the dataset directly —
the final CSV must be a deterministic function of (year, EDGAR, code).

Old `shares-outstanding` project is reference material (traps, scoping,
triangulation), not a code import — its 100% was partly sample-specific.

Built `census_lib.py` + `1_build_population.py` (population index with
header-SIC ABS filter). Pre-run gate (adversarial code review + live smoke
test) caught and fixed: latin-1 (not UTF-8) decoding of SGML headers, a 256 KB
streaming cap that could silently truncate large multi-registrant headers
(now 2 MB + a visible sentinel/flag), and unpinned CSV line endings
(now `lineterminator="\n"`).

**2025 population run (verified):** 7,650 filings — 6,431 10-K, 1,077 20-F,
142 40-F (matches the old project's index counts exactly). 825 ABS 10-Ks
excluded via header SIC 6189 → **6,825 in scope**. 199 multi-filer filings,
171 with no primary-filer SIC, 0 header parse failures, 0 truncations.
Post-run gate: 10/11 integrity checks passed first try (live URL spot-checks
included); the one failure — `header_missing_sic` described "all filers"
while `sic` is the primary filer's — fixed by redefining the flag as
`sic_missing` (primary filer), rebuilt offline from cache, full assertion
suite green.

Notes for the downstream relevance/status pass:
- 5 in-scope securitization-style vehicles carry non-6189 or missing SICs
  (Ameren Missouri Securitization Funding I, Apollo Asset Backed Credit Co,
  Duke Energy Progress SC Storm Funding, MBC Funding II, Virginia Power Fuel
  Securitization). Per the locked scope decision the automatic exclusion is
  SIC-6189-only, so they stay in the population and should resolve to
  debt-only / no-public-shares status flags, not silent exclusion.
- 109 rows have a 4-digit `sic` but blank `sic_desc` — EDGAR's own headers
  print no description for those codes (6221, 3949, 9995); faithful to source.
- The 171 `sic_missing` rows are mostly BDCs / private-credit funds and
  foreign issuers — legitimate in-scope filers, just untagged.

Next: script 2 — fetch + cache each in-scope filing's primary document (cover
region + its inline-XBRL dei facts), then the extractor.

## 2026-06-11 — DATASET COMPLETE

Final state: `shares_outstanding_2025.csv` — **8,410 share-class rows across
6,771 filings**; `filing_coverage_2025.csv` accounts for all 7,650 filings
(825 ABS-excluded, 6,185 XBRL-validated, 150 reads-confirmed, 436 override,
54 no-disclosure, **0 unresolved**). Row provenance: 7,910 XBRL_MATCH (94.1%),
85 XBRL_AGG_MATCH, 231 READS_SONNET_CONFIRMED, 184 OVERRIDE_VERIFIED.
Byte-identical across full rebuilds; assertion suite green (no future dates,
no ambiguous class labels, 1 genuinely undatable row).

The ladder as run: ~5 extractor iteration rounds against full-population
XBRL signal (86% → 91.2% filing-level validation, every rule general);
tier-2: 2×745 + 649 blind haiku reads; tier-3: 414 + 68 sonnet adjudications
of every conflict, 110 blind sonnet verifications; Opus (6 of the ≤10
budget): 3-agent panel on XBRL-contradicted rulings, 1 flag-resolution
agent, 1 full audit, 1 delta audit. Both audits spot-checked rows against
the filings; all counts matched; their label/date findings (market-value-
date grabs, equal-count class collapses, label swaps) were fixed via general
rules + audited overrides and re-verified. `overrides.py` carries every
non-extractor row with provenance (150 confirmed / 54 no-shares / 436
override entries).

For 2026: set `year = 2026` in scripts 1–7, run in order; the tier-2/3
process re-runs only for whatever the new year's XBRL can't settle.

## 2026-06-11 (later) — PLDT pairing defect: diagnosis and systemic fix

Evan caught a value-to-class rotation in PLDT's 20-F: all three counts right,
each attributed to the previous class's label. Two root causes, both fixed:

1. **Binding**: in number-first lists ("216,055,775 shares of Common Capital
   Stock"), the noun regex matched mid-phrase and the qualifier word in the
   gap defeated after-binding, so the previous line's noun won. The binds
   test now accepts gaps of "shares of" + the phrase's own qualifiers, rate
   tokens (6.50%), or same-line brand words (Petrobras); the class-led noun
   accepts Units; SPAC "N units, each unit..." binds the adjacent unit noun;
   "per share" bares are never class nouns; digit-hyphen-digit file numbers
   no longer condemn "Stock-2,563,034" (a round-4 rule had silently killed
   Molson); space-grouped numbers displace their own fragments (Nokia).
2. **Verification blindness**: tier-2 reconciliation had compared value SETS,
   so rotations passed "confirmation". Two pairing censuses now run as
   permanent assertions: (a) every row's share_type vs the filer's own
   dimension member (0 contradictions; types auto-corrected from members,
   TYPE_FROM_XBRL); (b) extraction (value→type) pairs vs both blind reads.
   The override-prune rule also no longer drops label-disambiguating entries.

Re-ruled after the rework: 23 residual filings + 14 label collisions (sonnet,
folded into overrides). Closing Opus pairing audit (7th of the ≤10 budget):
**30/30 multi-class filings PASS, 85/85 rows correctly paired** — including
PLDT, Liberty Media's tracking-stock matrix, KKR's nine classes, AEP's seven
registrants. Final: 8,412 rows / 6,771 filings / 0 unresolved /
byte-identical rebuilds.

All 6,825 in-scope primary documents fetched and cached (24 GB raw → 1.7 GB
gzipped, 0 failures). Extractor + inline-XBRL parser built and iterated
against the full-population XBRL signal — the validation ladder working as
designed. General rules added this round (each from a diagnosed cover
pattern, never a per-filing hack):

- decoys: non-affiliate market-value share bases (three phrasings, with
  sentence-boundary guards), treasury labels vs "(exclusive of treasury
  shares)" clauses, carve-outs ("including/excluding N shares"), authorized
  vs outstanding in one sentence, footnote-marker digits, "Class 1" digit
  capture, slash-date components, "N classes of stock";
- scale: "(in thousands)" before or after the number; space-grouped European
  digits (Nokia) on FPI covers; Decimal-exact iXBRL scaling ("66.4"
  scale=6);
- binding: class noun AFTER the number wins when connected by "shares of"
  (Ford's Class B), but a fresh label across a newline belongs to the next
  table row (Molson); long noun phrases (AllianceBernstein units) reachable;
  "Series N" suffix after the noun (Brookfield's 18 preference series);
- structure: cover capped at 15k chars (Intel/GE put financials early);
  same-class rows superseded by later-dated ones (the stale Q2 market-value
  basis count); weak-labeled totals dropped (Astronics); multi-registrant
  cover tables parsed per registrant with the count in the last numeric
  column (AEP 7/7 registrants validate, Hawaiian Electric 2/2);
- statuses: ZERO_FACT (tagged 0 = no-public-shares signal), PROSE_SUPERSET
  (all facts confirmed, extra prose rows for tier-2), ROWS_OK_FACTS_UNMATCHED
  (extra facts — exactly how a missed class hides; never auto-validated).

Snapshot mid-iteration (first 700 cached filings): 96.9% validated-or-
superset before the latest fix round. Script 3 (text + facts caches) and
script 5 (companyconcept API, the secondary XBRL source) running; full
population numbers next.
