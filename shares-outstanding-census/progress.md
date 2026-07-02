# Progress log

## 2026-07-02 — combined registrant-level dataset (ROADMAP goal 1) built

The shares and float censuses merged into one published relational pair
(`15_build_combined.py`, `combined_overrides.py`), joined by (accession, cik).
Deterministic assembly over the finished censuses — no new extraction.

Outputs: `registrants_2025.csv` (7,911 rows, one per registrant-CIK per filing:
6,978 IN_SCOPE + 933 EXCLUDED_ABS; 7,650 primary + 261 co-filers, each carrying
its OWN CIK/name/SIC — a subsidiary never wears the parent's identity),
`share_classes_2025.csv` (8,414 rows + 26 per-class floats), and a nested JSONL.
Stata/R-safe (flat), primary key (accession, cik), byte-identical rebuilds.

Decisions (Evan): (A) `shares_total` summed only within one share type — mixed
common+preferred → empty + DISCLOSED_MIXED_TYPES; (B) the 170 NO_FLOAT_STATED
filings are cover-stated no-float (None/zero/N-A/no-public-market), NOT missed
extractions — the audit trail's "STATED_ON_COVER" framing was a mischaracterization
caught by reading the covers; they get `float_status FLOAT_STATED_NONE` + a
`float_status_detail` reason, classified from the cover field and confirmed by a
48-cover adversarial read (0 hidden positive floats); (C) per-class float joins
by class_designator, attached only when a designator maps to exactly one float
class (the JLL "Class M" vs "Class M-I" collision stays empty).

Names for 176 co-filer pairs (91 CIKs) with no row-level name resolved from the
cached SGML FILER headers (Entergy operating cos, AEP Transmission, Tanger LP …),
100% coverage. 68 co-filer names, 170-filing cover classification, and the read
verdicts are committed (`combined_overrides.py`) so a from-scratch rebuild
reproduces them.

Verification: exhaustive independent reconciliation over all 7,911 rows
(universe, share sums, float values/aggregation, names, mutual exclusivity,
status justification) — all green; byte-identical rebuild; 10-agent stratified
audit against the actual filings + a schema/publishability critic. The audit
found and drove fixes for: a fractional-shares drop (Union Carbide 935.51 →
DISCLOSED_FRACTIONAL, was silently lost), the empty `float_status` on value rows
(now DISCLOSED), the `FLOAT_STATED_NONE:reason` colon (split into
`float_status_detail`), plus `fiscal_year` and shares min/max as-of dates; and
one **upstream** float defect — JLL Income Property Trust (0001314152-25-000031),
a non-traded REIT whose NAV-based cover float is stated in thousands
($1,173,740 = $1.174B at 100.3M sh × $11.71 NAV) but was recorded ×1000 too
small. Corrected in `float_overrides.py` (all five classes ×1000, total ~$2.603B,
`SCALE_THOUSANDS_CORRECTED`), following the existing scale-fix precedent; a NAV
cross-check sweep confirmed it is the only such non-traded-fund case. Float
census and combined dataset rebuilt byte-identical; `14_check_float.py` green.

Queued follow-up: `TAGGED_ZERO_COVER_SILENT` (338 rows) can misname covers that
affirmatively state "no public market" (e.g. 0000038009-25-000007) — a
cover-language sweep would split genuine-silence from affirmative-no-market.

## 2026-06-12 — registrant identity, consolidated float, plausibility smoke test

Evan's eye-test review of the float dataset surfaced four issues; all fixed
in one rework, both datasets rebuilt:

1. **Per-row registrant CIKs.** In combined multi-registrant filings every
   row now carries the entity's OWN CIK + conformed name from the SGML
   header FILER blocks (AEP's seven operating companies, Exelon's five,
   Southern's six, Carnival plc, Rio Tinto Ltd, the seven Frontier funds —
   each a real FILER). General name matcher in census_lib
   (`match_label_to_filer`: canonical tokens incl. CO/CORP/INC/LP/PLC
   fusions, exact > name+suffix > abbreviation-subsequence tiers, ties =
   no match); 11 unmatchable abbreviations ("OG&E", "Wpl", "EIDP", "Cusa"…)
   mapped in committed `entity_aliases.py`, each verified against its
   header. `registrant_or_class` is gone — `registrant` (+ its `cik`) and
   `class_or_series` are separate columns in BOTH datasets; junk
   sentence-fragment labels (~80 distinct) killed by general label-hygiene
   rules in the extractor; raw XBRL members now camel-split (incl. upper
   runs: CubesmartLPAndSubsidiaries). Extractor changes verified
   value-neutral: 0 status changes, 0 row-count changes vs snapshot.
2. **Consolidated value.** `public_float` + `float_basis` (STATED_VALUE /
   STATED_ZERO / STATED_NONE / RESOLVED_FILER_ERROR); as-filed evidence
   kept in `public_float_cover` / `public_float_xbrl`. Rounded prints
   confirmed by an exact tag adopt the tag (TAG_PRECISION_ADOPTED).
3. **Zero semantics.** A cover printing "None" for a wholly-owned co-filer
   is a stated zero (STATED_NONE, 10 rows) — distinct from printed $0
   (STATED_ZERO, 91 rows) and from silence (no row; coverage carries
   NO_FLOAT_DISCLOSED). Every zero row audited against its cover.
4. **Plausibility smoke test** (the human "that can't be right" check, now
   a documented tier): implausible as-filed values are checked against
   independent web sources, judgment logged in float_overrides.py. Twin Vee
   resolved ($5,188,400 — cover prints "$5,188,400 million", tag repeats
   the mantissa x1000; web cap ≈$3M makes both impossible; printed digits
   at scale 0 are the disclosure). Graphjet confirmed unresolvable (prints
   its $6.00 share price in the float blank; flagged
   FLOAT_EQUALS_STATED_PRICE). Sonnet Bio's "1 share outstanding" verified
   GENUINE (merged into Hyperliquid Strategies 2025-12-02, all public
   shares canceled). All other micro-floats verified genuine as printed.

Audits this round: 9 haiku readers re-read every zero row's cover (99 at
audit time; the attribution fixes split two more, giving 101); 16 sonnet
auditors re-checked every multi-registrant filing's CIK attribution
(95 filings, 292 rows); a fresh-eyes sonnet pass re-verified every fix;
1 Opus delta audit. float_status byte-identical to
the pre-rework snapshot (the extractor changes were value-neutral);
coverage dispositions changed only for the corrected filings (→
ROWS_FROM_OVERRIDE); row CSVs rebuild byte-identical across runs.

The mapping audit caught 8 pre-existing defects the old mixed column had
hidden — all cover-verified and fixed via committed overrides:
- unlabeled second-registrant values collapsed into the primary by the
  extractor's value-keyed dedup: Alliant (IPL/WPL $0 floats), American
  States Water (Golden State $0), Lamar (Lamar Media $0 float AND its
  100-share count), Brandywine (the Operating Partnership's $2.3M unit
  float), CenterPoint (Houston Electric's 1,000 shares; CERC's "None"
  float), Berkshire Hathaway Energy (Sierra Pacific's 1,000 shares);
- the Ferrellgas cover table fully re-ruled (4 independent auditors
  concurring): Class A/B Units rotated across registrants, garbled label,
  and Ferrellgas Partners Finance Corp omitted entirely;
- two cover-stated as-of dates displaced by tag instants (Spire, Prosper);
- one Opus-panel override reversed: SL Green Operating Partnership's
  301,668 was the cover's NON-AFFILIATE unit count, not units outstanding.
Override rows whose value is the filer's own tagged number keep XBRL_MATCH
with an OVERRIDE_ATTRIBUTION flag (both pipelines, symmetric rule).

Queued for the next extractor round (general rules, not per-filing): dedup
by (value, as_of) must key the registrant attribution too — that one
mechanism caused 6 of the 8 findings; and a tag instant equal to the
filing date should not displace a cover's explicit as-of date (43 rows
currently carry DATE_FROM_XBRL_TAG with as_of == filing date; only the two
audited ones are corrected). Also flagged by the Opus delta audit, both
pre-existing: two covers print a class line twice and the duplicates
publish as two identical rows (0000950170-25-043115 Class T,
0001437749-25-006605 Class B) — decide dedup-or-keep next round.

Final state: public_float_2025.csv 5,134 rows / 5,069 filings (4,922
XBRL_MATCH 95.9%, 2 AGG, 139+9 reads, 62 override; basis: 5,032 value,
91 zero, 10 none, 1 resolved); shares_outstanding_2025.csv 8,414 rows /
6,771 filings (7,908 XBRL_MATCH 94.0%, 110 AGG, 218 reads, 178 override).
All 7,650 filings accounted for in both coverage tables, 0 unresolved.

ROADMAP.md added: the combined registrant-level dataset (one row per CIK
per filing, relational master + class tables — Stata-safe, no nested
cells) and the unthrottled-runner design for the fast machine.

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
of every conflict, 110 blind sonnet verifications; Opus: 3-agent panel on XBRL-contradicted
rulings, 1 flag-resolution
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
folded into overrides). Closing Opus pairing audit:
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
