# shares-outstanding-census

The census successor to [shares-outstanding](../shares-outstanding/): shares
issued and outstanding — number, share type and class, as-of date — from the
cover page of **every** 10-K, 20-F, and 40-F filed in a calendar year, not a
sample. Built to be re-run annually (set the year, run the pipeline) and to a
publishable standard: the output is meant to be cited, so every number must be
traceable and the dataset must rebuild byte-identical on any machine.

## What defines the dataset

- **Population** — every filing whose form type is exactly `10-K`, `20-F`, or
  `40-F` in the year's four EDGAR quarterly indexes. Filed-in-year, not
  fiscal-year; no amendments.
- **ABS excluded** — filings whose SGML-header SIC is 6189 (asset-backed
  securities) are flagged `excluded_abs` and skipped downstream, but stay in
  the population file so the accounting is complete.
- **Cover page only** — the number is the legally required cover-page
  disclosure (the same one the filer tags as XBRL
  `dei:EntityCommonStockSharesOutstanding`). No balance-sheet fallback.
- **Every filing accounted for** — non-ABS filings with no share count (unit
  MLPs, fund LPs, debt-only issuers) keep their row with an explicit status
  rather than disappearing.
- **One row per share class per filing**, with accession, CIK, company name,
  and filing URL on every row.

## How the numbers are trusted (the validation ladder)

1. **Scrape vs XBRL.** The extractor reads the cover prose; the filer's own
   inline-XBRL tag of the same number is the cross-check. Agreement is the
   presumption of correctness.
2. **Sub-agent reads.** Where XBRL is absent or disagrees, independent agents
   read the cached cover text. Their reports are used to *improve the
   extractor* — general, documented rules only, nothing filing-specific —
   iterating until under 1% of in-scope filings remain unresolved.
3. **Manual overrides.** The final ≤1% get hand-entered answers, hardcoded in
   committed code with provenance, each verified adversarially by a second
   independent reader.

Sub-agent output never enters the dataset directly — it only drives parser
rules and the override table. The final CSV is a pure function of
(year, EDGAR archives, committed code), so any machine reproduces it exactly.

## Relationship to `shares-outstanding`

That project proved extraction on a 1,000-filing sample of the same forms and
reached 100% on it, partly through sample-specific golden rulings. Its lessons
carry over (cover-region scoping, the decoy traps, form-type dispatch, the
triangulation idea); its code and rulings are not imported.

## Setup (once per machine)

1. Copy `config.example.py` to `config.py`; set `USER_AGENT`
   (`"Your Name your@email"`) and `DATA_DIR` (a folder for caches and outputs).
2. Dependencies: `requests`, `pandas`, `beautifulsoup4`, `lxml` (standard
   Spyder/Anaconda).

## Pipeline (run in order, in Spyder)

| Script | What it does |
|---|---|
| `1_build_population.py` | Quarterly EDGAR indexes → every exact-form annual filing, deduped by accession; fetches each filing's SGML header (cached) for canonical name / CIK / SIC; flags ABS (SIC 6189); writes `population_{year}.csv`. |

`census_lib.py` is the shared engine. All fetches are throttled (~6 req/s)
and cached under `DATA_DIR/cache/`, so interrupted runs resume where they
left off and finished runs re-run offline.
