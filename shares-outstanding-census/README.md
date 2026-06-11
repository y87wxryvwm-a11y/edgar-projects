# shares-outstanding-census

The census successor to [shares-outstanding](../shares-outstanding/): shares
issued and outstanding — number, share type and class, as-of date — from the
cover page of **every** 10-K, 20-F, and 40-F filed in a calendar year, not a
sample. Built to be re-run annually (set the year, run the pipeline) and to a
publishable standard: the output is meant to be cited, so every number must be
traceable and the dataset must rebuild byte-identical on any machine.

The same machinery also produces a **public-float census** over the identical
population (scripts 8–14 below): the aggregate market value of common equity
held by non-affiliates from each 10-K cover, validated against the filer's
own `dei:EntityPublicFloat` tag. Standalone output, joined to the shares
dataset by accession number.

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
| `2_fetch_documents.py` | Streams each in-scope filing's primary document (the download stops once it's captured — exhibits never transfer); gzip-cached with a sha256 metadata sidecar. |
| `3_extract_facts.py` | One offline pass over the cached documents: every inline-XBRL `dei:EntityCommonStockSharesOutstanding` fact (value, instant, dimensions) → `ixbrl_facts_{year}.csv`, plus the document reduced to clean text → `cache/text/`. |
| `4_extract_and_validate.py` | Runs the cover extractor over the cached text and validates every row against the XBRL facts; writes `extraction_{year}.csv` (one row per share class) and `filing_status_{year}.csv` (the per-filing verdict driving the improvement loop). Fast to re-run after every extractor change. |
| `5_fetch_xbrl_api.py` | SEC companyconcept API per CIK (cached, 404s included) → `xbrl_api_facts_{year}.csv`, the secondary XBRL source where a filing's own document carries no parseable facts. |
| `6_build_evidence.py` | Neutral evidence packets (cover text + extraction + both XBRL fact sets) for whatever XBRL can't settle — the input to the independent-read tiers. |
| `7_build_final.py` | Deterministic assembly of `shares_outstanding_{year}.csv` (one row per share class, with validation provenance and quality flags) and `filing_coverage_{year}.csv` (every filing accounted for). Consumes `overrides.py`; no network, no sub-agents. |

### Public-float pipeline (scripts 8–14, same caches, run after 1–3)

| Script | What it does |
|---|---|
| `8_extract_float_facts.py` | Offline pass over the cached documents: every inline-XBRL `dei:EntityPublicFloat` fact (value, instant, dimensions, currency; zeros and nils kept as signals) → `float_facts_{year}.csv`. |
| `9_extract_validate_float.py` | Runs `float_extractor.py` over the cached text and validates each value against the float facts, precision-aware (an exact figure must match exactly; "$5.6 billion" matches within its printed precision; identical-mantissa tag-scale errors are isolated, never auto-validated) → `float_extraction_{year}.csv`, `float_status_{year}.csv`. |
| `10_fetch_float_api.py` | SEC companyconcept API for `EntityPublicFloat` per CIK (cached, 404s included) → `float_api_facts_{year}.csv`, the secondary XBRL source. |
| `11_build_float_evidence.py` | Blind evidence packets (cover text separated from extraction context) for whatever XBRL can't settle, plus negative-class samples — the input to the independent-read tiers. |
| `13_fold_float_reads.py` | Folds the saved read/adjudication verdicts into review-ready override tables (merged into `float_overrides.py` only after adversarial verification). |
| `12_build_final_float.py` | Deterministic assembly of `public_float_{year}.csv` (one row per filing; per registrant/class on combined filings; both the cover value and the tag value as filed) and `float_coverage_{year}.csv` (every filing accounted for). |
| `14_check_float.py` | Assertion suite: coverage completeness, row integrity, and an implied-price cross-check against the shares-outstanding dataset (float ÷ shares must land in a sane band). |

The float disclosure is a 10-K cover requirement, so every 20-F/40-F resolves
to an explicit no-disclosure status rather than a missing row; so do shell
and wholly-owned 10-K filers. Filer-side oddities are reported as filed but
flagged (`IMPLAUSIBLE_AS_FILED`, `AS_FILED_MICRO_VALUE`, `IMPLAUSIBLE_DATE`).

**2025 result:** 5,132 float rows across 5,069 filings; all 7,650 population
filings accounted for (1,753 explicit no-disclosure, 825 ABS-excluded);
95.9% of rows validated by the filer's own XBRL, the rest by two-blind-read
confirmation or audited overrides; two independent Opus audits (full + delta)
passed; byte-identical across rebuilds.

`overrides.py` is the committed record of every filing whose rows don't come
from the extractor alone — confirmations, no-disclosure filings, and
hand-verified overrides, each with provenance; `float_overrides.py` is its
public-float counterpart. `METHODOLOGY.md` is the plain-English account of
how every number was produced and verified.

**2025 result:** 8,410 share-class rows across 6,771 filings; all 7,650
population filings accounted for; 94.1% of rows validated by the filer's own
XBRL, the rest by independent multi-model reads or audited overrides;
byte-identical across rebuilds.

`census_lib.py` is the shared engine; `cover_extractor.py` is the documented
extraction methodology (every rule a general property of how filings are
written — never a fix for one particular filing). All fetches are throttled
(~6 req/s) and cached under `DATA_DIR/cache/`, so interrupted runs resume
where they left off and finished runs re-run offline.
