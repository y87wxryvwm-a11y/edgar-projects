# shares-outstanding

Extract **shares issued and outstanding** from SEC annual filings — the number,
the class of shares, and the applicable "as of" date — from the cover page of
each filing. Built for a study sample of 1,000 annual filings split **50% 10-K /
40% 20-F / 10% 40-F**, drawn at random from the prior calendar year.

Example target (Apple's FY2025 10-K, accession `0000320193-25-000079`):

> "14,776,353,000 shares of common stock were issued and outstanding as of October 17, 2025."

→ number `14,776,353,000` · class `common stock` · date `2025-10-17`.

## Setup (once per machine)

1. Copy `config.example.py` to `config.py` and set:
   - `USER_AGENT` — `"Your Name your@email"` (SEC requires a real contact; generic UAs get 403'd).
   - `DATA_DIR` — a folder *outside* this repo for the data files the scripts read/write.
2. Dependencies (already present in a standard Spyder/Anaconda environment):
   `requests`, `beautifulsoup4`, `lxml`, `pandas`.

`config.py` and the data files are git-ignored — only code is committed.

## Pipeline (run in order, in Spyder)

Each script has a `# ---- EDIT THIS ----` block at the top; set the year and
sample size there, then run.

| Script | What it does |
|---|---|
| `1_build_index_and_sample.py` | Downloads the four quarterly EDGAR full-index files for the year (cached), writes the full annual-filing index, and draws the stratified random sample. |
| `2_extract.py` | Downloads each sampled filing's primary document, extracts (shares, class, date) per class, cross-checks the XBRL `dei` fact, and writes the results CSV + per-filing validation input with confidence and flags. |
| `3_dump_evidence.py` | Writes a neutral evidence packet per filing (cover text + every "outstanding" context + the SEC structured fact) — the input for independent validation. |
| `4_…`–`8_…` | The adversarial audit + golden-table loop (see `progress.md`): batch the evidence, reconcile blind agent verdicts, adjudicate disagreements, build the golden truth table, score the extractor against it. |
| `9_build_relevance_batches.py` | Dumps every sampled 10-K cover (offline, from cache) and batches them for the relevance classification agents. |
| `10_reconcile_relevance.py` | Reconciles the two blind relevance passes, batches disagreements for adjudication, writes `relevance_*.json` — 20-F/40-F are NOT_RELEVANT_FORM by rule; 10-Ks are RELEVANT or NOT_RELEVANT_ABS / _UNITS / _DEBT_ONLY. |
| `11_build_final.py` | Merges sample + relevance + extraction + golden status into the final study CSV (`final_*.csv`). |

The study's relevant universe is corporate 10-K registrants with public equity
shares; ABS issuers, unit-denominated non-corporate registrants (MLPs, fund
LPs/LLCs, ETF trusts), debt-only issuers, and the 20-F / 40-F forms are marked
not relevant rather than extracted.

`shares_lib.py` is the engine imported by all three. `validate_helper.py` builds
one evidence packet (used by `3_dump_evidence.py` and runnable standalone:
`python validate_helper.py <cik> <accession> <form>`).

### The full 1,000-filing run

Set `SAMPLE_SIZE = 1000` in `1_build_index_and_sample.py` (the 50/40/10 mix and
seed are already configured), run it, then set the matching `SAMPLE_SIZE = 1000`
in `2_extract.py` and run that. Throttled to ~6 requests/second (well under SEC's
10 req/s limit); the quarterly indexes are cached so re-runs are cheap.

## How the extraction works, and how we trust it

Two extraction strategies, dispatched by form type:

- **10-K** — scan the cover region (everything before `PART I`) for a share-count
  number tied to a share word and the word "outstanding", skipping decoys
  (balance-sheet figures in thousands, authorized/treasury/weighted-average
  shares, the `$` market-value line). Handles scale words ("5,822 **million**")
  and multi-class issuers (Alphabet A/B/C).
- **20-F / 40-F** — anchor on the fixed regulatory cover phrase *"…number of
  outstanding shares of each of the issuer's classes of capital or common stock
  as of the close of the period…"* and read the class/number listing that
  follows (both "Label: N" and "N Label" shapes). The as-of date falls back to
  the filing's `CONFORMED PERIOD OF REPORT` (fiscal close) when none is printed.

Every extraction carries a **confidence score** and **flags** so right answers
are separable from wrong ones and likely misses are visible:

- `XBRL_MATCH` — the number agrees with SEC's structured
  `dei:EntityCommonStockSharesOutstanding` fact (the strongest signal).
- `XBRL_MISMATCH` — disagrees with a contemporaneous dei fact (likely wrong).
- `NO_MATCH` — nothing extracted (a possible false negative).
- `XBRL_FOUND_BUT_NO_PROSE` — SEC has the number but we didn't — a near-certain miss.
- `MULTI_CLASS`, `MULTI_REGISTRANT`, `NO_DATE`, `SCALE_WORD_USED`,
  `NUMBER_OUT_OF_RANGE`, `DATE_IMPLAUSIBLE`, `NO_COVER_MARKERS` — review hints.

The numbers are triangulated three ways: the **prose** scrape, SEC's independent
**XBRL** fact, and an **independent sub-agent read** of the actual cover page
(see `HOW_WE_GOT_THE_DATA.md`).
