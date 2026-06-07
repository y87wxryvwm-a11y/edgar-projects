# How we extract shares issued and outstanding

A plain-English walkthrough of how this pipeline turns a thousand raw annual
filings into reliable `(number, share class, as-of date)` rows — and, just as
importantly, how it tells a right answer from a wrong one and how it avoids
silently missing the number.

## The goal

From a filing's cover page we want three facts. Using Apple's FY2025 10-K
(`0000320193-25-000079`):

> "14,776,353,000 shares of common stock were issued and outstanding as of October 17, 2025."

- **number** — 14,776,353,000
- **class** — common stock
- **as-of date** — 2025-10-17

The study sample is 1,000 filings: **50% 10-K** (domestic), **40% 20-F**
(foreign private issuers), **10% 40-F** (Canadian MJDS), drawn at random from the
prior calendar year (2025).

## Where the data comes from

Everything is SEC EDGAR. Three endpoints do the work.

1. **Quarterly full-index** — `…/full-index/{year}/QTR{1..4}/master.idx`, a
   pipe-delimited list of every filing that quarter (`CIK|Company|Form|Date|File`).
   We pull four per year, keep the rows whose form is exactly `10-K`, `20-F`, or
   `40-F` (exact match, so amendments like `10-K/A` don't sneak in), dedupe by
   accession, and draw the stratified sample. The 2025 index has ~6,431 10-Ks,
   ~1,077 20-Fs, and ~142 40-Fs, so the 500/400/100 split is comfortably feasible.

2. **The complete submission text** — `…/edgar/data/{CIK}/{accession}.txt`. We
   stream it and stop at the first `<DOCUMENT>` whose `<TYPE>` matches the form,
   so we download only the primary document (~1–2 MB) and not the megabytes of
   XBRL and exhibits behind it. We also read `CONFORMED PERIOD OF REPORT` from the
   SGML header — the fiscal close, used as the as-of date for foreign forms that
   don't print one on the cover. HTML / inline-XBRL is reduced to clean text with
   BeautifulSoup, after deleting the `ix:header`/`ix:hidden` blocks that otherwise
   dump a wall of XBRL context at the top of the page.

3. **The structured XBRL fact** — `…/api/xbrl/companyconcept/CIK{cik}/dei/
   EntityCommonStockSharesOutstanding.json`. This is SEC's own tagged value of the
   cover-page share count, and it carries the as-of date in its `end` field. Where
   a filer tags it, it's an independent ground truth for the number *and* the date.

## How we find the number

Two strategies, chosen by form type, because the cover wording differs.

**10-K — cover-window scan.** There is no single fixed phrase, so we restrict to
the cover region (everything before `PART I`) and, for every occurrence of
"outstanding", look for a nearby number that (a) sits close to a share word and
(b) isn't a decoy. This naturally captures the wide range of real phrasings:

- `14,776,353,000 shares of common stock were issued and outstanding as of …` (Apple)
- `As of …, there were 5,822 million shares of … Class A … outstanding, 837 million … Class B …` (Alphabet — scale words + three classes)
- `number of shares outstanding … is 11,146,230 as of …` (JAKKS)
- `Number of Redeemable Capital Shares outstanding as of … : 1,000,000` (number after a colon)
- `there were outstanding, exclusive of treasury shares, 20,077,893 shares …` (Financial Institutions)

**20-F / 40-F — regulatory anchor.** These covers carry a fixed instruction:
*"…number of outstanding shares of each of the issuer's classes of capital or
common stock as of the close of the period covered by the annual report…"* We
anchor on that phrase and parse the class/number listing that follows, in either
shape — label-first (`Ordinary Shares … : 1,228,504,232`, SAP) or number-first
(`295,935,686 Common Shares  4,866,814 Series A First Preferred Shares …`,
Emera). Multi-class issuers (Brookfield's limited-voting + preference series) come
out as one row per class.

Scale words ("million"/"billion"), thousands separators, and `(as of <date>)`
parentheticals are all handled; the class label is mapped to a type
(common / ordinary / preferred / depositary / other).

## The traps, and how each is defused

These are the ways a naive scraper gets a *wrong* number. Every one bit us during
development and is now handled:

| Trap | Example | Defense |
|---|---|---|
| Balance-sheet count in **thousands** | Apple "14,773,260 … shares issued and outstanding" | restrict to the cover region (before PART I); the balance sheet is well past it |
| **Authorized** shares | "1,000,000,000 authorized" (Endo) | reject a number immediately followed by "authorized" with no "outstanding"/"issued" |
| **Weighted-average** shares (EPS) | "weighted-average basic shares outstanding 14,948,500" | in the body, not the cover; tight "weighted" pre-check as backstop |
| The **$ market-value** line | "aggregate market value … held by non-affiliates … $ 3.25 trillion" | skip any number preceded by `$` (even "`$ `" with a space) |
| **Calendar years** from dates | "December 31, 2024" | drop bare 4-digit 19xx/20xx with no comma and no scale word |
| **Record-holder** counts | "73,288 record holders" | reject a number followed by "record holders / holders of record" |
| **Curly apostrophe** breaking the anchor | "issuer's" with U+2019 | normalize unicode punctuation before matching |
| **Combined / multi-registrant** 10-Ks | Exelon + ComEd + PECO + … in one filing | flag `MULTI_REGISTRANT`; capture each |

## How we know it's right, wrong, or missed — three independent checks

No single method is trusted on its own. Each number is triangulated:

1. **Prose scrape** (the regexes above) — the primary read.
2. **XBRL cross-check** — compare to SEC's `dei:EntityCommonStockSharesOutstanding`
   for a contemporaneous period (within ~400 days of filing). Agreement →
   `XBRL_MATCH`; disagreement → `XBRL_MISMATCH`; SEC has it but we don't →
   `XBRL_FOUND_BUT_NO_PROSE` (a near-certain false negative). We only compare
   against a *recent* fact, so delisted issuers with stale tags don't raise false
   alarms.
3. **Independent sub-agent read** — a separate agent reads the actual cover text
   (not our regex output) for each sampled filing, decides the ground truth with
   its own judgment, and grades our extraction (`3_dump_evidence.py` builds the
   evidence; the validation workflow runs the auditors).

Every row also carries a **confidence score** and **flags** (`NO_MATCH`,
`NO_DATE`, `MULTI_CLASS`, `SCALE_WORD_USED`, `NUMBER_OUT_OF_RANGE`,
`DATE_IMPLAUSIBLE`, `NO_COVER_MARKERS`, …) so a reviewer can sort by trust and
focus on the handful that need eyes.

**Avoiding false negatives specifically.** A miss is the quietest failure, so it
gets the loudest signals: `NO_MATCH` whenever nothing is extracted, and
`XBRL_FOUND_BUT_NO_PROSE` when SEC's structured data proves a number exists that
we didn't find. True negatives are real, too — asset-backed trusts (CMBS, auto-
loan owner trusts) file 10-Ks but have no common stock outstanding; those
correctly produce no row and are confirmed `TRUE_NEGATIVE` by the auditor pass.

## Validation results (60-filing stratified sample)

A stratified sample of 60 filings (30 10-K, 24 20-F, 6 40-F — the same 50/40/10
mix as the full study) was extracted and then **independently audited**: a
separate agent read each filing's actual cover text (not the extractor's output),
decided the ground truth itself, and graded the extraction. We iterated the
parser twice on what the auditors flagged.

**Final outcome — every filing handled correctly:**

| Metric | Result |
|---|---|
| Primary number correct | **60 / 60** |
| As-of date correct | **60 / 60** |
| Auditor verdict | **57 AGREE + 3 TRUE_NEGATIVE + 0 PARTIAL** |
| Per form | 10-K 30/30 · 20-F 24/24 · 40-F 6/6 |
| Coverage | 57/60 extracted; the other 3 are CMBS / auto-loan / commodity ABS trusts with no common stock — correctly empty (TRUE_NEGATIVE) |
| XBRL agreement | 44 of 60 carry a contemporaneous `dei` fact and **all 44 match**; the rest are foreign / multi-class filers without a comparable tagged rollup, validated by prose + the auditor |

**What the two iterations caught (and what it teaches about scraping this data):**
the numbers were right from the first pass (60/60); every issue the auditors
raised was a *secondary* one, and each became a parser rule:

- Iteration 1 (12 issues): the regulatory anchor used literal spaces and silently
  failed when a cover broke the phrase across **newlines** → foreign issuers fell
  back and mislabeled their class as "common stock"; identical-count preferred
  series were **deduped** away; some foreign covers print no inline date.
- Iteration 2 (5 issues): a label-first list **without a colon** (`Common Shares
  926,610,598`) mis-bound the label; a **subset** in parentheses (`(including
  335,787,795 … ADS)`), a **treasury** count, and a **warrant** count were each
  mistaken for a separate share class.
- Iteration 3: 0 issues.

The lesson is the one the pipeline is built around: getting the right *number* is
the easy 95%; the last 5% — right class, right date, no decoys, no missed
classes, and knowing when the correct answer is "none" — is where the
triangulation (prose + XBRL + an independent reader) earns its keep.

Artifacts in `DATA_DIR`: `extraction_results_*.csv` (one row per class, with
confidence + flags), `validation_input_*.jsonl`, `validation_verdicts.csv`, and
`evidence/` (the neutral packet each auditor read).
