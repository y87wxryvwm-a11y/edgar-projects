# How the 2025 census datasets were built

Two datasets over the same population: shares outstanding (below) and
public float (last section). The validation philosophy is identical.

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

1. **`XBRL_MATCH` (7,908 rows — 94.0%)** — the prose count equals the filer's own
   inline-XBRL `dei:EntityCommonStockSharesOutstanding` tag in the same
   document (or, where the document carries no parseable tags, SEC's
   companyconcept API record for the same accession). Two independent
   expressions of the number by the filer, in agreement.
2. **`XBRL_AGG_MATCH` (110 rows)** — per-class prose rows whose sum equals
   the filer's single tagged total.
3. **`READS_SONNET_CONFIRMED` (218 rows)** — no usable XBRL: the extraction
   was confirmed by independent machine readers (two blind passes, plus a
   stronger-model adjudication wherever any disagreement existed).
4. **`OVERRIDE_VERIFIED` (178 rows)** — the extractor's output was wrong or
   incomplete; the rows come from `overrides.py`, the committed, per-filing
   record of hand-verified readings. Every entry carries its provenance
   (which independent readings agreed, and the final adjudicator). The
   hardest 53 filings — including every case where a reading contradicted
   the filer's own XBRL — were ruled by a top-tier model panel.

Ambiguous class attributions were resolved with the filer's own XBRL
dimension members (`CLASS_FROM_XBRL` / `REGISTRANT_FROM_XBRL` in
`quality_flags`) — the filer's tagging, not our guess.

## Whose number is it (registrant identity)

Each row belongs to exactly one registrant, identified by `cik` +
`registrant`. In combined multi-registrant filings (a utility holding
company and its subsidiary co-filers in one 10-K), every subsidiary row
carries the subsidiary's **own** CIK and EDGAR conformed name, taken from
the filing's SGML header FILER blocks — never the parent's CIK. Cover
labels are matched to FILER blocks by a general name matcher
(`census_lib.match_label_to_filer`: canonical tokens, abbreviation tiers);
the few labels it cannot connect ("OG&E", "EIDP") are mapped in the
committed `entity_aliases.py`, each entry verified against the header.
`class_or_series` carries any below-registrant designation that is not
itself a share class — a fund series, a tracking-stock group. A registrant
name and a share class never share a column.

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

# How the 2025 public-float census was built

`public_float_2025.csv` holds, for every 10-K that discloses it, the cover
page's aggregate market value of common equity held by non-affiliates — the
"public float" of Exchange Act Rule 12b-2 — as of the last business day of
the filer's most recently completed second fiscal quarter. Same population
as the shares census (7,650 filings, ABS excluded); `float_coverage_2025.csv`
accounts for every filing. The disclosure is a 10-K cover requirement, so
all 1,219 20-F/40-F filings, and 10-K shells, wholly-owned subsidiaries and
not-yet-traded registrants, carry explicit no-disclosure dispositions
instead of rows.

## Whose number is it (registrant identity)

Same rule as the shares census: each row belongs to exactly one registrant
(`cik` + `registrant`, the entity's own CIK and conformed name from the
filing's SGML header FILER blocks — a subsidiary co-filer never carries its
parent's CIK), with `class_or_series` for below-registrant breakouts
(share classes, fund series). Registrants and classes never share a column.

## One verified value, two as-filed witnesses

`public_float` is the single verified value; `float_basis` says what kind
of disclosure it is:

- **STATED_VALUE** — the cover prints a dollar value. Where the print is
  rounded ("$5.6 billion") and the filer's own tag confirms it within the
  printed precision, the tag's exact figure is adopted (flagged
  `TAG_PRECISION_ADOPTED`).
- **STATED_ZERO / STATED_NONE** — the cover prints $0, or prints "None"
  for that registrant (the norm for wholly-owned utility co-filers). These
  are genuine zero floats *stated in the filing* — distinct from a filing
  that says nothing, which never gets a row (its filing appears in
  `float_coverage` as `NO_FLOAT_DISCLOSED`).
- **RESOLVED_FILER_ERROR** — both as-filed numbers are wrong and the true
  reading is recoverable from the filing itself; the value comes from a
  logged override judgment (see below).

The as-filed evidence stays beside it: `public_float_cover` (the cover's
printed number normalized to plain dollars) and `public_float_xbrl` (the
filer's `dei:EntityPublicFloat` tag as filed). They usually agree exactly.
Where they disagree by a clean power of ten with identical digits, the
tag's scale attribute is a filing-agent error (64 filings); the cover is
the disclosure of record and the case went to independent reads, never
auto-validation.

## The plausibility smoke test

Implausible as-filed values (consolidated float above $5T, or implied
$/share outside [0.00005, 800000] against the shares census) are checked
against independent web sources — the check a human reviewer would make —
before any judgment. The full record lives in `float_overrides.py`. In
2025 it resolved one filing (Twin Vee PowerCats: the cover prints
"$5,188,400 million" and the tag repeats the same mantissa at x1000; web
sources cap the company's entire market value near $3M, so the printed
digits at scale 0 are the disclosure and `public_float` = 5,188,400 under
RESOLVED_FILER_ERROR), confirmed one filer error as unresolvable (Graphjet
prints its $6.00 share price in the float blank and tagged 6 — the true
float appears nowhere in the filing; kept as filed, flagged
`FLOAT_EQUALS_STATED_PRICE`), and verified every remaining micro-float as
genuine (par-value-based and sub-penny-price floats on shells, each stated
and tagged identically; flagged `AS_FILED_MICRO_VALUE`).

## The validation ladder

1. **XBRL_MATCH (4,922 rows — 95.9%)** — the cover value equals the filer's
   inline tag (or the SEC companyconcept record), within the printed
   precision of the prose. Includes override rows whose value is the
   filer's own tagged number and where the override supplied only the
   attribution (flagged `OVERRIDE_ATTRIBUTION`).
2. **XBRL_AGG_MATCH (2 rows)** — per-class cover values summing to the
   filer's single tagged total.
3. **READS_2BLIND_CONFIRMED (139) / READS_SONNET_CONFIRMED (9)** — no usable
   XBRL or a tag-scale error: two independent machine readers reproduced the
   extraction blind, or an adjudicator ruled it right.
4. **OVERRIDE_VERIFIED (62 rows)** — the extractor was wrong or incomplete;
   rows come from `float_overrides.py`, each adjudicated, then adversarially
   verified by a separate reader instructed to refute it.

The no-disclosure classes were sampled the same way: 91 filings with no
float row (all 41 silent 10-Ks among them) were independently read blind;
none hid a disclosed float that survived adjudication.

## Cross-dataset check

Because the shares census covers the same filings, every float row was also
screened by implied price (float ÷ total shares outstanding). Everything
outside a generous band was individually verified against its cover and
then against the web (the smoke test above); the survivors are genuine
as-filed oddities and carry flags.

## Reproducibility and audits

Same property as the shares census: `public_float_2025.csv` is a
deterministic function of (year, EDGAR archives, this repository); rebuilds
hash identical. Two independent Opus-tier audits (a full stratified audit of
~70 rows/dispositions against the covers, then a delta audit of the fixes)
passed; the registrant-identity rework was audited row-by-row (every
multi-registrant filing's CIK attribution, every zero row's printed basis)
by independent readers.

## Known limits

- 10 multi-class filings carry correct per-class values but no class labels
  (their filers tagged no class dimensions to attribute from).
- The as-of date is the cover's stated date; where only the tag carries a
  date it is adopted and flagged DATE_FROM_XBRL_TAG. One cover prints a
  future date (a filer typo), kept as filed under IMPLAUSIBLE_DATE.
- Micro-floats under $1,000 are as-filed, flagged AS_FILED_MICRO_VALUE, and
  each verified genuine by the smoke test — except Graphjet's, a confirmed
  filer error whose true value the filing never states
  (FLOAT_EQUALS_STATED_PRICE).
