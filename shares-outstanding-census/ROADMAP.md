# Roadmap

Two committed future goals, design settled in advance so today's code keeps
pointing at them. Neither is built yet.

## 1. The combined registrant-level dataset

The shares and float censuses merge into one published dataset whose **base
row is a unique registrant — anything with its own CIK inside an annual
filing**, including subsidiary co-filers and ABS issuers. Nothing is hidden:
a registrant with no share count and no float keeps its row with explicit
status values.

Shape: a relational pair of tables, not nested cells. R and Stata cannot
work with a dictionary packed into a CSV cell, so "one data structure per
company" is delivered the standard way — a company-level master table plus
a class-level long table joined by key. (A JSON Lines convenience file with
the classes nested per registrant can be generated from the same pair for
Python users.)

**`registrants_{year}.csv`** — one row per (accession, cik):
- identity: accession, cik, registrant (conformed name), form, date_filed,
  sic, is_primary_filer, filing_index_url
- scope: status (IN_SCOPE / EXCLUDED_ABS), with ABS rows carrying empty
  measures rather than vanishing
- shares: n_share_classes, shares_total (sum where summing is meaningful,
  else empty + flag), shares_status (DISCLOSED / NO_COUNT_DISCLOSED: kind)
- float: public_float, float_basis (STATED_VALUE / STATED_ZERO /
  STATED_NONE / RESOLVED_FILER_ERROR), float_status for rows with no value
  (NOT_DISCLOSED / NOT_REQUIRED_FORM for 20-F/40-F / TAGGED_ZERO_COVER_SILENT
  / NIL …), public_float_date
  - the three user-facing cases stay distinct: not applicable / genuinely
    zero (stated) / a total value
- per-class float, where covers break it out, lives in the class table

**`share_classes_{year}.csv`** — one row per (accession, cik, class):
today's shares_outstanding columns plus a per-class public_float column
where disclosed (Royal Caribbean-style class floats, ProShares series).

Both current datasets already carry the join key (cik + registrant per
row, own CIKs for co-filers), so the merge is a deterministic assembly
script over existing outputs — no new extraction.

## 2. The unthrottled runner (separate fast machine)

Once the dataset design is frozen, a stripped variant of the pipeline runs
on a beefier machine with no rate limiting, producing **byte-identical**
final CSVs. Design constraints already in place:

- Every network access goes through one chokepoint
  (`census_lib.throttled_get`); the fast variant swaps in a concurrent
  fetcher (no throttle, retry on 429/503) without touching extraction
  logic. Nothing else in the pipeline knows the network exists.
- All downstream stages are pure functions over the caches keyed by
  accession, so fetch order/concurrency cannot change output bytes.
- The runner folder must be self-contained: scripts 1–14 + census_lib +
  extractors + overrides/aliases + config.example.py, no imports from
  elsewhere in the repo.
- Verification: after a fast run, `14_check_float.py` (and the shares
  assertions) must pass and the CSVs must hash equal to the reference
  machine's. Caches are immutable EDGAR content, so equality is expected,
  not hoped for.

Practical note: EDGAR enforces ~10 req/s per IP and may block heavy
abusers, so "as fast as possible" still means a declared User-Agent and
backoff on 403/429 — the fast variant removes our self-imposed 6 req/s
pacing and fetches concurrently; it does not remove error handling.
