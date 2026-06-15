# Progress log

## 2026-06-15 — project start, dataset complete

New folder per Evan: a one-row-per-filing register of the 2025 annual-filer
population with CIK, Company Period, Filing Date, SIC, State, State
Incorporated, Accession Number.

**Population** reuses the census definition exactly — exact-form 10-K/20-F/40-F
in the four 2025 quarterly indexes, deduped by accession, no amendments: 7,650
filings (6,431/1,077/142), ABS (SIC 6189, 825) included as annual filers. The
accession set is re-derived here independently from the cached master indexes
and matches `population_2025.csv` row-for-row; CIK/SIC/period/filing-date/
accession also match it row-for-row. Engine (`registrant_lib.py`) is
self-contained — same EDGAR source, own code — and reads the census cache when
present so it runs offline.

Five of the seven columns come straight from the SGML header (primary FILER
block). The two new ones needed work:

- **State** (business-address state, → mail) — from the header; as-filed.
  Blank for 112 (foreign filers with no state/province). Codes upper-cased (2
  filers keyed `wa`/`ct`).
- **State Incorporated** — header STATE OF INCORPORATION, blank for 1,152
  filings. Investigated three fills:
  1. *XBRL* (`dei:EntityIncorporationStateCountryCode`, as-filed): fills ~73%
     but **noisy** — filers mix codes/names, ISO vs EDGAR (`IL`=Israel vs
     Illinois), country vs province (`Canada` vs Ontario), and several tag
     "Delaware" where the header AND EDGAR's record say CA. Rejected as a
     source; kept as a verifier (Evan's steer: XBRL as a verification source).
  2. *EDGAR submissions API* (`stateOfIncorporation`, clean EDGAR codes):
     authoritative and agrees with the header wherever both exist, but where
     the header is silent it can be stale (a reincorporation) or conflated (a
     foreign filer's location mistagged as incorporation).
  3. *Pure header* (leave blank): available via `FILL_BLANKS_FROM_API=False`.

  **Chosen (Evan): fill from the API but keep a fill only when the filing's own
  as-filed XBRL doesn't contradict it.** Best of both — completeness without the
  stale fills. The XBRL name is decoded to an EDGAR code via EDGAR's own
  code↔name pairs (US postal + foreign descriptions in the submissions records),
  and a fill whose decoded XBRL differs from the API value is dropped. 18 fills
  dropped this way (incl. Redwood DE→Maryland, Metalpha Hong-Kong→Cayman),
  caught originally by the blind cover reads below.

  Final source split: State 7,536 header / 2 API / 112 none; State Incorporated
  6,498 header / 213 XBRL-validated API / 18 dropped (XBRL_CONFLICT) / 921 none.
  A `_provenance.csv` sidecar records source + raw header/API/XBRL values per row.

The header proved the reliable side throughout: where it states a value it
agrees with EDGAR's authoritative record 96.8% (State) / 98.8% (State
Incorporated) — the gaps are relocations/redomiciliations (as-filed vs current,
header = the filing moment) — and with the filing's own as-filed XBRL 95.1% /
95.2% (the gaps are XBRL's noise, which is why XBRL validates fills rather than
sourcing them).

**Verification.** `2_verify` is a deterministic green-or-raise suite (21/21):
completeness re-derived from the raw indexes + matched to the census; an
independently written second header parser (line state-machine vs the build's
section-regex) reproduces every header value; API-filled values re-checked
against the cached record; every XBRL_CONFLICT drop rests on real evidence;
provenance/resolution consistency; format. `3` is the API cross-check on
header-sourced rows (independent); `4` is the XBRL as-filed cross-check
(informational). Plus blind LLM reads of a stratified sample: 20/20 raw headers
reproduced the field selection exactly (incl. the lowercase wa/ct), 9/9 cover
pages confirmed the header-sourced incorporations, and the cover reads of the
API-filled rows are what surfaced the 2 stale fills (Redwood, Metalpha) now
dropped by XBRL validation. A CSV review-gate agent passed (7,650 rows, exact
columns, no defects). The workflow's parallel read step wouldn't spawn agents,
so the reads were run as direct sub-agents instead.

One real data finding logged: 2 filers key the business-state code lowercase
(`wa`/`ct`) — normalized to uppercase (meaning unchanged) so a state isn't
split into two categories.
