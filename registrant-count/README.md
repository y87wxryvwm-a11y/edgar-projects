# registrant-count

A flat register of every 2025 SEC annual-report **registrant**, built from the
same population as the [shares-outstanding census](../shares-outstanding-census/)
and re-runnable annually. **One row per registrant CIK** — each company appears
once, represented by its last non-amended annual report of the year, and every
column holds that CIK's own value (see [One row per CIK](#one-row-per-cik)).

| Column | Source |
|---|---|
| CIK | the registrant's own FILER block in the SGML header |
| Company Period | header CONFORMED PERIOD OF REPORT, as ISO `YYYY-MM-DD` |
| Filing Date | EDGAR quarterly index, ISO `YYYY-MM-DD` |
| SIC | this CIK's STANDARD INDUSTRIAL CLASSIFICATION |
| State | this CIK's BUSINESS ADDRESS state (→ mail → EDGAR record); EDGAR code |
| State Incorporated | this CIK's STATE OF INCORPORATION (→ EDGAR record); EDGAR code |
| Accession Number | the report this CIK is on (`NNNNNNNNNN-NN-NNNNNN`) |
| BDC | `1` if this CIK's SEC FILE NUMBER starts `814-` (a business development company) |
| ABS | `1` if this CIK's SIC is 6189 (asset-backed securities) |
| multi | `1` if the report carries more than one registrant (combined filing) |
| text_url | URL of the full-submission raw `.txt` |
| filing_url | URL of the filing's EDGAR index page |
| wksi | `dei:EntityWellKnownSeasonedIssuer` checkbox (`1`/`0`) |
| shell | `dei:EntityShellCompany` checkbox (`1`/`0`) |
| afs | accelerated-filer status: `LAF` / `AF` / `NAF` — never blank (`NAF` is the default unless the filing marks Accelerated or Large Accelerated) |
| src | `dei:EntitySmallBusiness` (smaller reporting company) checkbox (`1`/`0`) |
| egc | `dei:EntityEmergingGrowthCompany` checkbox (`1`/`0`) |
| sec_12b | `1` if a security is registered under Exchange Act §12(b) (exchange-listed) |
| sec_12g | `1` if no §12(b) but a security under §12(g) |
| sec_15d | `1` otherwise (the §15(d) reporting default) — exactly one of the three is `1` |

Output: `registrant_count_<year>.csv` (+ a `_provenance.csv` sidecar for the
State / State Incorporated sourcing, a `_fills.csv` sidecar recording how each
status cell was resolved, and a cached `cover_facts_<year>.csv`).

**Every status flag is filled 100% — no blanks.** Each cell is resolved in this
order (recorded per cell in `_fills.csv`): the **filing's own value** wins first
— its dei XBRL tag (`AS_FILED`), or a blind cover-page read where the tag was
absent or wrong (`AGENT_READ`, hardcoded in `registrant_overrides.py`). Only
where the filing is silent do we assume: a status the form structurally can't
carry is `DEFINITIONAL` (ABS issuers have no common equity → `NAF` / not-SRC;
foreign private issuers on 20-F/40-F aren't smaller reporting companies → src 0;
40-F covers have no accelerated-filer box → `NAF`); otherwise the rules-defined
size baseline from the public-float census (`SIZE_HEURISTIC`: afs ≥ $700M `LAF`,
≥ $75M `AF`, else `NAF`; src < $250M float → 1); else the residual `DEFAULT`
(`NAF`, not-SRC). **We default to what the filing says** — a cover that reads
Large Accelerated Filer stays `LAF` even where its float makes that unusual; the
14 filings that disclose both `afs=LAF` and `src=1` (clinical-stage biotechs,
mostly) are reported exactly as filed. We never *manufacture* an impossible
status: a heuristic/default fill is barred from pairing `afs=LAF` with `src=1`
(verified). Findings from the manual cover reads are committed as code
(`registrant_overrides.py`, `registrant_coregistrant_facts.py`), so a from-
scratch rebuild reproduces them exactly.

**Co-registrant statuses** come from the combined cover itself. A combined
filing's inline XBRL tags every registrant's cover facts under the *parent's*
CIK, so a subsidiary's own status can't be read from the XBRL — it's read from
the cover text, which lists each registrant's checkboxes (a per-registrant block,
or a matrix). The 199 combined covers were read by independent agents (a small
model reads, a larger one reviews, the largest adjudicates disagreements) and the
adjudicated per-CIK values committed to `registrant_coregistrant_facts.py`.

**The cover checkboxes** (wksi, shell, afs, src, egc) are read from the filing's
inline-XBRL `dei` cover facts — the tag IS the printed checkbox. The boolean is
taken from the XBRL *transform* (`ixt:booleantrue`/`booleanfalse`), NOT the
displayed glyph: a `booleanfalse` fact often renders ☒ (a checked box next to
"No"), so trusting the glyph would (and originally did) flag thousands of
non-shells as shells. Where a 10-K doesn't tag a fact (~1–4%) the printed
checkbox is scraped, and where neither the tag nor the scrape resolves it, the
cover was read directly (`registrant_overrides.py`) — including one scanned-image
10-K read from the page images. 40-F covers structurally omit the wksi / shell /
accelerated-filer boxes (they carry only EGC); those fall to the documented
defaults. ABS 10-Ks aren't in the cover cache, so ABS take the definitional
default (`NAF` / not-WKSI / not-shell / not-SRC / not-EGC) rather than a read.

**The registration sections** (sec_12b / sec_12g / sec_15d) follow the §12(b) >
§12(g) > §15(d) hierarchy and are determined two ways and combined: the cover's
"Securities registered pursuant to Section 12(b)/(g)" blocks are **scraped**, and
**cross-checked** against the filing's XBRL (`dei:Security12bTitle` /
`Security12gTitle`). A security counts as registered if either source shows it;
the scrape-vs-XBRL agreement is reported by the build. A filer with no §12(b)/(g)
security on the cover defaults to §15(d). ABS documents aren't cached, so ABS
default to §15(d) (their asset-backed 10-Ks carry no §12 securities).

## Population

The filings are the census's: every report whose form type is **exactly**
`10-K`, `20-F`, or `40-F` in the year's four EDGAR quarterly indexes — filed-in-
year, not fiscal-year, no `/A` amendments. **2025: 7,650 filings** (6,431 10-K,
1,077 20-F, 142 40-F). ABS issuers (SIC 6189) are included; filter `SIC == 6189`
to drop them.

## One row per CIK

The unit of this dataset is the **registrant**, not the filing. Two adjustments
turn the 7,650 filings into **7,762 registrant rows**:

* **Combined filings are exploded.** A single annual report often covers several
  registrants — a utility holding company and its operating subsidiaries (AEP =
  8 CIKs, Entergy = 7, Southern = 6), an airline holding company and its airline,
  a REIT and its operating partnership. Each FILER block is its own company with
  its own CIK, SIC, address, state of incorporation, file number and **its own
  cover statuses** (AEP is a Large Accelerated Filer; its subsidiaries are Non-
  accelerated). We emit a row per filer CIK — so every registrant appears, +261
  co-registrants that a primary-filer-only register drops — each pointing back to
  the same filing (`text_url` / `filing_url` / accession) but carrying that CIK's
  own values.
* **Each CIK is then deduped to its last report of the year.** A company that
  filed several annual reports in the calendar year (a delinquent filer catching
  up on back years; a registrant that appears on a combined filing *and* files
  its own report) collapses to its latest-filed, non-amended one — so a CIK never
  appears twice. (−149 superseded filings.)

Net: 7,650 filings → 7,911 filer rows → **7,762 one per distinct CIK** (verified:
the CIK set equals the universe of every filer CIK across all 7,650 filings, and
each CIK's row is its latest filing).

## The two location fields

Both are EDGAR State-or-Country codes (`CA`, `DE`, `M0`=Japan, `A6`=Ontario,
`E9`=Cayman, `F4`=China …), upper-cased — a couple of filers key them lowercase
(`wa`/`ct`).

* **State** is the registrant's business-address state from its own FILER block,
  falling back to its mail-address state. As-filed.
* **State Incorporated** is that CIK's STATE OF INCORPORATION from its FILER block.
  The header omits that line for some filings; where it does, the value is filled
  from **EDGAR's own authoritative company record** for that CIK (the submissions
  API, same code space) — but a fill is **kept only when the filing's own as-filed
  inline-XBRL doesn't contradict it** (validated for the primary registrant; a
  co-registrant's fill comes straight from its own submissions record). The header is trustworthy (wherever it states a value
  it agrees with EDGAR's record 98.8% of the time), but EDGAR's record can be
  stale or conflated where the header is silent — a reincorporation, or a foreign
  filer whose location is mistagged as its incorporation. The filing's own XBRL
  catches those: e.g. EDGAR says Redwood is DE-incorporated and Metalpha is in
  Hong Kong, but each filing's cover/XBRL says Maryland and Cayman Islands — so
  those fills are dropped rather than published wrong. Whatever neither the header
  nor a validated record provides — mostly ABS vehicles, funds, trusts and
  foreign issuers with no structured state of incorporation — stays blank. Set
  `FILL_BLANKS_FROM_API = False` for a pure as-filed-header column instead.

  Field provenance (2025): State — 7,536 header, 2 EDGAR-record, 112 blank;
  State Incorporated — 6,498 header, 213 XBRL-validated EDGAR-record fills, 18
  dropped as XBRL-contradicted, 921 blank. The `_provenance.csv` sidecar records,
  per row, the source and the raw header / EDGAR / XBRL values behind each fill.

The XBRL `dei` incorporation tag is used to **validate** fills, not as a primary
source: read alone it is noisy (filers mix codes and names, ISO and EDGAR codes,
country vs. province), so it is the right tool to catch a contradicted fill but
the wrong tool to source from. The XBRL name is decoded to an EDGAR code using
EDGAR's own code↔name pairs (US postal codes plus the foreign descriptions in
the submissions records) — no hand-typed country table.

## Setup (once per machine)

Copy `config.example.py` to `config.py` and set `USER_AGENT` and `DATA_DIR`. If
you already have the shares-outstanding-census cache, point `SEED_CACHE_DIRS` at
its `cache/` folder and `CENSUS_POPULATION_CSV` at its `population_<year>.csv` —
the build then runs almost entirely offline. Otherwise everything is fetched
from EDGAR (throttled, cached) on first run. Dependencies: `requests`, `pandas`
(plus `lxml` for the XBRL cross-check).

## Scripts (run in order, in Spyder)

| Script | What it does |
|---|---|
| `1_build_registrant_count.py` | Quarterly indexes → population; SGML header → **one record per filer CIK** with that CIK's SIC/state/incorporation/file-number; fills header-blank State / State Incorporated from EDGAR's record (XBRL-validated for the primary); inline-XBRL `dei` cover facts + §12 scrape for the primary, committed cover reads for co-registrants; resolves every status flag (`registrant_fills`); **dedups to one row per CIK (latest report)**. Writes the CSV + `_provenance` + `_fills` sidecars + a `cover_facts_<year>.csv` cache. **First run parses ~6,825 documents (one-time, ~20 min); re-runs finish in seconds.** |
| `2_verify_registrant_count.py` | Deterministic suite (35 checks): one row per CIK; the CSV CIK set equals the universe of every filer CIK across all filings (independent header parser); each CIK's row is its latest-filed report; per-CIK SIC/State/Incorporation/BDC/ABS/multi re-derived from that CIK's own filer block; status flags 100% filled, registration mutually exclusive, no fill manufactures an impossible LAF+SRC; format. Raises if any check fails. |
| `3_crosscheck_api.py` | Cross-checks the **header-sourced** values against EDGAR's authoritative record (independent — those rows didn't use it). |
| `4_crosscheck_xbrl.py` | Cross-checks against the filing's **own as-filed inline-XBRL** `dei` state tags (no current-vs-filed drift). Informational — reports agreement and itemizes the XBRL's noise. |

`registrant_lib.py` is the self-contained engine (index/header parsing, the
location fields, cached-doc XBRL extraction, the cover-checkbox / §12 facts, and
submissions fetch). `registrant_fills.py` is the deterministic 100%-fill resolver
(as-filed > agent-read > definitional > size-heuristic > default, with the
impossible-combo guard); `registrant_overrides.py` and
`registrant_coregistrant_facts.py` hold the committed cover-read findings (single
filers, and per-CIK statuses for combined filings). All fetches are throttled
(~6 req/s) and cached, so interrupted runs resume and finished runs re-run offline.

## How the values are trusted

* **Coverage (one row per CIK)** — an independent header parser re-derives every
  filer CIK from all 7,650 filings; the CSV's CIK set equals that universe exactly
  (0 missing, 0 extra), each CIK appears once, and each CIK's row is verified to
  be its latest-filed report of the year. Every accession is a real 2025 annual
  filing; no amendments.
* **Per-CIK fields** — a second, independently written parser (a line state-
  machine over every FILER block, vs. the build's section-regex) re-derives each
  CIK's own SIC / State / State of Incorporation / file-number(BDC) / SIC-6189(ABS)
  and the filing's filer count (multi), and must match every row.
* **State fields** — header-sourced values agree with EDGAR's authoritative
  record 96.8% (State) / 98.8% (State Incorporated), the few differences being
  as-filed-vs-current drift and redomiciliations. Every record-sourced fill is
  validated against the filing's own as-filed XBRL (18 contradicted fills
  dropped). Independent blind LLM reads confirmed the header field selection on
  20/20 raw headers and the incorporations on 9/9 cover pages.
* **Cover flags** — a 30-agent blind audit read 382 actual cover pages
  (stratified to rare 1-values and scrape/XBRL disagreements) and reconciled
  against every flag; the residual was adversarially re-checked by two skeptic
  agents quoting cover text. Agreement: wksi 100%, shell 99%, src 99%, egc 98%,
  afs 95%, reg 98%, BDC 99%. The audit drove the fixes documented in
  `progress.md` (the shell transform-vs-glyph bug, the 12(g) heading variants,
  …); the remaining disagreements are first-pass-agent errors the skeptics
  overturned, or the dei tag vs a literal blank checkbox (the tag wins), not
  extractor errors.
* **100% fill + blind reads** — to leave no blank, the 73 filings whose flags the
  extractor couldn't resolve (tag absent + scrape missed) or whose value looked
  impossible (afs/src vs float) were blind-read by independent agents over the
  actual cover; that filled 33 gaps and confirmed 39 of 40 suspicious values as
  genuinely as-filed (commodity trusts that check `LAF` on nominal float;
  recently-IPO'd `NAF` first filers; biotechs that check both `LAF` and `SRC`),
  catching one real extractor error (a Bitcoin ETF's afs). One scanned-image 10-K
  was read from its page images. All reads are committed in
  `registrant_overrides.py`. `2_verify` then asserts zero blank status cells and
  that no fill manufactured an impossible `LAF`+`SRC` pair.
