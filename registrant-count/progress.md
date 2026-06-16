# Progress log

## 2026-06-16 — 100% fill of every status column (no blanks)

Evan: every status column must be filled 100% — manual agent checks and logical
heuristics allowed — defaulting to **what the filing says**; assume only when the
filing is silent (NAF the default like 15(d); SRC by size). Findings hardcoded so
a from-scratch rebuild reproduces exactly.

Done. New `registrant_fills.py` resolves every flag in order **as-filed (XBRL) >
agent-read > definitional > public-float size baseline > default**, with a guard
that no heuristic/default fill may manufacture an impossible `afs=LAF` + `src=1`
pair (as-filed pairs are kept). `registrant_overrides.py` holds the committed
cover-read findings. `_fills.csv` records the method per cell. `2_verify` now
asserts zero blank status cells (42/42 PASS).

The only blanks were afs (982): 811 ABS, 142 40-F, ~29 non-ABS 10-K/20-F. Plan:
- **73 blind cover reads** (workflow) over every non-ABS/non-40-F filing that was
  blank OR whose value looked impossible (afs/src vs census float). Filled 33
  gaps; **confirmed 39 of 40 suspicious values as genuinely as-filed** — commodity
  trusts checking `LAF` on $1M nominal float, 2024-IPO/spinoff first filers that
  are `NAF` at $10–47B float (Reddit, GE Vernova, Solventum, the Bitcoin ETFs…),
  and 14 clinical biotechs that check **both** `LAF` and `SRC`. Caught one real
  extractor error: Bitwise Bitcoin ETF afs AF→NAF. One scanned-image 10-K (HST
  Global) read from its page JPGs (NAF/SRC/EGC, 12(g)).
- **afs=NONE → NAF**: where the cover shows no filer-category box marked (e.g.
  Graybar's voting-trust 10-K), NAF per the rule.
- **40-F → NAF**: 40-F covers structurally lack the accelerated-filer box (only
  EGC); confirmed on a spot-check; no census float for MJDS filers → NAF default.
- **20-F src → 0**: foreign private issuers aren't smaller reporting companies.
- **ABS → definitional NAF/0** (Evan: ABS are the least concern, skip reading
  them). Note: a spot-check found ABS 10-Ks (utility/auto/CMBS) DO carry the
  checkboxes — but they're not in the cover cache and read as `NAF`/not-X, the
  default we already assign.

Reg corrections from the reads (vetted individually): NAPC 12g→15d, Kyivstar
15d→12b, HST 15d→12g; **rejected** Kioni's 15d→12g (both 12(b)/12(g) "None" =
15(d), agent over-reasoned). Net vs prior: afs 982 blanks→0, +13 src, +9 egc,
−7/−5 shell/wksi (20-F boolballotbox mis-reads corrected), 6 reg cells.

## 2026-06-15 (later still) — 30-agent blind audit of the cover flags + fixes

Evan pushed for thorough independent auditing ("you will look into individual
rows and find the values do not match the filing unless you verify"). Ran a
fleet of 30 blind agents reading actual cover pages (382 filings, stratified to
rare 1-values + scrape/XBRL disagreements + each form), reconciled against every
flag, then adversarially re-checked the residual with 2 independent skeptic
agents quoting cover evidence. It caught real systematic bugs — exactly the
concern. All fixed; agreement now wksi 100 / shell 99 / src 99 / egc 98 / afs 95
/ reg 98 / bdc 99 %, and the residual is first-agent error (skeptics confirmed
our value) or authoritative-tag-vs-literal-box, not extractor error.

Bugs the audit found and fixed:
1. **shell (the big one, 83% → 99%, shell=1 891 → 252).** `dei:EntityShellCompany`
   is tagged with transform `ixt:booleanfalse` (value = false) but RENDERS the
   glyph ☒ (a checked box next to "No"). The decoder trusted the glyph → ~640
   non-shells wrongly flagged. Fix: the transform (`boolean{true,false}` /
   `fixed-{true,false}`) is authoritative; only `boolballotbox`/text reads the
   glyph. (A rare filer mis-tags the ☒ next to *No* under `boolballotbox` — e.g.
   WeRide's 20-F — which no glyph rule recovers; ~1% residual.)
2. **afs (AF vs LAF).** `EntityFilerCategory` = "Large&#160;accelerated&#160;
   filer" — non-breaking spaces broke the substring match. Fix: normalize
   whitespace. (The remaining afs "mismatches" are the dei tag = Non-accelerated
   filer vs a literal cover with all three boxes empty — the tag is the filer's
   authoritative category; we keep it.)
3. **sec_12b over-fired.** Delisted issuers tag a `Security12bTitle` with no
   exchange; empty/nil title & exchange tags. Fix: 12(b) requires a real
   `SecurityExchangeName`; nil/empty/"None" ignored.
4. **sec_12g under-fired badly (the skeptic pass caught this).** The 12(g)
   heading varies far more than assumed: "registered **under** Section 12(g)"
   (not only "pursuant to"), "Securities **to be** registered", "of the
   **Exchange** Act" / no colon, "(Title of Class)" placeholders, and the §15(d)
   "Securities for which there is a reporting obligation" boundary. Generalized
   the scrape; 12g 714 → 853. (IBOC is a genuine filer quirk: Nasdaq stock under
   a 12(g) heading — classified 12b on its exchange listing.)
5. **src/egc/wksi/shell absent-XBRL** (~1–4% of 10-Ks don't tag the dei fact):
   added a cover-checkbox scrape fallback (only when the XBRL tag is absent).
6. **ABS** now any-filer SIC 6189 (matches the census's 825, not 824).

`2_verify` extended; `data/_make_audit_sample.py` + `_reconcile_audit.py` are the
audit harness (gitignored). `cover_facts_<year>.csv` caches the per-filing facts.

## 2026-06-15 (later) — 13 added columns

Evan asked for: BDC, ABS, multi, text_url, filing_url, wksi, shell, afs, src,
egc, sec_12b, sec_12g, sec_15d. Sources, all confirmed against real filings:

- **BDC** — any filer's SEC FILE NUMBER starts `814-` (from the header). **ABS**
  — SIC 6189. **multi** — >1 FILER block. **text_url** — full-submission `.txt`.
  **filing_url** — the filing's EDGAR index page (Evan: index page, not the
  primary doc). All header/deterministic, no new fetches.
- **wksi / shell / src / egc** — the inline-XBRL `dei` cover checkbox facts
  (the tag is the printed box). Decoder handles all three encodings:
  fixed-true/false formats, the ballot-box glyph (☒/☐), and Yes/No text.
  **afs** — `dei:EntityFilerCategory` → `LAF`/`AF`/`NAF` (Evan: categorical, not
  1/0). Blank where the form has no box: 40-F (MJDS) omit wksi/shell/afs; ABS
  10-Ks omit all.
- **sec_12b / sec_12g / sec_15d** — Evan's steer: scrape, then check with XBRL.
  The cover's "Securities registered pursuant to §12(b)/(g)" blocks are scraped
  AND cross-checked against `dei:Security12b/gTitle`; a security counts if
  either shows it; hierarchy 12b > 12g > 15d, 15d the default. Build reports the
  scrape-vs-XBRL agreement. (Evan flagged that file-number prefixes do NOT imply
  12b/12g — correct; MUFG is `000-` but its NYSE ADSs are 12(b). File number is
  used only for BDC.) ABS docs aren't cached → ABS default to 15(d), correct for
  asset-backed 10-Ks.

Smoke test (AMD/MUFG/Rogers-40-F/Redwood-BDC) all correct, scrape==XBRL on each.
The cover-fact parse over the ~6,825 cached docs is one-time (~20 min), cached
to `cover_facts_<year>.csv`; re-runs are instant. `2_verify` extended (0/1 and
afs domains, exactly-one-registration-section, ABS==SIC6189, URL shape, BDC and
multi re-derived independently from the headers).

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
