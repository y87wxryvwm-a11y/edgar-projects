# progress — shares-outstanding

## Status: validated pipeline, ready for the full 1,000-filing run

A complete pipeline extracts `(number, share class, as-of date)` from the cover
page of SEC annual filings (10-K / 20-F / 40-F), with confidence scores, flags,
and a three-way validation design (prose + XBRL + independent sub-agent read).

### What's done
- `shares_lib.py` — the engine (index fetch, stratified sampler, streaming
  primary-doc extractor, 10-K cover-window + 20-F/40-F regulatory-anchor parsers,
  XBRL `dei` cross-check, confidence + flags).
- Runner scripts `1_build_index_and_sample.py`, `2_extract.py`,
  `3_dump_evidence.py`; `validate_helper.py`; `config.example.py`.
- Docs: `README.md`, `HOW_WE_GOT_THE_DATA.md`.
- Validated on a 60-filing stratified sample with two independent sub-agent audit
  passes + parser iteration:
  **final 60/60 numbers, 60/60 dates, 57 AGREE + 3 TRUE_NEGATIVE, 0 PARTIAL.**

### Validation method (per the user's request)
For each sampled filing, an independent agent read the actual cover text (via the
neutral `evidence/<accession>.txt` packet — not the parser's output), decided the
ground truth itself, and graded the extractor. Disagreements drove two rounds of
fixes (newline-broken anchor, lowercase "ordinary shares", number-first lists,
identical-count series dedupe, date fallback, narrative/subset/treasury/warrant
decoys). See `HOW_WE_GOT_THE_DATA.md` for the full disposition.

### Next lines of discovery
- **Run the full 1,000.** Set `SAMPLE_SIZE = 1000` in `1_build_index_and_sample.py`
  and `2_extract.py` and run (2025 has enough: ~6,431 10-K / 1,077 20-F / 142 40-F).
  Throttled to ~6 req/s; quarterly indexes are cached.
- **Spot-audit the full run** by sampling ~40 rows (especially `NO_XBRL` and
  `MULTI_CLASS`) through the same sub-agent validation workflow.
- **Known minor limitations** (number + date still correct):
  - Combined multi-registrant 10-Ks (Exelon-style) are flagged `MULTI_REGISTRANT`
    but not split per co-registrant.
  - A foreign cover that says only "the Registrant's shares" (no "ordinary"/
    "common") yields a generic class label; type defaults to common.
- **Optional**: pull `dei:EntityCommonStockSharesOutstanding` per-class via the
  XBRL *frames* API to cross-check multi-class totals where the companyconcept
  rollup is absent.
