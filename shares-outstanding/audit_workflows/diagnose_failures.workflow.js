export const meta = {
  name: 'shares-diagnose-failures',
  description: 'One agent per golden-check failure: read the full cover evidence, explain the exact failure mechanism, and propose the smallest generalizable parser rule',
  phases: [{ title: 'Diagnose', detail: 'one agent per failing filing; returns structured diagnosis' }],
}

// args: { inputPath, count, libPath } — each agent reads its own failure row
// (by index) from inputPath, so the list never travels through args.
const A = typeof args === 'string' ? JSON.parse(args) : args
const { inputPath, count, libPath } = A
if (!inputPath || !count || !libPath) {
  throw new Error('bad args: ' + JSON.stringify(args).slice(0, 200))
}

const SCHEMA = {
  type: 'object',
  properties: {
    accession: { type: 'string' },
    cover_truth_snippet: { type: 'string', description: 'the exact cover sentence(s) where the golden-truth numbers/classes/date appear, verbatim' },
    failure_mechanism: { type: 'string', description: 'precisely which number/date/type is wrong or missing and what text pattern in the cover caused the extractor to fail' },
    proposed_rule: { type: 'string', description: 'the smallest generalizable parser rule that fixes this without overfitting to one company' },
    pattern_key: { type: 'string', description: 'short snake_case key grouping failures with the same root cause, e.g. multi_registrant_cover, as_of_date_vs_fiscal_close, decimal_class_list' },
    confidence: { type: 'string', enum: ['high', 'medium', 'low'] },
  },
  required: ['accession', 'cover_truth_snippet', 'failure_mechanism', 'proposed_rule', 'pattern_key', 'confidence'],
}

const PROMPT = (i) => `You are diagnosing ONE golden-check failure of a shares-outstanding cover-page extractor for SEC annual filings (10-K / 20-F / 40-F). Do NOT edit any files — return a structured diagnosis only.

Read the JSON array at ${inputPath} and take element index ${i} (0-based) — that is your failing filing: {accession, form, company, status, detail, flags, ext, truth, cover_path}.

- "truth" is the adversarially-audited golden answer, formatted "number/share_type/as_of_date" per share class.
- "ext" is what the extractor currently returns, same format.
- "status": FAIL_EXTRA_NUMBER = extractor reported number(s) not in the truth; FAIL_MISS_NUMBER = a truth number is missing; FAIL_MISS = extractor returned nothing; FAIL_DATE = right numbers, wrong as-of date; FAIL_TYPE = right numbers, wrong share_type.

EVIDENCE: read the filing's cover_path (full cover region as the parser sees it + every "outstanding" context in the document).

THE EXTRACTOR: read the extraction logic in ${libPath} — for 10-K the cover-window scan (extract_cover_window / _skip_number_context / _nearest_date / _grab_class_label / classify_share_type and the post-filters in process_filing); for 20-F/40-F the anchor strategy (extract_anchor / _extract_span_pairs / _LABEL_AT_START_RE / _LABEL_PHRASE_RE) with cover-window fallback — so your diagnosis names the actual mechanism, not a guess.

Diagnose:
1. Find the exact cover sentence(s) where the golden truth appears (quote verbatim).
2. Explain the precise failure mechanism — which truth/extra number, which text pattern, which parser rule misfires or is missing.
3. Propose the SMALLEST generalizable rule that fixes it. It must not overfit to this one company, and must be safe against the ~900 currently-passing filings.
4. Assign a pattern_key so failures with the same root cause group together.`

phase('Diagnose')

const out = await parallel(
  Array.from({ length: count }, (_, i) => () =>
    // model policy: code-reasoning diagnosis runs on sonnet, never the
    // inherited Fable tier (fleet of it kills the plan budget)
    agent(PROMPT(i), { label: `diag:${i}`, phase: 'Diagnose', schema: SCHEMA, model: 'sonnet' })
  )
)

const ok = out.filter(Boolean)
log(`diagnosed ${ok.length}/${count} failures`)
return ok
