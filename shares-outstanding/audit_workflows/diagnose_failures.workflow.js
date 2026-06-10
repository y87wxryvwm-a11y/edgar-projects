export const meta = {
  name: 'shares-diagnose-failures',
  description: 'One agent per golden-check failure: read the full cover evidence, explain the exact failure mechanism, and propose the smallest generalizable parser rule',
  phases: [{ title: 'Diagnose', detail: 'one agent per failing filing; returns structured diagnosis' }],
}

// args: { inputPath: '<diagnosis_input.json>', libPath: '<shares_lib.py>' }
const A = typeof args === 'string' ? JSON.parse(args) : args
const { failures, libPath } = A
if (!Array.isArray(failures) || !failures.length || !libPath) {
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

const PROMPT = (f) => `You are diagnosing ONE golden-check failure of a shares-outstanding cover-page extractor for SEC 10-K filings. Do NOT edit any files — return a structured diagnosis only.

THE FAILING FILING:
${JSON.stringify(f, null, 1)}

- "truth" is the adversarially-audited golden answer, formatted "number/share_type/as_of_date" per share class.
- "ext" is what the extractor currently returns, same format.
- "status" tells you the kind of mismatch: FAIL_EXTRA_NUMBER = extractor reported number(s) not in the truth; FAIL_MISS_NUMBER = a truth number is missing from the extraction; FAIL_MISS = extractor returned nothing; FAIL_DATE = right numbers, wrong as-of date; FAIL_TYPE = right numbers, wrong share_type.

EVIDENCE: read ${f.cover_path}
(full cover region as the parser sees it + every "outstanding" context in the document).

THE EXTRACTOR: read the 10-K extraction logic in ${libPath} (the cover_region / extract_10k / decoy-skip / _nearest_date / classify_share_type functions) so your diagnosis names the actual mechanism, not a guess.

Diagnose:
1. Find the exact cover sentence(s) where the golden truth appears (quote verbatim).
2. Explain the precise failure mechanism — which truth/extra number, which text pattern, which parser rule misfires or is missing.
3. Propose the SMALLEST generalizable rule that fixes it. It must not overfit to this one company, and must be safe against the regression set (861 currently-passing filings).
4. Assign a pattern_key so failures with the same root cause group together.`

phase('Diagnose')

const out = await parallel(
  failures.map((f) => () =>
    // model policy: code-reasoning diagnosis runs on sonnet, never the
    // inherited Fable tier (fleet of it kills the plan budget)
    agent(PROMPT(f), { label: `diag:${f.company.slice(0, 22)}`, phase: 'Diagnose', schema: SCHEMA, model: 'sonnet' })
  )
)

const ok = out.filter(Boolean)
log(`diagnosed ${ok.length}/${failures.length} failures`)
return ok
