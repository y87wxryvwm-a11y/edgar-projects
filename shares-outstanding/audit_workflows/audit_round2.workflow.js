export const meta = {
  name: 'shares-audit-round2',
  description: 'Adjudicate audit-vs-extractor disagreements against the authoritative FULL cover; deliver definitive ground truth + who-is-correct + root cause',
  phases: [{ title: 'Adjudicate', detail: 'one agent per batch of disagreements; full cover / live filing' }],
}

// args: { batchesDir, resultsDir, sharesDir, ids:[int,...] } — agents read their own batch file (resumable).
const A = typeof args === 'string' ? JSON.parse(args) : args
const { batchesDir, resultsDir, sharesDir, ids } = A
if (!Array.isArray(ids) || !ids.length || !batchesDir || !resultsDir || !sharesDir) {
  throw new Error('bad args: ' + JSON.stringify(A).slice(0, 300))
}
const pad = (i) => String(i).padStart(3, '0')

const DEFN = `GROUND-TRUTH DEFINITION (apply EXACTLY and CONSISTENTLY):
- The answer is the count(s) of the issuer's OUTSTANDING equity interests as STATED IN THE COVER-PAGE PROSE of this filing: common stock, ordinary shares, every class of capital stock (incl. each preferred/preference series), AND units of beneficial interest / limited-partnership units / trust units when those ARE the issuer's outstanding equity-like interest (commodity pools, statutory trusts, LPs). Report EACH distinct class as its own entry.
- TRUE_NEGATIVE only when the cover genuinely reports NO outstanding equity interest: bankruptcy-remote asset-backed issuers (CMBS / auto-loan / owner trusts) that state no share/unit count, OR a cover whose share-count line is "N/A" / "Not Applicable" / "None" / blank with no number in the cover prose. A number that exists ONLY in XBRL but is NOT printed in the cover prose does NOT make it HAS_SHARES — the faithful cover extraction is empty (note this).
- NUMBER: exactly as printed in the cover prose (integer, no commas). If a grand TOTAL and its component classes are both printed, report the COMPONENT classes, NOT the redundant total.
- MULTI-REGISTRANT combined filing (e.g. Exelon + ComEd + PECO): the answer is ONLY the shares of the PRIMARY registrant whose filing this is (match the company/CIK). Other co-registrants' counts are NOT part of this filing's answer.
- DATE: the date the cover ties to the count. For 10-K, that is the recent "as of <practicable date>" next to the share count, NOT the fiscal year-end unless that is what is printed. For 20-F/40-F use the printed date, else the fiscal period end (PERIOD_OF_REPORT).
- DECOYS that are never the answer: authorized; treasury; weighted-average (EPS); balance-sheet figures in thousands; the $ market-value line; record/registered-holder counts; warrants/options; a parenthetical SUBSET ("including X ADSs"); shares of a different registrant.
- share_type in {common, ordinary, preferred, depositary, other}. Trust/LP units => "other" unless labelled depositary. Foreign equity => "ordinary"; US equity => "common".`

const PROMPT = (id, batchPath, outPath) => `You are the FINAL ADJUDICATOR. Read the batch of disagreements at:
${batchPath}
It is a JSON array; each item has {accession, cik, form, company, status, detail, ext, aud, aud_notes, evidence_path, txt_url}. "ext" is what the project's extractor produced; "aud" is what an independent round-1 auditor said; they disagree. Establish the DEFINITIVE truth for the COVER-PAGE "shares issued and outstanding" of EACH filing, then rule on who was correct. Your ruling overrides both parties. Be rigorous and consistent.

${DEFN}

HOW TO ESTABLISH TRUTH for each item:
1. Read its evidence_path packet.
2. ALWAYS confirm against the FULL untruncated cover (the exact text the extractor saw) by running, with the Bash tool, from ${sharesDir}:
     ../.venv/bin/python dump_cover.py <accession>
   This prints the full cover region + every 'outstanding' context, no truncation.
3. Only if still ambiguous, fetch the live filing primary document (txt_url).
Decide the truth from the cover PROSE, applying the definition above.

THEN judge each item:
- who_correct = "EXTRACTOR" if the extractor's set of (number, class, date) equals the truth; "AUDITOR" if the round-1 auditor was right and the extractor wrong; "NEITHER" if both wrong; "BOTH" if equivalent (only a share_type label nuance).
- root_cause: if the extractor is wrong, a short machine tag, e.g.: MISSED_ALL, MISSED_CLASS, WRONG_NUMBER, EXTRA_TOTAL_LINE, EXTRA_COREGISTRANT, EXTRA_SUBSET, EXTRA_DECOY, PICKED_DECOY_AUTHORIZED, PICKED_DECOY_TREASURY, PICKED_DECOY_WEIGHTED, PICKED_DECOY_MARKETVALUE, WRONG_DATE_FISCAL_NOT_PRACTICABLE, WRONG_DATE, WRONG_TYPE, FALSE_POSITIVE_SHOULD_BE_TRUENEG, FALSE_NEGATIVE_IS_REAL, EMPTY_DOCUMENT_PARSE. If the extractor is correct (auditor erred), root_cause = "EXTRACTOR_OK".
- fix_hint: one sentence on the parser rule that would fix it (only when extractor wrong).

WRITE all rulings to this absolute path with the Write tool:
${outPath}
EXACT JSON (numbers are integers, no commas):
{"rulings":[
  {"accession":"<acc>","form":"<form>","company":"<co>",
   "definitive":{"verdict":"HAS_SHARES","classes":[{"number":123,"share_class":"common stock","share_type":"common","as_of_date":"2025-02-14"}]},
   "who_correct":"EXTRACTOR|AUDITOR|NEITHER|BOTH","root_cause":"<tag>","fix_hint":"<one sentence or empty>",
   "evidence_used":"packet|full_cover|live","ruling_basis":"<the exact cover sentence that decides it>"}
]}
One ruling per item, in order, every item exactly once. For TRUE_NEGATIVE use "classes":[].

Writing the file is your deliverable. After the Write succeeds, reply "DONE ${id}".`

phase('Adjudicate')

const res = await parallel(
  ids.map((i) => () => {
    const id = pad(i)
    const batchPath = `${batchesDir}/adjbatch_${id}.json`
    const outPath = `${resultsDir}/adjbatch_${id}.json`
    return agent(PROMPT(id, batchPath, outPath), { label: `adj:${id}`, phase: 'Adjudicate' })
  })
)
const returned = res.filter(Boolean).length
log(`adjudication: attempted ${ids.length} batches, ${returned} agents returned (verify result files on disk)`)
return { attempted: ids.length, returned }
