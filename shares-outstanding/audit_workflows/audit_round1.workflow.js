export const meta = {
  name: 'shares-audit-round1',
  description: 'Independent blind adversarial audit of shares-outstanding extractions (ground truth from neutral evidence packets); each agent writes its verdicts to disk',
  phases: [{ title: 'Audit', detail: 'one agent per batch; reads evidence, decides truth, writes verdicts file' }],
}

// args: { batchesDir, resultsDir, ids: [int, ...] }  — only the listed batch ids are run (resumable).
const A = typeof args === 'string' ? JSON.parse(args) : args
const { batchesDir, resultsDir, ids } = A
if (!Array.isArray(ids) || !ids.length || !batchesDir || !resultsDir) {
  throw new Error('bad args: ' + JSON.stringify(args))
}
const pad = (i) => String(i).padStart(3, '0')

const PROMPT = (id, batchPath, outPath) => `You are an INDEPENDENT, ADVERSARIAL auditor establishing GROUND TRUTH for the "shares issued and outstanding" reported on the cover pages of SEC annual filings (10-K / 20-F / 40-F). You do NOT see any extractor's output — you decide the truth yourself, from the evidence only.

Read the batch file at this absolute path:
${batchPath}
It is a JSON array of filings; each has {accession, form, company, cik, evidence_path}.

For EACH filing in the batch:
1. Read its evidence_path (an absolute path). The neutral packet contains: a header (FORM, DOC_TYPE, PERIOD_OF_REPORT), the COVER REGION text, every "outstanding" context found in the document, and SEC's structured dei:EntityCommonStockSharesOutstanding fact.
2. From this evidence ALONE, determine the shares ISSUED AND OUTSTANDING stated on the filing's COVER PAGE — for EACH share class: the number, the class label as printed, the share_type, and the as-of date.

Rules:
- Report the number AS PRINTED IN THE COVER PROSE. The dei XBRL fact is only a cross-check; if prose and XBRL differ, trust the cover prose number and say so in notes.
- MULTIPLE CLASSES (e.g. Class A/B/C common; or ordinary plus several preferred series) => report EVERY distinct class as its own entry. Never merge or drop a class. But do NOT split one class into duplicates.
- TRUE_NEGATIVE = the filing genuinely has NO common/ordinary shares outstanding: asset-backed-securities trusts (CMBS, auto-loan / owner trusts), commodity pools, and similar structured issuers whose cover states no share count. verdict="TRUE_NEGATIVE", classes=[].
- UNDETERMINABLE = the evidence is genuinely insufficient (fetch error / empty packet). Use rarely.
- DECOYS — never report these as the outstanding count: AUTHORIZED shares; TREASURY shares; WEIGHTED-AVERAGE shares (EPS); balance-sheet share counts stated in THOUSANDS; the $ aggregate-market-value line; record-holder / holders-of-record counts; WARRANTS; shares of a different registrant/subsidiary than the filer; a parenthetical SUBSET ("including X ADSs").
- as_of_date: ISO YYYY-MM-DD, taken from the "as of <date>" next to the count. For 20-F/40-F with no inline date, use PERIOD_OF_REPORT (fiscal close). If truly none, "".
- share_type in {common, ordinary, preferred, depositary, other}. US domestic issuer => "common"; foreign private issuer (20-F) => "ordinary"; preferred/preference => "preferred"; ADS/ADR/depositary => "depositary".

Then WRITE your verdicts to this absolute path using the Write tool:
${outPath}
EXACT JSON shape (numbers are integers, NO commas, NO quotes around numbers):
{"batch_id":"${id}","results":[
  {"accession":"<accession>","form":"<form>","company":"<company>","verdict":"HAS_SHARES",
   "classes":[{"number":77565827,"share_class":"common stock","share_type":"common","as_of_date":"2025-02-14"}],
   "confidence":"high","notes":"<the exact cover sentence you relied on, or why TRUE_NEGATIVE>"}
]}
Include EVERY filing of the batch exactly once, in order. For TRUE_NEGATIVE/UNDETERMINABLE use "classes":[].

Be meticulous: this audit is the sole gate on a 100%-accuracy claim. Writing the file is your deliverable. After the Write succeeds, reply with one line: "DONE ${id}".`

phase('Audit')

const res = await parallel(
  ids.map((i) => () => {
    const id = pad(i)
    const batchPath = `${batchesDir}/batch_${id}.json`
    const outPath = `${resultsDir}/batch_${id}.json`
    return agent(PROMPT(id, batchPath, outPath), { label: `audit:${id}`, phase: 'Audit' })
  })
)

const returned = res.filter(Boolean).length
log(`audit pass: attempted ${ids.length} batches, ${returned} agents returned (verify result files on disk)`)
return { attempted: ids.length, returned }
