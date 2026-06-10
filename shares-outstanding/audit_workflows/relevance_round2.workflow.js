export const meta = {
  name: 'shares-relevance-round2',
  description: 'Adjudicate every relevance disagreement between the two blind classification passes: a fresh agent reads the full cover and issues a definitive ruling',
  phases: [{ title: 'Adjudicate', detail: 'one agent per disputed batch; sees both prior verdicts plus the full cover evidence' }],
}

// args: { batchesDir, resultsDir, ids: [int, ...] }
const A = typeof args === 'string' ? JSON.parse(args) : args
const { batchesDir, resultsDir, ids } = A
if (!Array.isArray(ids) || !ids.length || !batchesDir || !resultsDir) {
  throw new Error('bad args: ' + JSON.stringify(args))
}
const pad = (i) => String(i).padStart(3, '0')

const PROMPT = (id, batchPath, outPath) => `You are the ADJUDICATOR for a study-relevance classification of SEC annual-report registrants (10-K / 20-F / 40-F). Two independent auditors disagreed (or were undeterminable) on the filings in this batch; you issue the DEFINITIVE ruling for each.

Read the batch file at this absolute path:
${batchPath}
It is a JSON array; each entry has {accession, company, cik, cover_path, verdict_A, verdict_B} — the two prior verdicts may be null (a missing read).

For EACH filing:
1. Read cover_path (the full cover region + every "outstanding" context).
2. Weigh both prior verdicts, but decide from the evidence — neither auditor outranks you.

The study covers SHARES OUTSTANDING of corporate equity issuers. Categories:
- "RELEVANT" — a CORPORATION (Inc., Corp., Ltd., PLC, REIT corporation, SPAC, holding company...) with at least one class of common/ordinary equity SHARES outstanding held by public investors — exchange-listed OR registered with outside shareholders even if non-traded (non-traded REITs are RELEVANT).
- "NOT_RELEVANT_ABS" — securitization vehicle (CMBS/RMBS trusts, receivables owner trusts and depositor LLCs, utility cost-recovery funding LLCs, structured-products repackaging trusts). Takes precedence over the other NOT_RELEVANT categories.
- "NOT_RELEVANT_UNITS" — NON-corporate registrant (LP, LLC, statutory/grantor/common-law trust) whose equity is UNITS / LP or membership interests / beneficial interests in a fund or pool — MLPs with public common units, fund LPs/LLCs, royalty trusts, commodity/currency/crypto ETF trusts (their "shares" are beneficial-interest units, not corporate stock).
- "NOT_RELEVANT_DEBT_ONLY" — NO public equity at all; files for registered debt or similar: wholly-owned subsidiaries (all stock parent-held — even when a share count is printed), member-owned cooperatives / Federal Home Loan Banks, mutual companies.
Foreign-issuer rules (20-F / 40-F): a foreign private issuer or Canadian MJDS issuer whose equity is common/ordinary shares (including shares represented by ADSs) held by public investors is RELEVANT — ADS structures do not make it a unit issuer. Foreign funds/unit trusts with units => NOT_RELEVANT_UNITS; foreign subsidiary debt issuers or covered-bond vehicles with no public equity => NOT_RELEVANT_DEBT_ONLY.
Edge rules: SPACs are corporations with shares => RELEVANT. An OPERATING company organized as a trust with publicly traded "common shares of beneficial interest" => RELEVANT, borderline=true. A POOLED INVESTMENT vehicle (fund/BDC) organized as a non-corporate trust/LP/LLC => NOT_RELEVANT_UNITS even when its interests are labelled "shares", borderline=true. Judge legal form from the exact registrant name and the jurisdiction-of-incorporation line, not the company's brand name.

Then WRITE your rulings to this absolute path using the Write tool:
${outPath}
EXACT JSON shape:
{"batch_id":"${id}","results":[
  {"accession":"<accession>","company":"<company>",
   "category":"RELEVANT|NOT_RELEVANT_ABS|NOT_RELEVANT_UNITS|NOT_RELEVANT_DEBT_ONLY",
   "registrant_kind":"corporation|lp|llc|trust|bank_coop|other",
   "borderline":false,
   "confidence":"high|medium|low",
   "agrees_with":"A|B|neither",
   "evidence":"<the exact cover text that settles it, <=400 chars>"}
]}
Include EVERY filing of the batch exactly once. UNDETERMINABLE is NOT available to you — you must rule. After the Write succeeds, reply with one line: "DONE ${id}".`

phase('Adjudicate')

const res = await parallel(
  ids.map((i) => () => {
    const id = pad(i)
    // model policy: adjudication needs judgment but not the main-loop model —
    // sonnet, never the inherited Fable tier
    return agent(PROMPT(id, `${batchesDir}/batch_${id}.json`, `${resultsDir}/batch_${id}.json`), {
      label: `adjudicate:${id}`,
      phase: 'Adjudicate',
      model: 'sonnet',
    })
  })
)
const returned = res.filter(Boolean).length
log(`relevance round 2: attempted ${ids.length} batches, ${returned} returned`)
return { attempted: ids.length, returned }
