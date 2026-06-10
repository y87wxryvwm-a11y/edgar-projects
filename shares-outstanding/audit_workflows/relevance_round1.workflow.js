export const meta = {
  name: 'shares-relevance-round1',
  description: 'Two independent blind classification passes (A/B) deciding study relevance for each sampled 10-K registrant; each agent writes its verdicts to disk',
  phases: [{ title: 'Classify', detail: 'two agents per batch (pass A and pass B), each reads the cover evidence and categorizes every filing' }],
}

// args: { batchesDir, resultsDir, work: [{pass:'A', ids:[int,...]}, ...] }
const A = typeof args === 'string' ? JSON.parse(args) : args
const { batchesDir, resultsDir, work } = A
if (!Array.isArray(work) || !work.length || !batchesDir || !resultsDir) {
  throw new Error('bad args: ' + JSON.stringify(args))
}
const pad = (i) => String(i).padStart(3, '0')

const DEFINITIONS = `The study covers SHARES OUTSTANDING of corporate equity issuers. Classify each 10-K registrant into exactly one category:

- "RELEVANT" — the registrant is a CORPORATION (Inc., Corp., Co., Ltd., PLC, S.A., N.V., bancorp, REIT incorporated as a corporation, SPAC, holding company...) with at least one class of common/ordinary equity SHARES outstanding that is held by public investors — exchange-listed OR registered with outside/public shareholders even if not listed (non-traded REITs and other 12(g) registrants are RELEVANT).
- "NOT_RELEVANT_ABS" — asset-backed-securities / securitization vehicle: CMBS or RMBS mortgage trusts, auto/equipment/consumer receivables owner trusts and their depositor LLCs, utility cost-recovery/securitization funding LLCs (e.g. wildfire or storm recovery bond issuers), structured-products repackaging trusts (STRATS, CORTS, IndexPlus...). Covers typically show "issuing entity / depositor / sponsor" blocks and no equity discussion. This category takes precedence over the other NOT_RELEVANT categories.
- "NOT_RELEVANT_UNITS" — NON-corporate registrant (limited partnership, LLC, statutory/grantor/common-law trust) whose equity securities are UNITS, limited-partner or membership interests, or beneficial interests in a fund/pool. Includes: MLPs with publicly traded common units (still NOT relevant — units, not shares), private fund LPs/LLCs, royalty trusts, and commodity/currency/crypto ETF trusts (their "shares" are units of fractional undivided beneficial interest in a trust, NOT corporate stock).
- "NOT_RELEVANT_DEBT_ONLY" — the registrant has NO public equity at all and files because of registered debt or similar: wholly-owned subsidiaries whose stock is 100% held by the parent (a cover line like "all of the registrant's outstanding common stock is held by X Corp" means DEBT_ONLY even though a share count is printed), member-owned cooperatives and Federal Home Loan Banks (capital stock held only by member institutions, no public market), mutual companies.
- "UNDETERMINABLE" — use rarely, only when the evidence is genuinely insufficient.

Edge rules:
- SPACs are corporations: their listed "units" are bundles of Class A shares + warrants/rights, and the registrant has common/ordinary shares outstanding => RELEVANT.
- An OPERATING company organized as a trust whose public security is "common shares of beneficial interest" (e.g. some exchange-listed REIT trusts) => RELEVANT, but set "borderline": true.
- A POOLED INVESTMENT vehicle (fund/BDC) organized as a non-corporate trust/LP/LLC => NOT_RELEVANT_UNITS even when its interests are labelled "shares"; set "borderline": true when they are labelled shares.
- Check the suffix of the registrant's exact legal name and the "(State or other jurisdiction of incorporation)" line; "...Trust", "...L.P.", "...LLC", "...Fund" names are strong but not conclusive signals — e.g. "Ashford Hospitality Trust, Inc." IS a corporation.`

const SCHEMA_TEXT = (id) => `{"batch_id":"${id}","results":[
  {"accession":"<accession>","company":"<company>",
   "category":"RELEVANT|NOT_RELEVANT_ABS|NOT_RELEVANT_UNITS|NOT_RELEVANT_DEBT_ONLY|UNDETERMINABLE",
   "registrant_kind":"corporation|lp|llc|trust|bank_coop|other",
   "equity_kind":"shares|units|beneficial_interests|membership_interests|none|other",
   "public_equity":true,
   "borderline":false,
   "confidence":"high|medium|low",
   "evidence":"<the exact cover text you relied on (registrant name line, security descriptions, 'held by parent' lines...), <=400 chars>"}
]}`

const PROMPT_A = (id, batchPath, outPath) => `You are an INDEPENDENT auditor classifying SEC 10-K registrants for study RELEVANCE. You decide from the cover evidence alone.

Read the batch file at this absolute path:
${batchPath}
It is a JSON array of filings; each has {accession, company, cik, form, cover_path}.

For EACH filing, read its cover_path (full cover region + every "outstanding" context) and classify it.

${DEFINITIONS}

Then WRITE your verdicts to this absolute path using the Write tool:
${outPath}
EXACT JSON shape:
${SCHEMA_TEXT(id)}
Include EVERY filing of the batch exactly once, in order. Be meticulous: a wrong RELEVANT/NOT_RELEVANT call corrupts the study sample. After the Write succeeds, reply with one line: "DONE ${id}".`

const PROMPT_B = (id, batchPath, outPath) => `You are an INDEPENDENT auditor classifying SEC 10-K registrants for study RELEVANCE. Work CHECKLIST-FIRST for each filing — answer these in order, from the cover evidence alone, before you pick a category:
1. LEGAL FORM: from the registrant's exact name and the "(State or other jurisdiction of incorporation or organization)" line — corporation, LP, LLC, or trust?
2. EQUITY SECURITIES: what equity does the registrant itself have outstanding, and what is it called on the cover — shares of stock, units, beneficial/membership interests, or none?
3. PUBLIC HOLDERS: is any of that equity held by the public (listed on an exchange, or registered with outside shareholders), or is it all held by a parent / member institutions?
4. SECURITIZATION: is the registrant an asset-backed issuing entity / depositor / funding vehicle (issuing-entity+depositor+sponsor cover blocks, receivables/mortgage pool, recovery bonds)?
Only then assign the category.

Read the batch file at this absolute path:
${batchPath}
It is a JSON array of filings; each has {accession, company, cik, form, cover_path}. For EACH filing, read its cover_path.

${DEFINITIONS}

Then WRITE your verdicts to this absolute path using the Write tool:
${outPath}
EXACT JSON shape:
${SCHEMA_TEXT(id)}
Include EVERY filing of the batch exactly once, in order. Be meticulous: a wrong RELEVANT/NOT_RELEVANT call corrupts the study sample. After the Write succeeds, reply with one line: "DONE ${id}".`

phase('Classify')

const jobs = []
for (const { pass: p, ids } of work) {
  for (const i of ids) {
    const id = pad(i)
    const batchPath = `${batchesDir}/batch_${id}.json`
    const outPath = `${resultsDir}/${p}/batch_${id}.json`
    jobs.push(() =>
      // model policy: bulk classification runs on haiku — NEVER the inherited
      // main-loop model (a Fable-tier fleet exhausts the plan budget instantly)
      agent((p === 'A' ? PROMPT_A : PROMPT_B)(id, batchPath, outPath), {
        label: `classify:${p}${id}`,
        phase: 'Classify',
        model: 'haiku',
      })
    )
  }
}

const res = await parallel(jobs)
const returned = res.filter(Boolean).length
log(`relevance round 1: attempted ${jobs.length} agent runs, ${returned} returned (verify result files on disk)`)
return { attempted: jobs.length, returned }
