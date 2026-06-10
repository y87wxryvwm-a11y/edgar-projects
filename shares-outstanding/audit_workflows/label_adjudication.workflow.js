export const meta = {
  name: 'shares-label-adjudication',
  description: 'Adjudicate disputed share-class labels: for each filing, rule the correct Class/Series designator of every share count from the full cover text',
  phases: [{ title: 'Adjudicate', detail: 'one sonnet agent per disputed filing; writes one ruling file per filing' }],
}

// args: { disputesPath, count, resultsDir } — each agent reads its own dispute
// (by index) from disputesPath, so the dispute list never travels through args.
const A = typeof args === 'string' ? JSON.parse(args) : args
const { disputesPath, count, resultsDir } = A
if (!disputesPath || !count || !resultsDir) {
  throw new Error('bad args: ' + JSON.stringify(args).slice(0, 200))
}

const PROMPT = (i) => `You are the ADJUDICATOR for disputed SHARE-CLASS LABELS on the cover of one SEC filing. The extractor and the golden table disagree on which class designator belongs to which share count. The golden labels were NEVER audited for this filing (they may carry an old parser bug), so do not defer to either side — rule from the cover text alone.

Read the JSON array at ${disputesPath} and take element index ${i} (0-based). It has {accession, company, form, disputed, extractor_classes, golden_classes, cover_path}.

Read the FULL cover evidence at its cover_path.

For EVERY share count stated as issued/outstanding on the cover, give the class label EXACTLY as the cover prints it for THAT number ("Class B ordinary shares", "Series A Preferred Stock", or "common stock" when the cover names no class). Match each number to its own label — in lists like "N1 Class A ... and N2 Class B ..." the label follows its number; in tables like "Class A Common Stock ... N1" the label precedes it. A parenthetical between number and label (e.g. "(post-reverse stock split adjusted to X)") belongs to the number before it.

Then WRITE your ruling to ${resultsDir}/<accession>.json using the Write tool:
{"accession":"<accession>","classes":[
  {"number":12345,"share_class":"<label exactly as printed>","share_type":"common|ordinary|preferred|depositary|other"}
],"evidence":"<the exact cover sentence(s), <=500 chars>"}
Include EVERY cover share class exactly once. After the Write succeeds, reply with one line: "DONE <accession>".`

phase('Adjudicate')

const res = await parallel(
  Array.from({ length: count }, (_, i) => () =>
    // model policy: adjudication runs on sonnet, never the main-loop tier
    agent(PROMPT(i), { label: `label:${i}`, phase: 'Adjudicate', model: 'sonnet' })
  )
)
const returned = res.filter(Boolean).length
log(`label adjudication: ${returned}/${count} agents returned`)
return { attempted: count, returned }
