"""Committed record of every public-float filing whose final rows don't come
from the validated extractor alone — the float analog of overrides.py.

Three tables, consumed by 12_build_final_float.py:

* CONFIRMED — extraction rows are correct as-is; confirmed by independent
  reads (READS_2BLIND_CONFIRMED: two blind haiku readers independently
  reproduced every row; READS_SONNET_CONFIRMED: a sonnet adjudication ruled
  the extraction right, or the extractor was generalized afterward and now
  reproduces the sonnet ruling).
* NO_FLOAT — the cover genuinely discloses no float value (kind + the
  adjudicator's reason).
* OVERRIDES — the extractor's output was wrong or incomplete; rows here
  replace it, each with provenance. Row fields: cover (the cover's stated
  value in plain dollars), xbrl (the tagged value as filed, "" if untagged),
  as_of, label, flags.

Every OVERRIDES and NO_FLOAT entry was adversarially verified by an
independent reader after generation (see progress.md). Nothing here is
consulted at extraction time, so the extractor stays general.

Override rows may carry a 'resolved' field: the verified value when BOTH
as-filed numbers (cover print and XBRL tag) are wrong. 12_build_final_float
publishes it as public_float with float_basis=RESOLVED_FILER_ERROR; the
as-filed values stay in their evidence columns. Every 'resolved' entry's
provenance must record the evidence, including the plausibility smoke test.

Plausibility smoke-test record (2026-06-12, per Evan): every row whose
as-filed value was implausible (>$5T, or implied $/share outside
[0.00005, 800000] against the shares census) was checked against
independent web sources before judgment:
* Twin Vee PowerCats (0001731122-25-000412) — resolved to $5,188,400; see
  the OVERRIDES entry below.
* Graphjet Technology (0001213900-25-125488) — cover prints "computed by
  reference to the closing sales price of $6.00 ... was $6.00": the filer
  printed its share price where the aggregate belongs, and tagged 6 in
  XBRL too. Web check: GTI was delisted from Nasdaq (Nov 2025) for low
  market value of PUBLICLY HELD shares, so a small float is real but $6.00
  is the price, not the float; the true value appears nowhere in the
  filing, so the row stays as filed, flagged FLOAT_EQUALS_STATED_PRICE +
  AS_FILED_MICRO_VALUE. Not resolvable without inventing a number.
* Sonnet BioTherapeutics (0001493152-25-027978) — "1 share outstanding"
  (Dec 12, 2025) looked impossible but is genuine: the merger into
  Hyperliquid Strategies closed Dec 2, 2025 and every public Sonnet share
  was canceled/converted; float ($4,150,174 at Mar 31, 2025) predates the
  merger. No action.
* The remaining micro-floats (Ultimate Holdings $140, Zentrum $193,
  ATLANTICA $49.17, Adapti $5.60, Greater Cannabis $586.11, SinglePoint,
  Marquie, Edgemode, Two Hands, Cosmos, World Health Energy, Green Planet)
  are genuine as printed: par-value-based or sub-penny-price floats on
  shells/OTC microcaps, each stated and tagged identically by the filer.
  They keep AS_FILED_MICRO_VALUE flags.
"""

CONFIRMED = {
    "0000004904-25-000027": "READS_SONNET_CONFIRMED",
    "0001423902-25-000033": "READS_SONNET_CONFIRMED",
    "0001437749-25-008584": "READS_2BLIND_CONFIRMED",
    "0000042888-25-000011": "READS_2BLIND_CONFIRMED",
    "0000051434-25-000013": "READS_2BLIND_CONFIRMED",
    "0000714562-25-000010": "READS_2BLIND_CONFIRMED",
    # 0000790816-25-000009 (Brandywine) moved to OVERRIDES 2026-06-12: the
    # blind reads confirmed both values, but the unit float needed its
    # registrant attribution (Brandywine Operating Partnership, L.P.).
    "0000918545-25-000002": "READS_2BLIND_CONFIRMED",
    "0000922224-25-000009": "READS_2BLIND_CONFIRMED",
    "0000927089-25-000061": "READS_2BLIND_CONFIRMED",
    "0000927089-25-000064": "READS_2BLIND_CONFIRMED",
    "0000950170-25-014412": "READS_2BLIND_CONFIRMED",
    "0000950170-25-018873": "READS_2BLIND_CONFIRMED",
    "0000950170-25-022244": "READS_2BLIND_CONFIRMED",
    "0000950170-25-023116": "READS_2BLIND_CONFIRMED",
    "0000950170-25-023344": "READS_2BLIND_CONFIRMED",
    "0000950170-25-024151": "READS_2BLIND_CONFIRMED",
    "0000950170-25-024488": "READS_2BLIND_CONFIRMED",
    "0000950170-25-024533": "READS_2BLIND_CONFIRMED",
    "0000950170-25-024839": "READS_2BLIND_CONFIRMED",
    "0000950170-25-026131": "READS_2BLIND_CONFIRMED",
    "0000950170-25-027602": "READS_2BLIND_CONFIRMED",
    "0000950170-25-027778": "READS_2BLIND_CONFIRMED",
    "0000950170-25-027896": "READS_2BLIND_CONFIRMED",
    "0000950170-25-028337": "READS_2BLIND_CONFIRMED",
    "0000950170-25-028533": "READS_2BLIND_CONFIRMED",
    "0000950170-25-029052": "READS_2BLIND_CONFIRMED",
    "0000950170-25-029068": "READS_2BLIND_CONFIRMED",
    "0000950170-25-029221": "READS_2BLIND_CONFIRMED",
    "0000950170-25-029514": "READS_2BLIND_CONFIRMED",
    "0000950170-25-029973": "READS_2BLIND_CONFIRMED",
    "0000950170-25-030855": "READS_2BLIND_CONFIRMED",
    "0000950170-25-030894": "READS_2BLIND_CONFIRMED",
    "0000950170-25-031309": "READS_2BLIND_CONFIRMED",
    "0000950170-25-033077": "READS_2BLIND_CONFIRMED",
    "0000950170-25-034183": "READS_2BLIND_CONFIRMED",
    "0000950170-25-034903": "READS_2BLIND_CONFIRMED",
    "0000950170-25-035158": "READS_2BLIND_CONFIRMED",
    "0000950170-25-035940": "READS_2BLIND_CONFIRMED",
    "0000950170-25-038044": "READS_2BLIND_CONFIRMED",
    "0000950170-25-038751": "READS_2BLIND_CONFIRMED",
    "0000950170-25-038826": "READS_2BLIND_CONFIRMED",
    "0000950170-25-039308": "READS_2BLIND_CONFIRMED",
    "0000950170-25-042159": "READS_2BLIND_CONFIRMED",
    "0000950170-25-043234": "READS_2BLIND_CONFIRMED",
    "0000950170-25-045242": "READS_2BLIND_CONFIRMED",
    "0000950170-25-045909": "READS_2BLIND_CONFIRMED",
    "0000950170-25-046444": "READS_2BLIND_CONFIRMED",
    "0000950170-25-046840": "READS_2BLIND_CONFIRMED",
    "0000950170-25-046884": "READS_2BLIND_CONFIRMED",
    "0000950170-25-046927": "READS_2BLIND_CONFIRMED",
    "0000950170-25-047570": "READS_2BLIND_CONFIRMED",
    "0000950170-25-047645": "READS_2BLIND_CONFIRMED",
    "0000950170-25-047695": "READS_2BLIND_CONFIRMED",
    "0000950170-25-047835": "READS_2BLIND_CONFIRMED",
    "0000950170-25-051566": "READS_2BLIND_CONFIRMED",
    "0000950170-25-070705": "READS_2BLIND_CONFIRMED",
    "0000950170-25-077746": "READS_2BLIND_CONFIRMED",
    "0000950170-25-111682": "READS_2BLIND_CONFIRMED",
    "0001013762-25-002744": "READS_2BLIND_CONFIRMED",
    "0001062993-25-006510": "READS_2BLIND_CONFIRMED",
    "0001062993-25-016369": "READS_2BLIND_CONFIRMED",
    "0001079973-25-000540": "READS_2BLIND_CONFIRMED",
    "0001079973-25-001836": "READS_2BLIND_CONFIRMED",
    "0001104659-25-025551": "READS_2BLIND_CONFIRMED",
    "0001104659-25-025600": "READS_2BLIND_CONFIRMED",
    "0001104659-25-026133": "READS_2BLIND_CONFIRMED",
    "0001104659-25-026134": "READS_2BLIND_CONFIRMED",
    "0001104659-25-029944": "READS_2BLIND_CONFIRMED",
    "0001104659-25-094578": "READS_2BLIND_CONFIRMED",
    "0001139020-25-000063": "READS_2BLIND_CONFIRMED",
    "0001139020-25-000067": "READS_2BLIND_CONFIRMED",
    "0001140361-25-047052": "READS_2BLIND_CONFIRMED",
    "0001178913-25-000605": "READS_2BLIND_CONFIRMED",
    "0001185185-25-000760": "READS_2BLIND_CONFIRMED",
    "0001193125-25-034579": "READS_2BLIND_CONFIRMED",
    "0001193125-25-054447": "READS_2BLIND_CONFIRMED",
    "0001193125-25-064109": "READS_2BLIND_CONFIRMED",
    "0001193125-25-065546": "READS_2BLIND_CONFIRMED",
    "0001193125-25-194119": "READS_2BLIND_CONFIRMED",
    "0001193125-25-222224": "READS_2BLIND_CONFIRMED",
    "0001193125-25-292412": "READS_2BLIND_CONFIRMED",
    "0001193125-25-297510": "READS_2BLIND_CONFIRMED",
    "0001193125-25-297570": "READS_2BLIND_CONFIRMED",
    "0001193125-25-297573": "READS_2BLIND_CONFIRMED",
    "0001199835-25-000103": "READS_2BLIND_CONFIRMED",
    "0001199835-25-000233": "READS_2BLIND_CONFIRMED",
    "0001199835-25-000302": "READS_2BLIND_CONFIRMED",
    "0001213900-25-023259": "READS_2BLIND_CONFIRMED",
    "0001213900-25-031639": "READS_2BLIND_CONFIRMED",
    "0001213900-25-032210": "READS_2BLIND_CONFIRMED",
    "0001213900-25-075991": "READS_2BLIND_CONFIRMED",
    "0001214659-25-005928": "READS_2BLIND_CONFIRMED",
    "0001262463-25-000185": "READS_2BLIND_CONFIRMED",
    "0001330399-25-000010": "READS_2BLIND_CONFIRMED",
    "0001331754-25-000063": "READS_2BLIND_CONFIRMED",
    "0001401521-25-000032": "READS_2BLIND_CONFIRMED",
    "0001410578-25-000354": "READS_2BLIND_CONFIRMED",
    "0001413447-25-000019": "READS_2BLIND_CONFIRMED",
    "0001437749-25-001189": "READS_2BLIND_CONFIRMED",
    "0001437749-25-007257": "READS_2BLIND_CONFIRMED",
    "0001437749-25-007492": "READS_2BLIND_CONFIRMED",
    "0001437749-25-007734": "READS_2BLIND_CONFIRMED",
    "0001437749-25-023703": "READS_2BLIND_CONFIRMED",
    "0001437749-25-028574": "READS_2BLIND_CONFIRMED",
    "0001474903-25-000016": "READS_2BLIND_CONFIRMED",
    "0001477932-25-001990": "READS_2BLIND_CONFIRMED",
    "0001477932-25-002248": "READS_2BLIND_CONFIRMED",
    "0001477932-25-002666": "READS_2BLIND_CONFIRMED",
    "0001477932-25-003933": "READS_2BLIND_CONFIRMED",
    "0001477932-25-003940": "READS_2BLIND_CONFIRMED",
    "0001477932-25-006766": "READS_2BLIND_CONFIRMED",
    "0001493152-25-007948": "READS_2BLIND_CONFIRMED",
    "0001493152-25-016069": "READS_2BLIND_CONFIRMED",
    "0001493152-25-018188": "READS_2BLIND_CONFIRMED",
    "0001493152-25-029094": "READS_2BLIND_CONFIRMED",
    "0001558370-25-001926": "READS_2BLIND_CONFIRMED",
    "0001558370-25-003015": "READS_2BLIND_CONFIRMED",
    "0001558370-25-003867": "READS_2BLIND_CONFIRMED",
    "0001580670-25-000016": "READS_2BLIND_CONFIRMED",
    "0001599916-25-000053": "READS_2BLIND_CONFIRMED",
    "0001628280-25-008367": "READS_2BLIND_CONFIRMED",
    "0001641172-25-001183": "READS_2BLIND_CONFIRMED",
    "0001641172-25-001201": "READS_2BLIND_CONFIRMED",
    "0001641172-25-001291": "READS_2BLIND_CONFIRMED",
    "0001641172-25-001741": "READS_2BLIND_CONFIRMED",
    "0001641172-25-004846": "READS_2BLIND_CONFIRMED",
    "0001641172-25-005487": "READS_2BLIND_CONFIRMED",
    "0001641172-25-015290": "READS_2BLIND_CONFIRMED",
    "0001641172-25-017758": "READS_SONNET_CONFIRMED",
    "0001654954-25-000915": "READS_2BLIND_CONFIRMED",
    "0001654954-25-003443": "READS_2BLIND_CONFIRMED",
    "0001654954-25-012437": "READS_2BLIND_CONFIRMED",
    "0001683168-25-000285": "READS_2BLIND_CONFIRMED",
    "0001683168-25-001163": "READS_2BLIND_CONFIRMED",
    "0001683168-25-001429": "READS_SONNET_CONFIRMED",
    "0001683168-25-001460": "READS_SONNET_CONFIRMED",
    "0001683168-25-001497": "READS_SONNET_CONFIRMED",
    "0001683168-25-001534": "READS_SONNET_CONFIRMED",
    "0001683168-25-001574": "READS_SONNET_CONFIRMED",
    "0001683168-25-001606": "READS_SONNET_CONFIRMED",
    "0001683168-25-001706": "READS_2BLIND_CONFIRMED",
    "0001683168-25-001708": "READS_2BLIND_CONFIRMED",
    "0001683168-25-002556": "READS_2BLIND_CONFIRMED",
    "0001684682-25-000005": "READS_2BLIND_CONFIRMED",
    "0001702744-25-000046": "READS_2BLIND_CONFIRMED",
    "0001712543-25-000016": "READS_2BLIND_CONFIRMED",
    "0001722482-25-000015": "READS_2BLIND_CONFIRMED",
    "0001753926-25-001384": "READS_2BLIND_CONFIRMED",
    "0001753926-25-001663": "READS_2BLIND_CONFIRMED",
    "0001753926-25-002005": "READS_SONNET_CONFIRMED",
    "0001755672-25-000005": "READS_2BLIND_CONFIRMED",
    "0001826000-25-000035": "READS_2BLIND_CONFIRMED",
    "0001826000-25-000076": "READS_2BLIND_CONFIRMED",
}

NO_FLOAT = {
    "0000950170-25-029405": 'NA_STATED; sonnet adjudication: The cover explicitly states the aggregate market value of non-affiliate shares is "N/A"; the extractor incorrectly captured the shares outstanding count (93,759,963) as a dollar float value.',
    "0000950170-25-029407": 'NA_STATED; sonnet adjudication: Cover line 59 explicitly states "N/A" for aggregate market value of shares held by non-affiliates; the extractor\'s value is the shares-outstanding count from line 60, not a float dollar amount.',
    "0000950170-25-039374": 'NA_STATED; sonnet adjudication: Cover explicitly states "NoT APPLICABLE" for the public float field; the extractor mistakenly captured the shares-outstanding count as a USD float value.',
    "0000950170-25-039452": 'NA_STATED; sonnet adjudication: The cover explicitly states "NoT APPLICABLE" for the aggregate market value of non-affiliate equity; the extractor mistakenly captured the shares-outstanding count as a float value.',
    "0001193125-25-066935": "ABSENT; sonnet adjudication: Form 40-F (foreign private issuer) does not require public float disclosure; the cover states only share count (282,875,928 common shares), and the extractor's rows are audit-fee figures scraped from the filing body, not float values.",
    "0001199835-25-000414": 'NO_PUBLIC_MARKET; sonnet adjudication: Cover explicitly states common stock was not publicly traded as of the second fiscal quarter end and therefore no aggregate market value can be calculated; the 264,637,563 figure is shares outstanding, not a float dollar value.',
    "0001376474-25-000217": 'ABSENT; sonnet adjudication: The $1.235 billion figure is an EGC revenue disqualification threshold, not a stated aggregate market value of non-affiliate holdings; the cover discloses no public float dollar amount.',
    "0001416265-25-000006": 'NA_STATED; sonnet adjudication: Both registrants\' float cells carry footnote "(a) Not applicable" — no dollar amount is disclosed; the 77,560,839 figure is a share count, not a float value, and the extractor wrongly treated it as USD.',
    "0001641172-25-012733": 'NA_STATED; sonnet adjudication: The cover explicitly states "N/A 0" for aggregate market value held by non-affiliates; the extractor wrongly captured the shares-outstanding count as a float value.',
    "0001812554-25-000009": "NO_PUBLIC_MARKET; sonnet adjudication: The cover explicitly states no established market exists for the shares, so no aggregate market value is provided; the extractor's $0.01 is the par value per share, not a float figure.",
    "0001869453-25-000011": "NO_PUBLIC_MARKET; sonnet adjudication: The cover explicitly states no float because there is no established market for the shares; the extractor's $0.01 is the par value from the shares-outstanding line, not a float disclosure.",
    "0001940243-25-000023": "NO_PUBLIC_MARKET; sonnet adjudication: Cover explicitly states no established public trading market existed as of May 31, 2025, so the float is indeterminable; the extractor's value=0 came from misreading the $0.001 par value as a float figure.",
}

OVERRIDES = {
    "0001109357-25-000043": {'rows': [{'cover': '34615866949', 'xbrl': '34615866949', 'as_of': '2024-06-30', 'label': 'Exelon Corporation Common Stock, without par value', 'flags': ''}, {'cover': '0', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'PECO Energy Company Common Stock, without par value', 'flags': 'NONE_STATED'}, {'cover': '0', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Baltimore Gas and Electric Company, without par value', 'flags': 'NONE_STATED'}, {'cover': '0', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Potomac Electric Power Company', 'flags': 'NONE_STATED'}, {'cover': '0', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Delmarva Power & Light Company', 'flags': 'NONE_STATED'}, {'cover': '0', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Atlantic City Electric Company', 'flags': 'NONE_STATED'}], 'provenance': "sonnet adjudication: the cover's per-registrant table states $34,615,866,949 for Exelon, 'None' (a stated zero) for PECO / BGE / Potomac / Delmarva / Atlantic City, 'No established market' for ComEd and 'Not applicable' for Pepco Holdings (both excluded as no-disclosure, not zeros); verified against the cover text by direct read of the table during diagnostics (adversarial check)."},
    "0001731122-25-000412": {'rows': [{'cover': '5188400000000', 'xbrl': '5188400000', 'resolved': '5188400', 'as_of': '2024-06-28', 'label': 'Common equity (voting and non-voting)', 'flags': 'IMPLAUSIBLE_AS_FILED'}], 'provenance': "Twin Vee PowerCats. The cover literally prints '$5,188,400 million' as of June 28, 2024 ($5.1884T as printed); the filer's own tag says 5,188,400,000 (same mantissa, spurious x1000 scale). Both as-filed readings fail the plausibility smoke test (2026-06-12, Evan-directed): web sources (Yahoo Finance, WallStreetZen, StockTitan) show VEEE market cap = $3.2M (Mar 2026), float = 2M shares, and a Feb 2026 offering that raised $3.0M gross - a $5.19B or $5.19T float is impossible for this issuer. The same cover states 14,874,452 shares outstanding, so the three readings imply $0.35, $349, or $348,855 per share; only the first is in VEEE's sub-dollar Nasdaq range. Judgment call, logged here: the printed digits are the disclosure and the word 'million' is the cover's error (the tag repeats the same mantissa with its own wrong scale), so public_float = 5,188,400 (scale 0). The as-filed values are preserved in public_float_cover / public_float_xbrl."},
    # ---- registrant-attribution corrections (2026-06-12 mapping audit) ----
    # Found by the per-registrant CIK audit (independent sonnet readers over
    # every multi-registrant filing), each verified against the cover text.
    # Root cause in all four: the extractor deduplicates by (value, as_of),
    # so a second registrant's identical/unlabeled value collapsed into one
    # row that fell to the primary filer. Values are unchanged from the
    # validated extraction (cover = tag); these entries add the attribution.
    "0000352541-25-000014": {'rows': [
        {'cover': '13000000000', 'xbrl': '13000000000', 'as_of': '2024-06-30', 'label': 'Alliant Energy Corporation', 'flags': ''},
        {'cover': '0', 'xbrl': '0', 'as_of': '2024-06-30', 'label': 'Interstate Power and Light Company', 'flags': 'ZERO_STATED'},
        {'cover': '0', 'xbrl': '0', 'as_of': '2024-06-30', 'label': 'Wisconsin Power and Light Company', 'flags': 'ZERO_STATED'},
    ], 'provenance': 'mapping audit 2026-06-12, cover-verified: the cover prints a three-line table as of June 30, 2024 - "Alliant Energy Corporation - $13.0 billion / Interstate Power and Light Company - $0 / Wisconsin Power and Light Company - $0". The two subsidiary zeros had collapsed into one unlabeled row attributed to the primary filer.'},
    "0001628280-25-006389": {'rows': [
        {'cover': '2694439281', 'xbrl': '2694439281', 'as_of': '2024-06-28', 'label': 'American States Water Company', 'flags': ''},
        {'cover': '0', 'xbrl': '0', 'as_of': '2024-06-28', 'label': 'Golden State Water Company', 'flags': 'ZERO_STATED'},
    ], 'provenance': 'mapping audit 2026-06-12, cover-verified: "The aggregate market value of all voting stock held by non-affiliates of Golden State Water Company was zero on June 28, 2024." The subsidiary zero had fallen unlabeled to the primary filer (American States Water, whose own float is $2,694,439,281).'},
    "0001628280-25-006706": {'rows': [
        {'cover': '10320640348', 'xbrl': '10320640348', 'as_of': '2024-06-28', 'label': 'Lamar Advertising Company', 'flags': ''},
        {'cover': '0', 'xbrl': '0', 'as_of': '2024-06-28', 'label': 'Lamar Media Corp.', 'flags': 'ZERO_STATED'},
    ], 'provenance': 'mapping audit 2026-06-12, cover-verified: "As of June 28, 2024, the aggregate market value of the voting stock held by nonaffiliates of Lamar Media Corp. was $0." The subsidiary zero had fallen unlabeled (and undated) to the primary filer; the cover dates it June 28, 2024.'},
    "0001130310-25-000040": {'rows': [
        {'cover': '19797614936', 'xbrl': '19797614936', 'as_of': '2024-06-30', 'label': 'CenterPoint Energy, Inc.', 'flags': ''},
        {'cover': '0', 'xbrl': '0', 'as_of': '2024-06-30', 'label': 'CenterPoint Energy Houston Electric, LLC', 'flags': 'NONE_STATED'},
        {'cover': '0', 'xbrl': '0', 'as_of': '2024-06-30', 'label': 'CenterPoint Energy Resources Corp.', 'flags': 'NONE_STATED'},
    ], 'provenance': 'mapping audit 2026-06-12 (second pass), cover-verified: "The aggregate market values of the voting stock held by non-affiliates of the Registrants as of June 30, 2024 are as follows: CenterPoint Energy, Inc. ... $19,797,614,936 / CenterPoint Energy Houston Electric, LLC None / CenterPoint Energy Resources Corp. None". The two "None" rows had collapsed into one undated row (value-keyed dedup); each subsidiary carries its own tagged zero (LegalEntityAxis CercCorp / HoustonElectric members).'},
    "0000790816-25-000009": {'rows': [
        {'cover': '743992592', 'xbrl': '743992592', 'as_of': '2024-06-30', 'label': 'Brandywine Realty Trust', 'flags': ''},
        {'cover': '2309866', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Brandywine Operating Partnership, L.P.', 'flags': ''},
    ], 'provenance': 'mapping audit 2026-06-12, cover-verified: "The aggregate market value of the 515,595 common units of limited partnership (\'Units\') held by non-affiliates of Brandywine Operating Partnership, L.P. was $2,309,866 ... on June 30, 2024". The unit float belongs to the Operating Partnership (its own CIK), not the Trust; both values were previously confirmed by two blind reads (the entry replaces the READS_2BLIND_CONFIRMED line).'},
    # ------------------------------------------------------------------------
    "0000079879-25-000034": {'rows': [{'cover': '29338000000', 'xbrl': '29338000000', 'as_of': '2024-06-30', 'label': 'Common Stock', 'flags': 'CURRENT_FLOAT_DROPPED'}], 'provenance': "manual rule-consistency entry: the cover states the required Q2 float ($29,338 million as of June 30, 2024) and a courtesy update ($26,163 million 'as of that date' = January 31, 2025, the shares-outstanding date); the disclosure of record is the Q2 value, matching the filer's own XBRL tag (29338000000 @ 2024-06-30) - the same rule the extractor applies to dated current-value pairs (e.g. Deere). Both rows were confirmed as stated by two blind reads."},
    "0000091142-25-000036": {'rows': [{'cover': '66240083', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Class A Common Stock', 'flags': ''}, {'cover': '9664355425', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Common Stock', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover states two float values — $66,240,083 for Class A Common Stock and $9,664,355,425 for Common Stock as of June 30, 2024 — but the extractor captured only the Class A figure; both blind reads correctly returned both classes. [ROWS_OK_FACTS_UNMATCHED -> READS_RIGHT]'},
    "0000944130-25-000007": {'rows': [{'cover': '0', 'xbrl': '', 'as_of': '2017-05-04', 'label': 'Class A Common Units', 'flags': ''}, {'cover': '7904250', 'xbrl': '', 'as_of': '2018-06-29', 'label': 'Series A Preferred Units', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover explicitly states both a $0 float for Class A Common Units (as of 2017-05-04) and a $7,904,250 float for Series A Preferred Units (as of 2018-06-29); both blind reads captured both rows, but the extractor omitted the Class A $0 row. [PROSE_ONLY -> READS_RIGHT]'},
    "0000950170-25-026068": {'rows': [{'cover': '308714932', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Common stock', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover states the float as of June 30, 2024 (last business day of Q2), using June 28 only as the pricing reference date; the extractor incorrectly used June 28 as the as_of date, while the blind reads correctly captured June 30. [SCALE_DISCREPANCY -> READS_RIGHT]'},
    "0000950170-25-026729": {'rows': [{'cover': '10757963994', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Common Stock', 'flags': ''}], 'provenance': "sonnet adjudication: The cover explicitly states the float was calculated using the June 28, 2024 closing price; the extractor used June 30 (sourced from the XBRL tag's instant date), while both blind reads correctly captured June 28. [SCALE_DISCREPANCY -> READS_RIGHT]"},
    "0000950170-25-027569": {'rows': [{'cover': '1298991046', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Common Stock', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover states the float "as of June 30, 2024" (using June 28 closing price); the extractor adopted the XBRL tag date of 2024-06-28 instead of the cover\'s stated 2024-06-30, and the XBRL value has a scale error (10^6 too large) that the cover resolves at $1,298,991,046. [SCALE_DISCREPANCY -> READS_RIGHT]'},
    "0000950170-25-038693": {'rows': [{'cover': '139872752.64', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Common Stock', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover states the float of $139,872,752.64 was computed by reference to the last reported price on June 28, 2024; the extractor captured the correct dollar amount but assigned the wrong as_of date (2024-12-31 instead of 2024-06-28), which both blind reads correctly identified. [SCALE_DISCREPANCY -> READS_RIGHT]'},
    "0001062993-25-016364": {'rows': [{'cover': '4462808', 'xbrl': '', 'as_of': '2025-01-31', 'label': '', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover states the float of $4,462,808 as of the last business day of the second fiscal quarter (FY ends July 31, so Q2 ends January 31, 2025); the extractor incorrectly used the shares-outstanding date of October 27, 2025. [PROSE_ONLY -> READS_RIGHT]'},
    "0001137091-25-000005": {'rows': [{'cover': '49500000', 'xbrl': '', 'as_of': '2024-12-31', 'label': 'Common Stock', 'flags': ''}], 'provenance': "sonnet adjudication: The cover states the public float of $49.5M is measured as of December 31, 2024 (using the June 30, 2024 OTC price as the valuation input); the $145.8M figure is the total market cap, not the float, so the extractor's two-row output and wrong as_of for the $49.5M row are both incorrect. [PROSE_SUPERSET -> READS_RIGHT]; adversarial challenge resolved by direct read: the cover explicitly states the value held by non-affiliates as of December 31, 2024 was $49.5 million (June 30, 2024 is only the OTC pricing date)"},
    "0001193125-25-039791": {'rows': [{'cover': '298712515', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares Short VIX Short-Term Futures ETF', 'flags': ''}, {'cover': '527486095', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares Ultra Bloomberg Crude Oil', 'flags': ''}, {'cover': '540643821', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares Ultra Bloomberg Natural Gas', 'flags': ''}, {'cover': '5595533', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares Ultra Euro', 'flags': ''}, {'cover': '216456025', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares Ultra Gold', 'flags': ''}, {'cover': '570829521', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares Ultra Silver', 'flags': ''}, {'cover': '232135198', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares Ultra VIX Short-Term Futures ETF', 'flags': ''}, {'cover': '44510138', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares Ultra Yen', 'flags': ''}, {'cover': '177620038', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares UltraShort Bloomberg Crude Oil', 'flags': ''}, {'cover': '147292427', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares UltraShort Bloomberg Natural Gas', 'flags': ''}, {'cover': '38226983', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares UltraShort Euro', 'flags': ''}, {'cover': '16131791', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares UltraShort Gold', 'flags': ''}, {'cover': '76198743', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares UltraShort Silver', 'flags': ''}, {'cover': '47495032', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares UltraShort Yen', 'flags': ''}, {'cover': '33401452', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares VIX Mid-Term Futures ETF', 'flags': ''}, {'cover': '148420028', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'ProShares VIX Short-Term Futures ETF', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover discloses 16 fund floats as of 2024-06-30; the extractor captured only 10, missing UltraShort Euro, UltraShort Gold, UltraShort Silver, UltraShort Yen, VIX Mid-Term Futures ETF, and VIX Short-Term Futures ETF — both blind reads captured all 16 correctly. [ROWS_OK_FACTS_UNMATCHED -> READS_RIGHT]'},
    "0001314152-25-000031": {'rows': [{'cover': '1173740', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Class A Common Stock', 'flags': ''}, {'cover': '306657', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Class M Common Stock', 'flags': ''}, {'cover': '38068', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Class A-I Common Stock', 'flags': ''}, {'cover': '1055952', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Class M-I Common Stock', 'flags': ''}, {'cover': '28168', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Class D Common Stock', 'flags': ''}], 'provenance': "sonnet adjudication: The cover states the float as of June 28, 2024; the extractor incorrectly used the XBRL tag instant date of 2024-06-30 instead of the cover's stated date of 2024-06-28, which both blind reads captured correctly. [ROWS_OK_FACTS_UNMATCHED -> READS_RIGHT]"},
    "0001326160-25-000072": {'rows': [{'cover': '77292284116', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Duke Energy Corporation', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover discloses one public float figure ($77,292,284,116 at June 30, 2024 for Duke Energy); the 776,461,008 figure is a share count (not a dollar value) and must not be a float row. [PROSE_SUPERSET -> READS_RIGHT]'},
    "0001410578-25-000698": {'rows': [{'cover': '24613330.50', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Class A ordinary shares', 'flags': ''}], 'provenance': "sonnet adjudication: The cover states the float was computed at June 28, 2024 (~$24,613,330.5); the extractor's as_of of 2024-12-31 was wrong (taken from the XBRL tag date, which also carries a 10x scale error), while both blind reads correctly captured the June 28, 2024 date and the cover value. [SCALE_DISCREPANCY -> READS_RIGHT]"},
    "0001437749-25-006605": {'rows': [{'cover': '4995579', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Class A Common Stock, $0.001 par value', 'flags': ''}, {'cover': '1000', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Class B Common Stock, $0.001 par value', 'flags': ''}, {'cover': '1500', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Class C Common Stock, $0.001 par value', 'flags': ''}], 'provenance': 'sonnet adjudication: The extractor captured share counts (5,550,643 / 20,760 / 31,938) from the "Shares held by non-affiliates" column instead of the adjacent dollar values ($4,995,579 / $1,000 / $1,500) in the "Aggregate market value held by non-affiliates" column; both blind reads correctly identified the three USD figures. [PROSE_ONLY -> READS_RIGHT]'},
    "0001477932-25-001897": {'rows': [{'cover': '48238927', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Common Stock', 'flags': ''}], 'provenance': "sonnet adjudication: The cover discloses one float of $48,238,927 as of June 30, 2024; $2.47 is the share price used to compute that figure, not a separate float value, so the extractor's two-row output is wrong and the blind reads are correct. [PROSE_SUPERSET -> READS_RIGHT]"},
    "0001493152-25-004075": {'rows': [{'cover': '41343000', 'xbrl': '', 'as_of': '2024-04-30', 'label': 'Common equity (voting and non-voting)', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover states one public float of $41,343,000 as of April 30, 2024; the $20,316,161 figure is revenue, not float, and should not be included. [PROSE_SUPERSET -> READS_RIGHT]'},
    "0001493152-25-011100": {'rows': [{'cover': '111000', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Common Stock', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover explicitly states "$111 thousands," so the correct float is $111,000 USD; the extractor dropped the stated scale and recorded 111 instead. [SCALE_DISCREPANCY -> READS_RIGHT]'},
    "0001525221-25-000016": {'rows': [{'cover': '820568000', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Common Stock', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover explicitly states the value is "(in thousands)", so $820,568 on the cover equals $820,568,000 in plain USD; the extractor stored the raw thousand-unit figure without scaling. [SCALE_DISCREPANCY -> READS_RIGHT]'},
    "0001632970-25-000018": {'rows': [{'cover': '942277000', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Voting Common Stock', 'flags': ''}, {'cover': '606540000', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Class T common stock', 'flags': ''}, {'cover': '1458259000', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Class I common stock', 'flags': ''}], 'provenance': 'sonnet adjudication: Cover discloses three separate class floats as of the last business day of Q2 2024 (June 28, which was a Friday; June 30 was a Sunday), with labels the extractor omitted and an as_of the extractor left blank for two rows; blind read 1 correctly captured the date as 2024-06-28 and all three labeled rows. [PROSE_SUPERSET -> READS_RIGHT]'},
    "0001641172-25-001819": {'rows': [{'cover': '32131327', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Class A Common Shares', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover explicitly states the value of Class A Common Shares held by non-affiliates was $32,131,327 as of June 28, 2024; the extractor missed it and both blind reads correctly captured it. [MISSED_BY_PROSE -> READS_RIGHT]'},
    "0001641172-25-003950": {'rows': [{'cover': '24361641', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Common Stock', 'flags': ''}], 'provenance': "sonnet adjudication: The cover states one float: $24,361,641 as of June 28, 2024; the $75 million figure is a regulatory threshold comparison, not a disclosed float value, so the extractor's second row is spurious. [PROSE_SUPERSET -> READS_RIGHT]"},
    "0001641172-25-004681": {'rows': [{'cover': '33995000', 'xbrl': '', 'as_of': '2024-06-28', 'label': 'Common Stock', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover states "$33,995 (in thousands)", so the true value is $33,995,000; the extractor dropped the thousands multiplier while both blind reads correctly returned 33995000. [SCALE_DISCREPANCY -> READS_RIGHT]'},
    "0001641172-25-015110": {'rows': [{'cover': '1001680', 'xbrl': '', 'as_of': '2024-06-30', 'label': 'Common Stock', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover explicitly states the value of common stock held by non-affiliates was $1,001,680 as of June 30, 2024; the extractor missed it and both blind reads correctly captured it. [MISSED_BY_PROSE -> READS_RIGHT]'},
    "0001683168-25-000206": {'rows': [{'cover': '140000', 'xbrl': '', 'as_of': '2024-04-30', 'label': '', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover states "$140,000 on April 30, 2024"; the extractor incorrectly captured the par value "$0.0001" as the float instead. [SCALE_DISCREPANCY -> READS_RIGHT]'},
    "0001683168-25-004268": {'rows': [{'cover': '0', 'xbrl': '', 'as_of': '2024-10-31', 'label': 'Common Stock', 'flags': ''}], 'provenance': "sonnet adjudication: The cover states the float was $0 as of the last business day of the second fiscal quarter (October 31, 2024); the extractor's rows carry the wrong date (blank / April 30 shares-outstanding date) and include a spurious second row from the par value, while both blind reads correctly captured value=0 as_of=2024-10-31. [PROSE_ONLY -> READS_RIGHT]"},
    "0001683168-25-004973": {'rows': [{'cover': '0', 'xbrl': '', 'as_of': '2024-10-31', 'label': '', 'flags': 'ZERO_STATED'}], 'provenance': 'sonnet adjudication: The cover states "$0 as of October 31, 2024" for the public float; the extractor misread the $0.0001 par value and used the filing date instead of the second-fiscal-quarter end date. [PROSE_ONLY -> READS_RIGHT]; label fragment ("common equity held by non-affiliates", a clip of the disclosure sentence) cleared 2026-06-12 - not a class designation.'},
    "0001683168-25-005216": {'rows': [{'cover': '0', 'xbrl': '', 'as_of': '2024-10-31', 'label': '', 'flags': ''}], 'provenance': 'sonnet adjudication: The cover states the float was approximately $0 as of October 31, 2024 (second fiscal quarter end); the extractor wrongly assigned as_of=2025-04-30 (the fiscal year end), while both blind reads correctly captured as_of=2024-10-31. [PROSE_ONLY -> READS_RIGHT]'},
    "0001683168-25-007116": {'rows': [{'cover': '0', 'xbrl': '', 'as_of': '2024-12-31', 'label': '', 'flags': 'ZERO_STATED'}], 'provenance': 'sonnet adjudication: The cover states "$0" for the public float measured as of the last business day of the second fiscal quarter (December 31, 2024 for a June 30 FY), but the extractor incorrectly assigned the shares-outstanding date of 2025-09-18 instead. [PROSE_ONLY -> READS_RIGHT]; label fragment ("common equity held by non-affiliates", a clip of the disclosure sentence) cleared 2026-06-12 - not a class designation.'},
    "0001713282-25-000027": {'rows': [{'cover': '0', 'xbrl': '', 'as_of': '2023-06-30', 'label': 'Common Stock', 'flags': ''}], 'provenance': 'negative-class sample (NO_FLOAT_STATED) overturned by sonnet: The cover explicitly states "N/A. 0" for the aggregate market value of common equity held by non-affiliates, and the XBRL fact confirms $0 as of the second-quarter end 2023-06-30; the first blind read had value_usd=0 correct but omitted the as_of date, while the extractor produced no rows.'},
    "0001731122-25-000267": {'rows': [{'cover': '11108160', 'xbrl': '', 'as_of': '2024-04-30', 'label': 'Common Stock', 'flags': ''}], 'provenance': "sonnet adjudication: The cover states one float of $11,108,160 as of the second fiscal quarter end (April 30, 2024); the $1,362,538 is revenue, not a float, so the extractor's second row is wrong, and neither blind read produced the correct single-row result. [PROSE_SUPERSET -> BOTH_WRONG]"},
    "0001995920-25-000021": {'rows': [{'cover': '0', 'xbrl': '', 'as_of': '2025-08-31', 'label': '', 'flags': ''}], 'provenance': "sonnet adjudication: The cover states one float disclosure ($0 as of 2025-08-31); the extractor's second row (as_of=2025-11-26, value=$0.001) is the common stock par value, not a float figure, so only the single $0 row is correct. [PROSE_ONLY -> READS_RIGHT]"},
}
