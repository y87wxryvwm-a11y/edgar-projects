# One-company Workspace bond pull

Run `pull_one_company.py` in Spyder on the machine where the existing
`fpi-market-cap/fetch_market_caps.py` works. It uses the same `ld.open_session()`
connection and the same simple `ld.get_data()` approach. No new app-key setup is
introduced.

## Run

1. Open Workspace and sign in using the existing working configuration.
2. Copy this folder's `config.example.py` to `config.py`. Set `DATA_DIR` to your
   output directory. This file is ignored by Git.
3. Open `pull_one_company.py` in Spyder. Use this script's directory as the working
   directory so that it imports this folder's configuration.
4. Run it. The defaults select **Credit Agricole SA, 8589934312** and compare against
   the user's **14,154 active issued bonds**, observed on September 22, 2026.
   `lseg-data` and `pandas` must be available in that Spyder environment, as for the
   market-cap script.

The user also reported **309 bonds to be issued and 26 loans**. Those are recorded
as reference observations, not added to 14,154. The code searches the government
and corporate instrument view with the Bonds category; it does not pull loans.
No inactive count has yet been supplied.

## What it retrieves

The search follows LSEG's published `ParentOAPermID` example, using the selected
organization ID. It requests active, inactive and unknown activity flags
separately, without a maturity exclusion. A request returning 10,000 rows is
split into disjoint issue-date ranges, including a separate missing-date query.
Only uncapped leaf results are combined. If a partition cannot be split further,
the script stops rather than claiming a complete result.

It saves issuer identity, ISIN, RIC, dates, currency, status, amounts and seniority,
then requests `TR.IsTLACEligible` in batches of 100 identifiers. ISIN is preferred,
with a RIC fallback. Y, N and missing results are retained. The current flag on an
inactive bond must not be interpreted as its eligibility when originally issued.

## Compare with Workspace

Each run creates a dated directory under `DATA_DIR/lseg_pilot/` containing:

- **`summary.json`**: exact queries, row counts, comparison with 14,154, duplicate
  and missing identifier counts, TLAC flag totals and failed-batch indicators.
- **`counts_by_status.csv`**: active/inactive/unknown and past/future/missing issue dates.
- **`counts_by_issuer.csv`**: issuer and status breakdown for diagnosing a hierarchy mismatch.
- **`bonds_with_tlac.csv`**: combined bond records and the eligibility flag.
- **`bonds_before_tlac.csv`**, raw search partitions and raw TLAC batches: evidence
  retained even if subsequent enrichment fails.

The script reports both all Search-active rows and a candidate "active issued"
count, defined explicitly as Search-active with an issue date on or before the
UTC retrieval date. **This is a proposed comparison, not a verified translation
of Debt Structure's screen categories.** Future issue dates are reported
separately; an announced bond may also have a missing date. Do not change the
filters merely to force the count to 14,154.

If the count differs, the first checks are the screen's scope and filters, the
exact parent relationship, instrument categories and activity definitions.
Crédit Agricole SA and Rue La Boétie produce equal screen counts according to the
user, but the same equivalence has not been established for this API filter.
The issuer breakdown and raw status values help locate the difference. Matching
exported ISIN lists is stronger verification than matching totals alone.

Duplicate ISIN rows are reported and retained, not silently discarded; inspect
them before interpreting row counts as distinct bonds. Outstanding-amount fields
are raw provider measures and have not been currency-converted or reconciled to
regulatory TLAC resources. `completed` in the summary describes the extraction
workflow, not proof of population completeness or equality to Workspace.

## Validation status

The code's syntax and installed LSEG library interfaces were checked locally.
An offline simulated-data run exercises the greater-than-10,000 partitioning,
null dates, status counts and identifier-based TLAC joining. It is not evidence
of vendor coverage. A local connection attempt found no Workspace desktop proxy
on this Mac; **a successful live pull and a match to the screen remain to be
confirmed in the user's working Workspace/Spyder environment**.

## Sources

- [LSEG parent-based debt search example](https://developers.lseg.com/en/article-catalog/article/debt-structure-analysis-on-an-organizational-level)
- [LSEG bond category and active/inactive search guidance](https://community.developers.lseg.com/discussion/85385/list-of-all-active-and-non-active-bonds-filtering-for-maturity-date)
- [LSEG search capabilities and limits](https://developers.lseg.com/en/article-catalog/article/building-search-into-your-application-workflow)
- [LSEG TLAC eligibility field example](https://community.developers.lseg.com/discussion/comment/90672/)
