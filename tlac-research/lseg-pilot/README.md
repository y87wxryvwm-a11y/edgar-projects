# Small Workspace search test

Run `pull_one_company.py` in the same Spyder environment as the successful
`fpi-market-cap` pull, with Workspace open and signed in.

Use the existing local `config.py` with `DATA_DIR` set to your output folder.
Run from this script's directory.

The script searches for `Credit Agricole` in the government/corporate instrument
view and returns **ten search results**, then retrieves `TR.IsTLACEligible` for
the returned ISINs. There are no parent, category or activity filters.

It prints both tables and saves `workspace_search_sample.csv` and
`workspace_tlac_sample.csv` in DATA_DIR. Re-running replaces those sample files.

This is only a search and field-retrieval test. Name search can include similarly
named or related entities. It is not a complete group universe and should not be
compared with the 14,154 screen count yet. Inspect the issuer names in the output.

The previous full pull returned zero bonds in the user's live environment. This
smaller replacement has been syntax/API-interface checked locally; a live result
still needs to be confirmed on the Workspace machine.

## Expanded column comparison

The user confirmed that this ten-result test works. Next run
`pull_bond_details.py` for a 100-result sample with the Excel columns in order,
plus TLAC flags. See [BOND_COLUMNS.md](BOND_COLUMNS.md) for field mappings and the
one unresolved legacy-seniority field. This expanded sample does not replace or
modify the working ten-result script.

## HSBC batched pull

Run `pull_hsbc_bonds.py` in Spyder with Workspace signed in, using the same local
`config.py`. It searches for HSBC with `IsActive eq true`, excluding matured (`MAT`)
and cancelled (`DC`) records, without a TLAC filter. Subsidiaries remain in scope;
the pull is not restricted to HSBC Holdings plc alone. The Workspace comparison
target supplied by the user is approximately 31,000 active group bonds. It exhausts
the name search, not a verified corporate-group universe; compare the result
with Workspace and HSBC's public instrument disclosures before claiming group
completeness. In particular, affiliated issuers need not have HSBC in their names.

- Search pages: 500 rows, saved individually. Date intervals split when they
  reach the 10,000-result search window; undated records are included separately.
- Detail batches: 100 unique identifiers and the 20 reference fields already
  agreed, with a four-second pause before each network request.
- Local rolling-24-hour budget: 500 attempted calls or 300,000 estimated cells,
  whichever comes first, including search. The ledger is shared across this
  script's run folders under DATA_DIR. Requests are reserved before sending,
  including failed attempts. This is a conservative local estimate, not the
  account's remaining quota: other scripts, Excel, SDK-internal requests and
  other account usage are not measured. Run only one copy of this script at once.
- Keep `run_folder` unchanged to resume. Saved successful pages/batches are
  reused. Change it for a fresh snapshot; changing fields or batch size requires
  a new folder. A run resumed on another day contains observations from multiple
  dates; per-request JSON files record retrieval times.
- Exceptions stop the script without automatic retries. Incomplete detail
  responses are saved as `.received.csv` for inspection, not accepted as completed
  batches. Completed batches survive a failure or budget stop. Run again after
  resolving an error, or after requests age out of the local budget (up to 24h).

Outputs beneath `DATA_DIR/hsbc_active_bonds_2026_09_22`:

- `search_*.csv`: saved search pages (including parent partitions later split).
- `bond_search_identifiers.csv`: combined final search partitions.
- `search_rows_without_identifier.csv`: records that cannot be queried by ISIN/RIC.
- `details_*.csv`: successful detail batches, each with a retrieval-time JSON file.
- `hsbc_bond_details.csv`: combined final data, one row per distinct lookup ID.
- `issuer_tlac_counts.csv`: Y/N/null counts by issuer.

ISIN is preferred, with RIC as fallback. Raw search rows are retained so identifier
duplication can be reviewed. Null TLAC values remain null, never N. No totals are
summed across currencies. Final combined detail output is written only after all
batches finish. The public-disclosure reconciliation is the next step.

LSEG sources: [usage guidelines](https://developers.lseg.com/en/api-catalog/lseg-data-platform/lseg-data-library-for-python/documentation)
and [usage monitoring](https://developers.lseg.com/en/article-catalog/article/check-lseg-data-library-for-python-usage-limits-remaining).
Published service limits are not a guarantee of this account's available quota.
The pull has been checked locally with simulated responses; a live run requires
the user's Workspace machine.

The active pull uses a new folder so archived results from the earlier unfiltered
run cannot be reused. Keep the usage ledger: previous requests still consumed
allowance. Issue-date splitting is only pagination, not a request for inactive
bonds; every interval and the undated partition receive the active filter.
