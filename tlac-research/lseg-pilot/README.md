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
