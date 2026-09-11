# FPI market capitalization

Run `fetch_market_caps.py` in Spyder on the machine running LSEG Workspace.

1. Install `lseg-data` and `pandas` in Spyder's Python environment:
   `pip install lseg-data pandas`.
2. Copy `config.example.py` to `config.py` in this folder. Set `INPUT_CSV` to
   the full path of your registrant-count CSV. The input needs `CIK` and `form`
   columns; all other columns are retained.
3. Open Workspace and sign in. The script uses `ld.open_session()` with no
   explicit app key, as shown in the LSEG Python quick-start. If your local
   session configuration requires credentials, resolve that configuration on
   the Workspace machine.
4. Set `valuation_date` at the top of the script and run it. Set Spyder's
   working directory to wherever you want the output CSV saved.

The script selects distinct CIKs with forms `20F`, `20F/A`, `40F`, or `40F/A`.
Hyphenated forms such as `20-F/A` are also accepted. It requests
`TR.CompanyMarketCap` in batches of 100 CIKs, with `SDate` set to the chosen
date, `Curn="USD"`, and `Scale=0` (unscaled dollars).

The values are left-merged by normalized CIK into the original data. The output
retains every original row and column and appends `market_cap_usd`. Every row
for a retrieved CIK receives that value, regardless of that row's form. Missing
values are blank. The script does not verify listing status or build a crosswalk.

Output: `<input_stem>_market_cap_<valuation_date>.csv` in the current working
directory. Re-running for the same input name and date replaces that output.
Do not sum repeated CIK rows when calculating an issuer-level aggregate.

The market cap is company-level global equity value; changing the valuation
date does not refresh population membership. Live retrieval must be confirmed
on the machine with Workspace access.

LSEG references:

- [Python quick-start](https://developers.lseg.com/en/api-catalog/lseg-data-platform/lseg-data-library-for-python/quick-start)
- [Historical company market cap](https://community.developers.lseg.com/discussion/34898/how-to-get-history-data-of-stock-market-value-data/p1)
