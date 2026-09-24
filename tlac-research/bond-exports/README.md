# Clean Workspace bond exports

## Add TLAC eligibility to a cleaned company file

After cleaning, run `add_tlac_eligibility.py` in Spyder on the machine running
Workspace. Requires `pandas` and `lseg-data`; it uses the existing default
`ld.open_session()` connection. No bond search is performed.

Set `input_filename` at the top (default `HSBC_Holdings.csv`). The file is read
from `DATA_DIR/bond_exports_cleaned`. Set `isin_column` and `issuer_column` only
if the export uses different header names; matching ignores case and surrounding
spaces. The script uses this folder's existing `config.py`.

It requests `TR.IsTLACEligible` in batches of 100 distinct nonblank ISINs, then
matches results by identifier and preserves all input rows and original columns.
Output files in `DATA_DIR/bond_exports_tlac` are:

- `<company>_tlac.csv`: enriched original rows, adding `tlac_lookup_isin`,
  `tlac_eligible`, `tlac_lookup_status`, `tlac_pulled_at_utc`, and `tlac_error`.
- `<company>_tlac_issuers.csv`: issuer names with Y flags, with row counts and
  distinct ISIN counts. Names come from the export, not a new issuer lookup.

The console prints input rows, missing ISINs, distinct ISINs requested, duplicate
excess rows, progress, Y/N/null counts, and the eligible issuer table. `null`
means LSEG returned the identifier but a blank flag; `no_response` means it did
not return that requested identifier; `missing_isin` means the input had no ISIN.
Neither null nor missing responses are treated as N. The timestamp records the
request run, not the effective date of LSEG's classification.

Each completed request is saved to `<company>_tlac_checkpoint.csv`. With
`resume = True` (the default), reruns reuse saved Y/N/null responses for matching
ISINs and retry request errors and missing responses. Set `resume = False` when
you want fresh current classifications for every bond. Resumed results retain
individual retrieval timestamps, so a resumed file can contain multiple dates.

A failed request is retried once after two seconds. A persistent error containing
HTTP code 400 is split into smaller requests until individual failing ISINs are
isolated. These rows have blank eligibility, status `request_error`, and the
exception text in `tlac_error`. They are also saved to `<company>_tlac_errors.csv`.
The console warns that results are incomplete if request errors remain. An error
at an individual ISIN does not prove the identifier is invalid or ineligible.

Other persistent request failures and unexpected response formats stop the run,
leaving completed requests in the checkpoint. Final output files from an earlier
run may remain; only a successful completion refreshes them. The previous script
version saved only at the end, so its failed runs have no on-disk checkpoint to
resume. Input CSVs are never overwritten.

## Clean the exports

Run `clean_bond_exports.py` in Spyder. Requires `pandas` and `openpyxl`.
This script reads local files and makes no LSEG API requests.

1. Copy `config.example.py` to `config.py` in this script's folder. Set `DATA_DIR`
   to your machine's data directory. The config is not committed.
2. Create a `bond_exports` folder inside that data directory. Place one Workspace
   company export per file there, with descriptive filenames (for example,
   `HSBC.xlsx`). Supported formats are `.xlsx` and `.xlsm`; resave older `.xls`
   or `.xlsb` files as `.xlsx` first. Excel temporary files are ignored.
3. Run the script. The two folder names at the top can be changed if needed.

The script reads only the `Bonds` sheet. Row 4 supplies the column names; rows
1–3 are ignored. Row 5 is the first data row. It captures the expected count from column B of the last
populated row, excluding that summary row from the data. Trailing empty rows
are ignored. A total must be a nonnegative integer; comma separators are allowed.
Formulas must have cached results saved by Excel.

Rows below the headers are retained when column C is an Excel date, an English
text date in `06-Mar-2027` format, or `Perpetual` (case and surrounding spaces
ignored). Completely blank rows are skipped. Other nonempty rows are saved for
inspection. Column J supplies the ISIN. All original columns are preserved;
blank or repeated headers receive unique names. Typed dates are serialized to
CSV by pandas; original text dates remain text.

Outputs in `DATA_DIR/bond_exports_cleaned` are replaced on each run:

- One company CSV per readable input file, including files with count mismatches.
  The filename keeps everything before the first digit in the input stem, then
  removes one trailing underscore. For example, `HSBC_Holdings_20260922.xlsx`
  becomes `HSBC_Holdings.csv`. Without digits, the full stem is kept (with a
  trailing underscore removed if present). Headers from Excel row 4 become the
  first CSV row; bond data start immediately below. Naming collisions stop the
  run before writing, so one input cannot overwrite another company's output.
  Company CSVs contain only the original exported columns. The four helper
  columns are used internally for validation and omitted from these files.
- `validation_summary.csv`: expected and retained counts, their difference,
  excluded nonempty rows, missing ISINs, and duplicate ISIN counts per file.
  `duplicate_isin_rows` counts all rows sharing an ISIN; `duplicate_isin_excess_rows`
  counts only occurrences after the first. Duplicates are not deleted. Missing
  or duplicate identifiers do not by themselves cause a count failure.
- `rows_excluded.csv`: rejected nonempty rows with their original location and
  exclusion reason. Header rows and the final total row are not included.

A count mismatch or unreadable file produces an error after saving the reports.
Unreadable files contribute no bond rows; inspect the summary before using the
company files. Matching the count verifies this cleaning rule against the export,
not that Workspace's original company universe or TLAC classification is correct.
Each company output preserves its own exported columns.
No bonds are deduplicated, including bonds present in more than one company file.
Input workbooks are never modified. Existing company outputs are replaced when
that input is successfully read; a failed input does not refresh its old output.
Check the current validation summary to identify failures. The old combined
`bonds_cleaned.csv` from earlier script versions is no longer produced or updated;
use the individually named company files instead.
