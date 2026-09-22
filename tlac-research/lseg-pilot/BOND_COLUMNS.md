# Bond reference fields

`pull_bond_details.py` now uses the working name search only to discover ISINs/RICs
and retain asset status. All other populated detail columns come from `ld.get_data`
using the fields below. It no longer substitutes Search properties for those
reference fields. The user corrected these mappings on September 22, 2026.

| Output | Requested field |
|---|---|
| description | TR.FiDescription |
| maturity_date | TR.FiMaturityDate |
| amount_outstanding | TR.CA.AmtOutstanding |
| amount_outstanding_currency | TR.FiAmtOutstandingCurrency |
| issued_amount | TR.FiFaceIssuedTotal |
| coupon | TR.FiCouponRate |
| coupon_class | TR.FiCouponClassDescription |
| coupon_type | TR.FiCouponTypeDescription |
| country_of_issue | TR.FiCountryName |
| currency | TR.FiCurrency |
| isin | TR.ISIN |
| cusip | TR.CUSIP |
| issue_date | TR.FiIssueDate |
| rank_seniority | TR.FiSeniorityTypeDescription |
| seniority_legacy | Blank; no separate field confirmed |
| instrument_type | TR.FiInstrumentTypeDescription |
| is_convertible | TR.FiIsConvertible |
| offering_type | TR.FiOfferingTypeDescription |
| domicile | TR.FiDomicile |
| asset_status | AssetStatusDescription from Search; no TR equivalent confirmed |
| issuer | TR.FiIssuerName |
| tlac_eligible | TR.IsTLACEligible |

The added amount-outstanding currency follows its amount, and coupon type follows
coupon class. Values are returned without currency conversion, scaling or
historical-date parameters. Missing values remain blank. The old CDS seniority
candidate has been removed. `lookup_identifier` is appended for tracing results.

## Run and compare

Run this script in the existing Workspace/Spyder environment using the same
local `config.py` and DATA_DIR. It defaults to 100 name-search results. The
original successful `pull_one_company.py` is unchanged.

Outputs in DATA_DIR:

- `bond_search_identifiers.csv`: discovery results.
- `bond_reference_batch_*.csv`: raw reference responses, saved before matching.
- `bond_details.csv`: ordered output with readable column names.

Re-running replaces matching filenames. It matches reference columns by their
field names (case-insensitive), and rows by the returned Instrument identifier;
it does not assign a meaning based solely on column position. Missing fields
are reported. Multiple reference rows for one identifier stop the join so we do
not silently choose a potentially wrong amount/date.

This remains a sample: a 100-row name search does not establish the complete
14,154-bond population. No parent filters or Excel workflow are introduced.
Syntax and offline matching checks passed; the newly specified fields still
need confirmation in the live Workspace session.

## Supplemental field verification

The user supplied the corrected reference fields. The additional maturity date,
coupon rate, currency, ISIN and CUSIP fields were checked in LSEG documentation:

- [LSEG fixed-income reference-data example](https://developers.lseg.com/en/article-catalog/article/the-data-library-for-python-quick-reference-guide-access-layer): TR.FiIssuerName, TR.ISIN, TR.CUSIP, TR.FiCurrency, TR.FiMaturityDate.
- [LSEG bond reference example](https://developers.lseg.com/en/article-catalog/article/event-driven-financial-calculation-with-eikon-excel-visual-basic): TR.FiDescription, TR.FiCouponRate, TR.FiMaturityDate, TR.FiCurrency.
