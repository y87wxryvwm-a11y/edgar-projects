# Python equivalents of the Workspace Excel bond columns

The user's ten-result Python search succeeded. `pull_bond_details.py` extends that
same name-search call to 100 results and the columns below, in the requested order.
Run it in the same Spyder environment, with the existing config.py and Workspace
session. It writes `bond_details.csv` and `bond_details_raw.csv` to DATA_DIR;
re-running replaces those files. The original working `pull_one_company.py` is
unchanged.

This is the next comparison sample. It does not yet reproduce the full Excel
universe. The full automated selection remains to be established in Python. Raising the row limit (maximum 10,000) is not a way to retrieve all
14,154 previously reported active issued bonds. Do not treat a name search as
proof of company affiliation, activity status or TLAC eligibility.

## Column mapping

Most fields use LSEG Search properties. `TR.IsTLACEligible` and the user-specified
`TR.FiSeniorityTypeDescription` are retrieved separately with `ld.get_data()`. A property documented as available is not proof that it
exactly reproduces a customized Excel field, units or formatting. Compare the
same ISIN in both outputs.

| Excel label | Python CSV column | Search property | Mapping note |
|---|---|---|---|
| Description | description | DTSubjectName | Compare display text with Excel. |
| Maturity date | maturity_date | MaturityDate | Raw returned date. |
| Amount outstanding | amount_outstanding | FaceOutstanding | Preserve raw units; compare Excel's currency and scaling. |
| Issued amount | issued_amount | FaceIssuedTotal | Preserve raw units; treatment of additional issuance needs checking before annual issuance analysis. |
| Coupon | coupon | CouponRate | Preserve raw value; check percentage display in Excel. |
| Coupon class | coupon_class | CurrentCouponClassDescription | Candidate for the displayed current coupon class. |
| Country of issue | country_of_issue | RCSCountryLeaf | Country-of-issue mapping supported by the Advanced Search example below. |
| Currency | currency | RCSCurrencyLeaf | Currency label from Search. |
| ISIN | isin | ISIN | Security identifier. |
| CUSIP | cusip | CUSIP | Security identifier; may be blank. |
| Issue date | issue_date | IssueDate | Raw returned date. |
| Rank (seniority) | rank_seniority | TR.FiSeniorityTypeDescription | User-specified field, retrieved with get_data. |
| Seniority (legacy) | seniority_legacy | Unconfirmed | Left blank, not populated with an assumed equivalent. |
| Instrument type | instrument_type | InstrumentTypeDescription | Compare classification wording with Excel. |
| is_convertible | is_convertible | IsConvertible | Provider flag; do not interpret as covering every statutory bail-in conversion. |
| Offering type | offering_type | OfferingTypeDescription | Provider description. |
| Domicile | domicile | RCSDomicileLeaf | Compare with Excel; distinct from parent domicile. |
| Asset status | asset_status | AssetStatusDescription | Raw status description; no active-only filtering. |
| Issuer | issuer | IssuerCommonName | Actual issuer name returned. |

Appended columns: `tlac_eligible`, `ric`, and `cds_seniority_candidate`. The last
contains `CdsSeniorityEquivalentDescription`, a documented alternative seniority
measure. Its equivalence to Excel's **Seniority (Legacy)** was not established in
public documentation; it is deliberately separate. Missing values remain blank.

## Public documentation used

- [LSEG forum: bond data via API](https://community.developers.lseg.com/discussion/131340/bond-data-via-api-in-python): published Search export includes most properties above.
- [LSEG Advanced Search example](https://developers.lseg.com/en/article-catalog/article/Find-content-and-functionality-using-Refinitiv-Data-Library-with-Eikon-Advanced-Search): country-of-issue filtering and exported Search fields; also explains exporting API queries from the interface.
- [LSEG convertible/amount example](https://community.developers.lseg.com/discussion/132101/convert-bonds-issued-amounts-in-eur-using-exchange-rate-at-issue-date): shows `IsConvertible`, `FaceOutstanding` and `FaceIssuedTotal`; discussion also illustrates why amount definitions and scaling must be checked rather than assumed.
- [LSEG debt-structure example](https://developers.lseg.com/en/article-catalog/article/debt-structure-analysis-on-an-organizational-level): uses `CdsSeniorityEquivalentDescription`.

## What remains to verify

Run the expanded sample and compare a few matching ISINs to Excel. Priority
comparisons are amounts, the two seniority columns and coupon class. The current
code has been syntax/API-interface checked and exercised with offline fixtures;
its new fields still need live confirmation. The connection and smaller name
search were already confirmed working by the user.

The user specified Python-only work and supplied `TR.FiSeniorityTypeDescription`
for seniority. No Excel workflow is required. The next stage is to expand the
working Python search into a complete selection after confirming these fields.
The separate legacy column remains blank because no independent field is verified.
