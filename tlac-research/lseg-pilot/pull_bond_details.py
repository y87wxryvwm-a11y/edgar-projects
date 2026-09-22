"""Find bond identifiers, then retrieve their reference fields with get_data."""

# ---- EDIT THIS --------------------------------------------------------------
company_search = "Credit Agricole"
number_of_results = 100
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("Set DATA_DIR in this folder's config.py.") from None

import os
import pandas as pd
import lseg.data as ld

# User-specified reference fields, plus verified maturity/coupon/currency/IDs.
columns = {
    "description": "TR.FiDescription",
    "maturity_date": "TR.FiMaturityDate",
    "amount_outstanding": "TR.CA.AmtOutstanding",
    "amount_outstanding_currency": "TR.FiAmtOutstandingCurrency",
    "issued_amount": "TR.FiFaceIssuedTotal",
    "coupon": "TR.FiCouponRate",
    "coupon_class": "TR.FiCouponClassDescription",
    "coupon_type": "TR.FiCouponTypeDescription",
    "country_of_issue": "TR.FiCountryName",
    "currency": "TR.FiCurrency",
    "isin": "TR.ISIN",
    "cusip": "TR.CUSIP",
    "issue_date": "TR.FiIssueDate",
    "rank_seniority": "TR.FiSeniorityTypeDescription",
    "seniority_legacy": None,  # No separate field confirmed; do not substitute one.
    "instrument_type": "TR.FiInstrumentTypeDescription",
    "is_convertible": "TR.FiIsConvertible",
    "offering_type": "TR.FiOfferingTypeDescription",
    "domicile": "TR.FiDomicile",
    "asset_status": None,  # Explicitly retained from Search's AssetStatusDescription.
    "issuer": "TR.FiIssuerName",
    "tlac_eligible": "TR.IsTLACEligible",
}
fields = [field for field in columns.values() if field]
directory = DATA_DIR.replace("\\", "/")
os.makedirs(directory, exist_ok=True)
if not isinstance(number_of_results, int) or not 1 <= number_of_results <= 10000:
    raise ValueError("number_of_results must be between 1 and 10,000 for this sample.")

ld.open_session()
try:
    # Keep the name search that worked. Search supplies IDs, not reference values.
    results = ld.discovery.search(
        view=ld.discovery.Views.GOV_CORP_INSTRUMENTS,
        query=company_search,
        top=number_of_results,
        select="ISIN,MainSuperRIC,AssetStatusDescription",
    )
    if results is None or results.empty:
        raise RuntimeError("Name search returned no results.")
    results = results.reset_index(drop=True)
    results.to_csv(os.path.join(directory, "bond_search_identifiers.csv"), index=False)
    isin = results.get("ISIN", pd.Series(pd.NA, index=results.index)).replace("", pd.NA)
    ric = results.get("MainSuperRIC", pd.Series(pd.NA, index=results.index)).replace("", pd.NA)
    lookup = isin.fillna(ric).astype("string")
    identifiers = lookup.dropna().drop_duplicates().tolist()
    if not identifiers:
        raise RuntimeError("Search returned no usable ISINs or RICs.")
    batches = []
    for start in range(0, len(identifiers), 100):
        values = ld.get_data(
            universe=identifiers[start:start + 100], fields=fields,
            header_type=ld.HeaderType.NAME,
        )
        if values is None or values.empty:
            raise RuntimeError("Reference-data request returned no rows; identifiers are saved.")
        # NAME headers can be uppercase; match the actual field names, never positions.
        values = values.rename(columns=lambda label: str(label).upper())
        if "INSTRUMENT" not in values:
            raise RuntimeError("Reference response lacks the Instrument matching key.")
        values.to_csv(os.path.join(directory, f"bond_reference_batch_{start:06d}.csv"), index=False)
        batches.append(values)
        print(f"Retrieved reference fields for {start + min(100, len(identifiers) - start):,} identifiers")
    raw = pd.concat(batches, ignore_index=True)
    if raw["INSTRUMENT"].duplicated().any():
        raise RuntimeError("Multiple reference rows per identifier; inspect saved batches before combining.")
    by_id = raw.set_index("INSTRUMENT")
    output = pd.DataFrame(index=results.index)
    missing = []
    for label, field in columns.items():
        if field and field.upper() in by_id:
            output[label] = lookup.map(by_id[field.upper()])
        else:
            output[label] = pd.NA
            if field:
                missing.append(field)
    output["asset_status"] = results.get("AssetStatusDescription", pd.Series(pd.NA, index=results.index))
    output["lookup_identifier"] = lookup
    output_path = os.path.join(directory, "bond_details.csv")
    output.to_csv(output_path, index=False)
    print(output.head(10).to_string(index=False))
    print("Saved:", output_path)
    if missing:
        print("Requested fields absent from response (left blank):", ", ".join(missing))
    print("Rows without any reference response:", int((lookup.notna() & ~lookup.isin(by_id.index)).sum()))
    print("Sample only; name-search results are not a complete company universe.")
    if len(results) == number_of_results:
        print("Requested row limit reached; more results may exist.")
finally:
    ld.close_session()
