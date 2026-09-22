"""Return a bond-search sample with columns arranged like the Workspace export."""

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

# Mapping order deliberately matches the user's Excel export.
# These are Search properties, not TR.* fundamental-data fields.
columns = {
    "description": "DTSubjectName",
    "maturity_date": "MaturityDate",
    "amount_outstanding": "FaceOutstanding",
    "issued_amount": "FaceIssuedTotal",
    "coupon": "CouponRate",
    "coupon_class": "CurrentCouponClassDescription",
    "country_of_issue": "RCSCountryLeaf",
    "currency": "RCSCurrencyLeaf",
    "isin": "ISIN",
    "cusip": "CUSIP",
    "issue_date": "IssueDate",
    "rank_seniority": None,  # Retrieved below with TR.FiSeniorityTypeDescription.
    "seniority_legacy": None,  # Exact Excel field code still needs confirmation.
    "instrument_type": "InstrumentTypeDescription",
    "is_convertible": "IsConvertible",
    "offering_type": "OfferingTypeDescription",
    "domicile": "RCSDomicileLeaf",
    "asset_status": "AssetStatusDescription",
    "issuer": "IssuerCommonName",
}
extra_fields = ["MainSuperRIC", "CdsSeniorityEquivalentDescription"]
fields = list(dict.fromkeys([x for x in columns.values() if x] + extra_fields))
directory = DATA_DIR.replace("\\", "/")
os.makedirs(directory, exist_ok=True)
if not isinstance(number_of_results, int) or not 1 <= number_of_results <= 10000:
    raise ValueError("number_of_results must be between 1 and 10,000 for this comparison sample.")

ld.open_session()
try:
    results = ld.discovery.search(
        view=ld.discovery.Views.GOV_CORP_INSTRUMENTS,
        query=company_search,
        top=number_of_results,
        select=",".join(fields),
    )
    if results is None or results.empty:
        raise RuntimeError("Name search returned no results.")
    results.to_csv(os.path.join(directory, "bond_details_raw.csv"), index=False)
    missing = [field for field in fields if field not in results.columns]
    if missing:
        print("Fields absent from the response (left blank):", ", ".join(missing))
    output = pd.DataFrame(index=results.index)
    for label, field in columns.items():
        output[label] = results[field] if field in results.columns else pd.NA
    output["tlac_eligible"] = pd.NA
    output["ric"] = results.get("MainSuperRIC", pd.Series(pd.NA, index=results.index))
    output["cds_seniority_candidate"] = results.get(
        "CdsSeniorityEquivalentDescription", pd.Series(pd.NA, index=results.index)
    )
    # Save the complete Search result before the separate TLAC request.
    output_path = os.path.join(directory, "bond_details.csv")
    output.to_csv(output_path, index=False)
    lookup = output["isin"].replace("", pd.NA).fillna(output["ric"].replace("", pd.NA))
    identifiers = lookup.dropna().astype(str).drop_duplicates().tolist()
    batches = []
    for start in range(0, len(identifiers), 100):
        flags = ld.get_data(universe=identifiers[start:start + 100],
                            fields=["TR.IsTLACEligible", "TR.FiSeniorityTypeDescription"])
        if flags is None or flags.shape[1] != 3:
            raise RuntimeError("Unexpected TLAC/seniority response; Search data are already saved.")
        flags = flags.copy()
        flags.columns = ["identifier", "tlac_eligible", "rank_seniority"]
        batches.append(flags)
    if batches:
        flags = pd.concat(batches, ignore_index=True)
        if flags["identifier"].duplicated().any():
            raise RuntimeError("Duplicate identifiers in TLAC response; Search data are already saved.")
        by_id = flags.set_index("identifier")
        output["tlac_eligible"] = lookup.map(by_id["tlac_eligible"])
        output["rank_seniority"] = lookup.map(by_id["rank_seniority"])
    output.to_csv(output_path, index=False)
    print(output.head(10).to_string(index=False))
    print(f"\nSaved {len(output):,} search-result rows to {output_path}")
    print("This is a name-search sample, not yet a complete or reconciled company universe.")
    if len(results) == number_of_results:
        print("The requested row limit was reached; additional results may exist.")
    print("seniority_legacy is blank pending the exact Excel code; CDS candidate is separate.")
finally:
    ld.close_session()
