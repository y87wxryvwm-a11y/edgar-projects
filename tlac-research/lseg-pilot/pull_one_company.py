"""Small Workspace search test: return ten instruments and their TLAC flags."""

# ---- EDIT THIS --------------------------------------------------------------
company_search = "Credit Agricole"
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("Set DATA_DIR in this folder's config.py.") from None

import os
import lseg.data as ld

directory = DATA_DIR.replace("\\", "/")
os.makedirs(directory, exist_ok=True)

ld.open_session()
try:
    results = ld.discovery.search(
        view=ld.discovery.Views.GOV_CORP_INSTRUMENTS,
        query=company_search,
        top=10,
        select="ISIN,MainSuperRIC,IssuerCommonName,IssueDate,MaturityDate",
    )
    print(results.to_string(index=False))
    results.to_csv(os.path.join(directory, "workspace_search_sample.csv"), index=False)
    if results.empty:
        raise RuntimeError("The simple name search returned no results.")

    identifiers = results["ISIN"].dropna().astype(str)
    identifiers = identifiers[identifiers.str.strip().ne("")].drop_duplicates().tolist()
    if not identifiers:
        raise RuntimeError("Results returned, but none has an ISIN; inspect the saved sample.")
    flags = ld.get_data(universe=identifiers, fields=["TR.IsTLACEligible"])
    print(flags.to_string(index=False))
    flags.to_csv(os.path.join(directory, "workspace_tlac_sample.csv"), index=False)
finally:
    ld.close_session()
