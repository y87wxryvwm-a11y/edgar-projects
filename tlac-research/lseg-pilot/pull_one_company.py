"""Pull one group's bonds from Workspace and compare counts before scaling up."""

# ---- EDIT THIS --------------------------------------------------------------
company_name = "Credit Agricole SA"
company_permid = "8589934312"
expected_active_issued = 14154  # Workspace count reported September 22, 2026.
expected_inactive = None     # Enter the count shown in Workspace, or leave None.
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError as exc:
    raise RuntimeError(
        "Copy config.example.py to config.py in this folder; set DATA_DIR."
    ) from exc

import json
import os
from datetime import date, datetime, timezone

import lseg.data as ld
import pandas as pd
from lseg.data.content import search

directory = DATA_DIR.replace("\\", "/")
run_time = datetime.now(timezone.utc)
output_dir = os.path.join(directory, "lseg_pilot", run_time.strftime("%Y%m%dT%H%M%S%fZ"))
os.makedirs(output_dir, exist_ok=True)
if not str(company_permid).isdigit():
    raise ValueError("company_permid must contain only digits.")

# This follows LSEG's published parent-based debt search. Whether its relationship
# matches the Debt Structure screen is the subject of this pilot, not an assumption.
base_filter = (
    f"ParentOAPermID eq '{company_permid}' "
    "and SearchAllCategoryv3 xeq 'Bonds'"
)
fields = [
    "ISIN", "MainSuperRIC", "DTSubjectName", "IssuerCommonName",
    "IssuerOAPermID", "ParentIssuerName", "ParentOAPermID", "IsActive",
    "AssetStatus", "Currency", "IssueDate", "MaturityDate", "FaceIssuedTotal",
    "EOMAmountOutstanding", "IsPerpetualSecurity",
    "CdsSeniorityEquivalentDescription",
]
summary = {
    "company_name": company_name, "company_permid": str(company_permid),
    "retrieved_at_utc": run_time.isoformat(), "search_view": "GOV_CORP_INSTRUMENTS",
    "base_filter": base_filter, "search_fields": fields,
    "status_definition": "LSEG Search IsActive true / false / null; not assumed identical to screen tabs",
    "expected_active_issued": expected_active_issued, "expected_inactive": expected_inactive,
    "workspace_reported_to_be_issued": 309, "workspace_reported_loans": 26,
    "queries": [], "tlac_batch_errors": [], "completed": False,
}

def fetch_partition(expression, label, lower=date(1800, 1, 1),
                    upper=date(2200, 1, 1), date_known=False, depth=0):
    """Split capped requests into disjoint issue-date partitions; never use skip past 10k."""
    response = search.Definition(
        view=search.Views.GOV_CORP_INSTRUMENTS,
        filter=expression, select=",".join(fields), top=10000,
    ).get_data()
    frame = response.data.df
    if frame is None:
        raise RuntimeError(f"No dataframe returned for {label}; inspect API response.")
    frame = frame.copy()
    query_number = len(summary["queries"])
    summary["queries"].append({"status": label, "filter": expression,
                                "rows": len(frame), "split": len(frame) >= 10000})
    frame.to_csv(os.path.join(output_dir, f"search_{query_number:04d}_{label}_raw.csv"), index=False)
    print(f"Search {query_number + 1}: {label}, {len(frame):,} records" +
          ("; partitioning capped response" if len(frame) >= 10000 else ""))
    if len(frame) < 10000:
        return [frame]
    if depth >= 25 or (upper - lower).days < 2:
        raise RuntimeError("A date partition still reaches 10,000 rows; stop rather than truncate.")
    # Null dates are explicitly included in their own partition.
    if not date_known:
        missing = fetch_partition(f"({expression}) and IssueDate eq null", label,
                                  lower, lower, True, 25)
        expression = f"({expression}) and IssueDate ne null"
    else:
        missing = []
    midpoint = lower + (upper - lower) // 2
    left = fetch_partition(f"({expression}) and IssueDate lt {midpoint.isoformat()}",
                           label, lower, midpoint, True, depth + 1)
    right = fetch_partition(f"({expression}) and IssueDate ge {midpoint.isoformat()}",
                            label, midpoint, upper, True, depth + 1)
    return missing + left + right


try:
    # Same connection call as the user's successful fpi-market-cap script.
    session = ld.open_session()
    if session.open_state != ld.OpenState.Opened:
        raise RuntimeError("Workspace session is not open. Run on the same machine and environment as the successful market-cap pull.")
    parts = []
    # A separate null query prevents silently dropping instruments without a flag.
    for label, condition in [("active", "IsActive eq true"),
                             ("inactive", "IsActive eq false"),
                             ("unknown", "IsActive eq null")]:
        expression = f"({base_filter}) and ({condition})"
        frame = pd.concat(fetch_partition(expression, label), ignore_index=True)
        frame["pilot_status"] = label
        parts.append(frame)

    bonds = pd.concat(parts, ignore_index=True)
    if bonds.empty:
        raise RuntimeError("Search returned zero bonds. Check the parent relationship and screen filters.")
    for field in ["ISIN", "MainSuperRIC"]:
        if field not in bonds:
            bonds[field] = pd.NA
    # Preserve raw rows: duplicate ISINs are a diagnostic, not silently discarded.
    bonds["lookup_identifier"] = bonds["ISIN"].replace("", pd.NA).fillna(
        bonds["MainSuperRIC"].replace("", pd.NA)
    )
    bonds.to_csv(os.path.join(output_dir, "bonds_before_tlac.csv"), index=False)
    summary["search_rows"] = len(bonds)
    summary["unique_isins"] = int(bonds["ISIN"].replace("", pd.NA).nunique())
    summary["missing_lookup_identifiers"] = int(bonds["lookup_identifier"].isna().sum())
    valid_isins = bonds["ISIN"].replace("", pd.NA).dropna()
    summary["duplicate_isin_rows"] = int(valid_isins.duplicated().sum())
    summary["count_comparisons"] = {}
    # The screen's "issued" label is not presumed equivalent to IsActive alone.
    # Report raw active rows plus an explicit issue-date-based candidate count.
    issue_dates = pd.to_datetime(bonds["IssueDate"], errors="coerce", utc=True)
    cutoff = pd.Timestamp(run_time.date(), tz="UTC")
    active = bonds["pilot_status"].eq("active")
    bonds["pilot_issue_bucket"] = "unknown_issue_date"
    bonds.loc[issue_dates.le(cutoff), "pilot_issue_bucket"] = "issued_by_pull_date"
    bonds.loc[issue_dates.gt(cutoff), "pilot_issue_bucket"] = "future_issue_date"
    candidate = int((active & issue_dates.le(cutoff)).sum())
    inactive_count = int(bonds["pilot_status"].eq("inactive").sum())
    summary["count_comparisons"] = {
        "active_all_search_rows": int(active.sum()),
        "active_issued_by_pull_date": candidate,
        "active_future_issue_date": int((active & issue_dates.gt(cutoff)).sum()),
        "active_unknown_issue_date": int((active & issue_dates.isna()).sum()),
        "workspace_active_issued": expected_active_issued,
        "active_issued_count_matches": candidate == expected_active_issued,
        "inactive_search_rows": inactive_count,
        "workspace_inactive": expected_inactive,
        "inactive_count_matches": None if expected_inactive is None else inactive_count == expected_inactive,
    }
    bonds.groupby(["pilot_status", "pilot_issue_bucket"], dropna=False).size().rename(
        "records").reset_index().to_csv(os.path.join(output_dir, "counts_by_status.csv"), index=False)
    bonds.groupby(["IssuerCommonName", "pilot_status", "AssetStatus"], dropna=False).size().rename(
        "records").reset_index().to_csv(os.path.join(output_dir, "counts_by_issuer.csv"), index=False)
    print("Count comparison:", summary["count_comparisons"])
    print("Workspace's 309 to-be-issued bonds and 26 loans are separate screen categories.")

    ids = bonds["lookup_identifier"].dropna().astype(str).drop_duplicates().tolist()
    flags = []
    for start in range(0, len(ids), 100):
        batch = ids[start:start + 100]
        try:
            result = ld.get_data(universe=batch, fields=["TR.IsTLACEligible"])
            if result is None or len(result.columns) != 2:
                raise RuntimeError("Expected input identifier plus one TLAC field.")
            result.to_csv(os.path.join(output_dir, f"tlac_batch_{start:06d}.csv"), index=False)
            # Same two-column convention as the successful market-cap script.
            result.columns = ["Instrument", "TR.IsTLACEligible"]
            flags.append(result[["Instrument", "TR.IsTLACEligible"]])
            print(f"TLAC requested for {start + len(batch):,} of {len(ids):,} identifiers")
        except Exception as exc:
            # Do not print exception text, which might include connection details.
            summary["tlac_batch_errors"].append({"start": start, "size": len(batch),
                                                   "error_type": type(exc).__name__})
            print(f"TLAC batch starting at {start} failed ({type(exc).__name__}); recorded as missing.")
    if flags:
        flag_table = pd.concat(flags, ignore_index=True)
        if flag_table["Instrument"].duplicated().any():
            raise RuntimeError("Duplicate lookup keys in TLAC results; inspect saved batches before joining.")
        bonds = bonds.merge(flag_table, how="left", left_on="lookup_identifier",
                            right_on="Instrument", validate="many_to_one")
    else:
        bonds["TR.IsTLACEligible"] = pd.NA
    bonds.to_csv(os.path.join(output_dir, "bonds_with_tlac.csv"), index=False)
    summary["tlac_flag_counts"] = {
        str(key): int(value) for key, value in
        bonds["TR.IsTLACEligible"].fillna("MISSING").value_counts().items()
    }
    summary["completed"] = not summary["tlac_batch_errors"]
    print("TLAC results:", summary["tlac_flag_counts"])
    print("Count equality alone is not an identifier-list reconciliation.")
finally:
    with open(os.path.join(output_dir, "summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    ld.close_session()
    print("Output folder:", output_dir)
