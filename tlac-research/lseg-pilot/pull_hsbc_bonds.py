"""Resumable HSBC name-search census and batched Workspace reference pull."""

# ---- EDIT THIS --------------------------------------------------------------
company_search = "HSBC"
run_folder = "hsbc_bonds_2026_09_22"  # Keep to resume; change for a fresh snapshot.
batch_size = 100
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("Set DATA_DIR in this folder's config.py.") from None

import hashlib
import json
import os
import time
from datetime import date, timedelta, datetime, timezone

import pandas as pd
import lseg.data as ld

directory = DATA_DIR.replace("\\", "/")
root = os.path.join(directory, run_folder)
os.makedirs(root, exist_ok=True)

COLUMNS = {
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
    "instrument_type": "TR.FiInstrumentTypeDescription",
    "is_convertible": "TR.FiIsConvertible",
    "offering_type": "TR.FiOfferingTypeDescription",
    "domicile": "TR.FiDomicile",
    "issuer": "TR.FiIssuerName",
    "tlac_eligible": "TR.IsTLACEligible",
}
FIELDS = list(COLUMNS.values())
SELECT = "ISIN,MainSuperRIC,AssetStatusDescription,IssuerCommonName,IssueDate"
PAGE_SIZE = 500
PAUSE_SECONDS = 4
# Local estimated usage across this script's runs in DATA_DIR, rolling 24 hours.
# These are deliberately small budgets, NOT measurements of account quota.
MAX_CALLS = 500
MAX_CELLS = 300_000
LEDGER = os.path.join(directory, "hsbc_pull_usage.json")


def save_json(path, value):
    with open(path + ".tmp", "w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2)
    os.replace(path + ".tmp", path)


def save_csv(path, frame):
    frame.to_csv(path + ".tmp", index=False)
    os.replace(path + ".tmp", path)


def reserve_request(cells):
    """Count attempts before sending; do not retry failures automatically."""
    now = time.time()
    events = json.load(open(LEDGER, encoding="utf-8")) if os.path.exists(LEDGER) else []
    events = [event for event in events if now - event["time"] < 86400]
    if len(events) >= MAX_CALLS or sum(e["cells"] for e in events) + cells > MAX_CELLS:
        raise RuntimeError(
            "Local rolling-24-hour pull budget reached. Saved batches are safe. "
            "Run again after earlier requests age out (up to 24 hours)."
        )
    time.sleep(PAUSE_SECONDS)
    events.append({"time": time.time(), "cells": cells})
    save_json(LEDGER, events)


def search_interval(lower=None, upper=None, missing=False):
    """Page within 10,000 rows; split saturated date intervals and start again."""
    if missing:
        expression = "IssueDate eq null"
    else:
        clauses = []
        if lower is not None:
            clauses.append(f"IssueDate ge {lower.isoformat()}")
        if upper is not None:
            clauses.append(f"IssueDate lt {upper.isoformat()}")
        expression = " and ".join(clauses) or "IssueDate ne null"
    pages = []
    for skip in range(0, 10000, PAGE_SIZE):
        key = hashlib.sha256(f"{expression}|{skip}".encode()).hexdigest()[:20]
        path = os.path.join(root, f"search_{key}.csv")
        if os.path.exists(path):
            frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        else:
            reserve_request(PAGE_SIZE * len(SELECT.split(",")))
            frame = ld.discovery.search(
                view=ld.discovery.Views.GOV_CORP_INSTRUMENTS,
                query=company_search, filter=expression, top=PAGE_SIZE, skip=skip,
                order_by="IssueDate asc,MainSuperRIC asc", select=SELECT,
            )
            if frame is None:
                raise RuntimeError("Search returned None; not treating this as an empty page.")
            if frame.empty:
                frame = pd.DataFrame(columns=SELECT.split(","))
            save_csv(path, frame)
            save_json(path + ".json", {
                "query": company_search, "filter": expression, "skip": skip,
                "retrieved_utc": datetime.now(timezone.utc).isoformat(),
            })
            print(f"Saved search page: {len(frame):,} rows; {expression}; offset {skip}")
        pages.append(frame)
        if len(frame) < PAGE_SIZE:
            return pd.concat(pages, ignore_index=True)
    if missing:
        raise RuntimeError("10,000 undated results: additional partitioning needed; search incomplete.")
    left = lower or date.min
    right = upper or date.max
    if (right - left).days <= 1:
        raise RuntimeError("10,000 results within one day; search incomplete, cannot safely truncate.")
    midpoint = left + timedelta(days=(right - left).days // 2)
    print(f"Splitting search at {midpoint}; cached parent pages retained for audit.")
    return pd.concat([
        search_interval(lower, midpoint), search_interval(midpoint, upper)
    ], ignore_index=True)


def main():
    if not isinstance(batch_size, int) or not 1 <= batch_size <= 100:
        raise ValueError("batch_size must be between 1 and 100.")
    settings = {"query": company_search, "fields": FIELDS, "batch_size": batch_size,
                "select": SELECT, "page_size": PAGE_SIZE, "version": 1}
    manifest = os.path.join(root, "settings.json")
    if os.path.exists(manifest) and json.load(open(manifest, encoding="utf-8")) != settings:
        raise RuntimeError("Settings changed. Use a new run_folder to avoid mixing cached data.")
    save_json(manifest, settings)
    ld.open_session()
    try:
        universe_path = os.path.join(root, "bond_search_identifiers.csv")
        if os.path.exists(universe_path):
            results = pd.read_csv(universe_path, dtype=str, keep_default_na=False)
        else:
            # Broad date buckets include old, future and undated issues. No status filter.
            results = pd.concat([
                search_interval(upper=date(1970, 1, 1)),
                search_interval(date(1970, 1, 1), date(2030, 1, 1)),
                search_interval(lower=date(2030, 1, 1)),
                search_interval(missing=True),
            ], ignore_index=True).fillna("")
            if results.empty:
                raise RuntimeError("No HSBC search results; no reference requests sent.")
            save_csv(universe_path, results)
        isin = results.get("ISIN", pd.Series("", index=results.index)).replace("", pd.NA)
        ric = results.get("MainSuperRIC", pd.Series("", index=results.index)).replace("", pd.NA)
        results["lookup_identifier"] = isin.fillna(ric)
        save_csv(os.path.join(root, "search_rows_without_identifier.csv"),
                 results[results["lookup_identifier"].isna()])
        universe = results.dropna(subset=["lookup_identifier"]).drop_duplicates("lookup_identifier")
        identifiers = sorted(universe["lookup_identifier"].tolist())
        if not identifiers:
            raise RuntimeError("Search returned no usable identifiers.")
        print(f"Search rows: {len(results):,}; distinct lookup identifiers: {len(identifiers):,}.")
        print(f"Details: {len(FIELDS)} fields, up to {batch_size} bonds per batch. Cached batches are reused.")
        batches = []
        for start in range(0, len(identifiers), batch_size):
            requested = identifiers[start:start + batch_size]
            path = os.path.join(root, f"details_{start // batch_size + 1:05d}.csv")
            if os.path.exists(path):
                values = pd.read_csv(path, dtype=str, keep_default_na=False)
            else:
                reserve_request(len(requested) * (len(FIELDS) + 1))
                values = ld.get_data(universe=requested, fields=FIELDS, header_type=ld.HeaderType.NAME)
                if values is None or values.empty:
                    raise RuntimeError("Empty detail response; stopping without marking batch complete.")
                values = values.rename(columns=lambda label: str(label).upper())
                save_csv(path + ".received.csv", values)
                if "INSTRUMENT" not in values or values["INSTRUMENT"].duplicated().any():
                    raise RuntimeError("Missing or duplicate Instrument keys; inspect received batch.")
                if set(values["INSTRUMENT"]) != set(requested):
                    raise RuntimeError("Incomplete or unexpected response identifiers; inspect received batch.")
                missing_fields = [f for f in FIELDS if f.upper() not in values]
                if missing_fields:
                    raise RuntimeError(f"Missing response columns: {missing_fields}. Received batch saved.")
                save_json(path + ".json", {
                    "identifiers": requested, "retrieved_utc": datetime.now(timezone.utc).isoformat()
                })
                os.replace(path + ".received.csv", path)
                print(f"Saved detail batch {start // batch_size + 1}: {start + len(requested):,}/{len(identifiers):,}")
            batches.append(values)
        raw = pd.concat(batches, ignore_index=True).set_index("INSTRUMENT")
        output = universe.set_index("lookup_identifier").reindex(identifiers)
        for label, field in COLUMNS.items():
            output[label] = raw[field.upper()].reindex(output.index)
        output = output.rename(columns={"AssetStatusDescription": "asset_status",
                                        "IssuerCommonName": "search_issuer"})
        save_csv(os.path.join(root, "hsbc_bond_details.csv"), output.reset_index())
        summary = output.assign(
            tlac_status=output["tlac_eligible"].fillna("").replace("", "NULL")
        ).groupby(["issuer", "tlac_status"], dropna=False).size().unstack(fill_value=0)
        save_csv(os.path.join(root, "issuer_tlac_counts.csv"), summary.reset_index())
        print(summary.to_string())
        print("Combined file:", os.path.join(root, "hsbc_bond_details.csv"))
        print("Name search exhausted. Group coverage still requires Workspace/public-list comparison.")
    finally:
        ld.close_session()


if __name__ == "__main__":
    main()
