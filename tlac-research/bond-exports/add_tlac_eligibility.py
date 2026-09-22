"""Add LSEG TLAC flags to a cleaned company CSV, preserving every input row."""

# ---- EDIT THIS --------------------------------------------------------------
input_filename = "HSBC_Holdings.csv"
isin_column = "ISIN"
issuer_column = "Issuer"
# -----------------------------------------------------------------------------
try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set DATA_DIR."
    ) from None

directory = DATA_DIR.replace("\\", "/")

import os
from datetime import datetime, timezone

import pandas as pd
import lseg.data as ld

FIELD = "TR.IsTLACEligible"
BATCH_SIZE = 100


def find_column(frame, requested):
    matches = [name for name in frame if name.strip().casefold() == requested.strip().casefold()]
    if len(matches) != 1:
        raise ValueError(f"Cannot uniquely find {requested!r}. CSV columns: {list(frame.columns)}")
    return matches[0]


def normalize(values):
    return values.astype("string").str.strip().str.upper()


def fetch_flags(identifiers):
    """Join by returned Instrument, never by response row order."""
    records = []
    for start in range(0, len(identifiers), BATCH_SIZE):
        batch = identifiers[start:start + BATCH_SIZE]
        response = ld.get_data(
            universe=batch, fields=[FIELD], header_type=ld.HeaderType.NAME,
        )
        if response is None or response.empty:
            raise RuntimeError(f"No response rows for batch starting at {start + 1}; no output written.")
        response = response.rename(columns=lambda name: str(name).strip().upper())
        if not {"INSTRUMENT", FIELD.upper()}.issubset(response.columns):
            raise RuntimeError(f"Unexpected LSEG columns: {list(response.columns)}; no output written.")
        response["INSTRUMENT"] = normalize(response["INSTRUMENT"])
        if response["INSTRUMENT"].duplicated().any():
            raise RuntimeError("LSEG returned multiple rows per ISIN; no output written.")
        if not response["INSTRUMENT"].isin(batch).all():
            raise RuntimeError("LSEG returned identifiers different from the requested ISINs; no output written.")
        flags = normalize(response[FIELD.upper()]).fillna("")
        if not flags.isin(["Y", "N", ""]).all():
            raise RuntimeError(f"Unexpected TLAC values: {flags.unique().tolist()}; no output written.")
        by_id = pd.Series(flags.to_numpy(), index=response["INSTRUMENT"])
        for isin in batch:
            value = by_id.get(isin, "")
            status = ("no_response" if isin not in by_id.index else
                      "null" if value == "" else value)
            records.append({"tlac_lookup_isin": isin, "tlac_eligible": value,
                            "tlac_lookup_status": status})
        print(f"Requested {min(start + BATCH_SIZE, len(identifiers)):,} / {len(identifiers):,} distinct ISINs")
    return pd.DataFrame(records, columns=["tlac_lookup_isin", "tlac_eligible", "tlac_lookup_status"])


def enrich(frame, lookup, isin_name, pulled_at):
    output = frame.copy()
    output["tlac_lookup_isin"] = normalize(output[isin_name]).fillna("")
    output = output.merge(lookup, on="tlac_lookup_isin", how="left", sort=False, validate="many_to_one")
    output["tlac_eligible"] = output["tlac_eligible"].fillna("")
    output["tlac_lookup_status"] = output["tlac_lookup_status"].fillna("missing_isin")
    output["tlac_pulled_at_utc"] = pulled_at
    assert len(output) == len(frame), "Merge changed the input row count."
    return output


def main():
    path = os.path.join(directory, "bond_exports_cleaned", input_filename)
    # Preserve original text, identifiers, empty cells, and exported number formatting.
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    isin_name = find_column(frame, isin_column)
    issuer_name = find_column(frame, issuer_column)
    added = {"tlac_lookup_isin", "tlac_eligible", "tlac_lookup_status", "tlac_pulled_at_utc"}
    if added.intersection(frame.columns):
        raise ValueError("Input already has TLAC enrichment columns. Use the original cleaned CSV.")
    keys = normalize(frame[isin_name]).fillna("")
    identifiers = keys[keys.ne("")].drop_duplicates().tolist()
    print(f"Input rows: {len(frame):,}")
    print(f"Rows missing ISIN: {keys.eq('').sum():,}")
    print(f"Distinct nonblank ISINs to request: {len(identifiers):,}")
    print(f"Repeated ISIN rows beyond first occurrence: {keys.ne('').sum() - len(identifiers):,}")
    pulled_at = datetime.now(timezone.utc).isoformat()
    if identifiers:
        ld.open_session()
        try:
            lookup = fetch_flags(identifiers)
        finally:
            ld.close_session()
    else:
        lookup = pd.DataFrame(columns=["tlac_lookup_isin", "tlac_eligible", "tlac_lookup_status"])
    output = enrich(frame, lookup, isin_name, pulled_at)
    print("\nTLAC results (input rows / distinct requested ISINs):")
    for status in ["Y", "N", "null", "no_response", "missing_isin"]:
        rows = output["tlac_lookup_status"].eq(status).sum()
        distinct = lookup["tlac_lookup_status"].eq(status).sum()
        print(f"  {status}: {rows:,} / {distinct:,}")
    eligible = output.loc[output["tlac_eligible"].eq("Y")].copy()
    eligible["issuer"] = eligible[issuer_name].str.strip().replace("", "[missing issuer]")
    summary = eligible.groupby("issuer", dropna=False).agg(
        eligible_rows=("tlac_lookup_isin", "size"),
        eligible_distinct_isins=("tlac_lookup_isin", "nunique"),
    ).reset_index().sort_values("eligible_distinct_isins", ascending=False)
    print("\nIssuer names with Y flags (from the input export):")
    print(summary.to_string(index=False) if len(summary) else "None.")
    print("\nNull and no_response values are unknown, not N. Duplicate input rows are preserved.")
    destination = os.path.join(directory, "bond_exports_tlac")
    os.makedirs(destination, exist_ok=True)
    stem = os.path.splitext(os.path.basename(input_filename))[0]
    for data, filename in [(output, stem + "_tlac.csv"),
                           (summary, stem + "_tlac_issuers.csv")]:
        target = os.path.join(destination, filename)
        data.to_csv(target + ".tmp", index=False, encoding="utf-8-sig")
        os.replace(target + ".tmp", target)
        print("Saved:", target)


if __name__ == "__main__":
    main()
