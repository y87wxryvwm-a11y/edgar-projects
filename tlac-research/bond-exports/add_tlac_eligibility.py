"""Add LSEG TLAC flags to a cleaned company CSV, preserving every input row."""

# ---- EDIT THIS --------------------------------------------------------------
input_filename = "HSBC_Holdings.csv"
isin_column = "ISIN"
issuer_column = "Issuer"
resume = True  # Reuse saved results; False starts a fresh pull.
# -----------------------------------------------------------------------------
try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set DATA_DIR."
    ) from None

directory = DATA_DIR.replace("\\", "/")

import os
import time
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


LOOKUP_COLUMNS = ["tlac_lookup_isin", "tlac_eligible", "tlac_lookup_status",
                  "tlac_pulled_at_utc", "tlac_error"]


def save_csv(frame, path):
    frame.to_csv(path + ".tmp", index=False, encoding="utf-8-sig")
    os.replace(path + ".tmp", path)


def fetch_flags(identifiers, checkpoint):
    """Save each completed request; split persistent HTTP 400 batches."""
    saved = pd.DataFrame(columns=LOOKUP_COLUMNS)
    if resume and os.path.exists(checkpoint):
        saved = pd.read_csv(checkpoint, dtype=str, keep_default_na=False)
        if not set(LOOKUP_COLUMNS).issubset(saved.columns) or saved["tlac_lookup_isin"].duplicated().any():
            raise ValueError("Invalid checkpoint; set resume=False for a fresh pull.")
        saved = saved.loc[saved["tlac_lookup_isin"].isin(identifiers) &
                          saved["tlac_lookup_status"].isin(["Y", "N", "null"])]
    records = saved.to_dict("records")
    done = set(saved["tlac_lookup_isin"])
    pending = [isin for isin in identifiers if isin not in done]
    print(f"Resuming {len(done):,} saved ISINs; {len(pending):,} still to request.")
    # Clear an old checkpoint on a deliberately fresh run, even if it then fails.
    save_csv(pd.DataFrame(records, columns=LOOKUP_COLUMNS), checkpoint)

    def request(batch):
        for attempt in range(2):
            try:
                response = ld.get_data(
                    universe=batch, fields=[FIELD], header_type=ld.HeaderType.NAME,
                )
                break
            except Exception as exc:
                if attempt == 0:
                    print(f"Request failed for {len(batch)} ISIN(s); retrying once.")
                    time.sleep(2)
                    continue
                # Other persistent errors (e.g. session/service failures) stop
                # with completed requests already saved, rather than fan out.
                if "400" not in str(exc):
                    raise
                if len(batch) > 1:
                    print(f"Persistent 400; splitting {len(batch)} ISINs to isolate the failure.")
                    middle = len(batch) // 2
                    request(batch[:middle])
                    request(batch[middle:])
                    return
                records.append(dict(zip(LOOKUP_COLUMNS, [batch[0], "", "request_error",
                    datetime.now(timezone.utc).isoformat(), str(exc)])))
                save_csv(pd.DataFrame(records, columns=LOOKUP_COLUMNS), checkpoint)
                print(f"Unresolved 400 for {batch[0]}; saved error and continuing.")
                return
        if response is None or response.empty:
            raise RuntimeError("Empty LSEG response. Completed requests are saved; rerun to resume.")
        response = response.rename(columns=lambda name: str(name).strip().upper())
        if not {"INSTRUMENT", FIELD.upper()}.issubset(response.columns):
            raise RuntimeError(f"Unexpected LSEG columns: {list(response.columns)}")
        response["INSTRUMENT"] = normalize(response["INSTRUMENT"])
        if response["INSTRUMENT"].duplicated().any():
            raise RuntimeError("LSEG returned multiple rows per ISIN.")
        if not response["INSTRUMENT"].isin(batch).all():
            raise RuntimeError("LSEG returned identifiers different from the requested ISINs.")
        flags = normalize(response[FIELD.upper()]).fillna("")
        if not flags.isin(["Y", "N", ""]).all():
            raise RuntimeError(f"Unexpected TLAC values: {flags.unique().tolist()}")
        by_id = pd.Series(flags.to_numpy(), index=response["INSTRUMENT"])
        timestamp = datetime.now(timezone.utc).isoformat()
        for isin in batch:
            value = by_id.get(isin, "")
            status = ("no_response" if isin not in by_id.index else
                      "null" if value == "" else value)
            records.append(dict(zip(LOOKUP_COLUMNS, [isin, value, status, timestamp, ""])))
        save_csv(pd.DataFrame(records, columns=LOOKUP_COLUMNS), checkpoint)

    for start in range(0, len(pending), BATCH_SIZE):
        request(pending[start:start + BATCH_SIZE])
        print(f"Saved {len(records):,} / {len(identifiers):,} distinct ISIN results")
    return pd.DataFrame(records, columns=LOOKUP_COLUMNS)


def enrich(frame, lookup, isin_name):
    output = frame.copy()
    output["tlac_lookup_isin"] = normalize(output[isin_name]).fillna("")
    output = output.merge(lookup, on="tlac_lookup_isin", how="left", sort=False, validate="many_to_one")
    output["tlac_eligible"] = output["tlac_eligible"].fillna("")
    output["tlac_lookup_status"] = output["tlac_lookup_status"].fillna("missing_isin")
    output["tlac_pulled_at_utc"] = output["tlac_pulled_at_utc"].fillna("")
    output["tlac_error"] = output["tlac_error"].fillna("")
    assert len(output) == len(frame), "Merge changed the input row count."
    return output


def main():
    path = os.path.join(directory, "bond_exports_cleaned", input_filename)
    # Preserve original text, identifiers, empty cells, and exported number formatting.
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    isin_name = find_column(frame, isin_column)
    issuer_name = find_column(frame, issuer_column)
    added = set(LOOKUP_COLUMNS)
    if added.intersection(frame.columns):
        raise ValueError("Input already has TLAC enrichment columns. Use the original cleaned CSV.")
    keys = normalize(frame[isin_name]).fillna("")
    identifiers = keys[keys.ne("")].drop_duplicates().tolist()
    print(f"Input rows: {len(frame):,}")
    print(f"Rows missing ISIN: {keys.eq('').sum():,}")
    print(f"Distinct nonblank ISINs to request: {len(identifiers):,}")
    print(f"Repeated ISIN rows beyond first occurrence: {keys.ne('').sum() - len(identifiers):,}")
    destination = os.path.join(directory, "bond_exports_tlac")
    os.makedirs(destination, exist_ok=True)
    stem = os.path.splitext(os.path.basename(input_filename))[0]
    checkpoint = os.path.join(destination, stem + "_tlac_checkpoint.csv")
    if identifiers:
        ld.open_session()
        try:
            lookup = fetch_flags(identifiers, checkpoint)
        finally:
            ld.close_session()
    else:
        lookup = pd.DataFrame(columns=LOOKUP_COLUMNS)
    output = enrich(frame, lookup, isin_name)
    print("\nTLAC results (input rows / distinct requested ISINs):")
    for status in ["Y", "N", "null", "no_response", "request_error", "missing_isin"]:
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
    print("\nNull, no_response and request_error are unknown, not N. Duplicate input rows are preserved.")
    errors = output.loc[output["tlac_lookup_status"].eq("request_error")]
    if len(errors):
        print(f"WARNING: {len(errors):,} rows have unresolved request errors; results are incomplete.")
    destination = os.path.join(directory, "bond_exports_tlac")
    os.makedirs(destination, exist_ok=True)
    stem = os.path.splitext(os.path.basename(input_filename))[0]
    for data, filename in [(output, stem + "_tlac.csv"),
                           (summary, stem + "_tlac_issuers.csv"),
                           (errors, stem + "_tlac_errors.csv")]:
        target = os.path.join(destination, filename)
        data.to_csv(target + ".tmp", index=False, encoding="utf-8-sig")
        os.replace(target + ".tmp", target)
        print("Saved:", target)


if __name__ == "__main__":
    main()
