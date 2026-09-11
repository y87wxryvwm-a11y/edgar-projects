"""Add dated LSEG company market caps to the original registrant CSV."""

# ---- EDIT THIS --------------------------------------------------------------
valuation_date = "2025-12-31"
# -----------------------------------------------------------------------------

try:
    from config import INPUT_CSV
except ImportError:
    raise RuntimeError(
        "config.py not found or INPUT_CSV missing. Copy config.example.py "
        "to config.py and set INPUT_CSV to the full input CSV path."
    ) from None

import os

import pandas as pd
import lseg.data as ld


def normalize_cik(values):
    """Build matching keys while retaining the original CSV's CIK column."""
    return (
        values.astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .replace("", pd.NA)
        .str.zfill(10)
    )


def main():
    input_path = INPUT_CSV.replace("\\", "/")
    input_stem = os.path.splitext(os.path.basename(input_path))[0]
    output_path = os.path.join(
        os.getcwd(), f"{input_stem}_market_cap_{valuation_date}.csv"
    )

    original = pd.read_csv(input_path, dtype={"CIK": "string"})
    requests = original[["CIK", "form"]].copy()
    forms = (
        requests["form"]
        .astype("string")
        .str.strip()
        .str.upper()
        .str.replace("-", "", regex=False)
    )
    selected = forms.isin(["20F", "20F/A", "40F", "40F/A"])
    ciks = (
        normalize_cik(requests.loc[selected, "CIK"])
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    if not ciks:
        raise ValueError("No CIKs found for 20F, 20F/A, 40F, or 40F/A.")
    if "market_cap_usd" in original.columns:
        raise ValueError("The input CSV already contains market_cap_usd.")

    # Workspace must be open and signed in on this machine.
    ld.open_session()
    try:
        batches = []
        for start in range(0, len(ciks), 100):
            batch = ciks[start:start + 100]
            values = ld.get_data(
                universe=batch,
                fields=["TR.CompanyMarketCap"],
                parameters={
                    "SDate": valuation_date,
                    "Curn": "USD",
                    "Scale": 0,
                },
            )
            # Response: input instrument followed by the requested field.
            values.columns = ["CIK", "market_cap_usd"]
            batches.append(values)
            print(f"Requested {start + len(batch):,} of {len(ciks):,} CIKs")
    finally:
        ld.close_session()

    market_caps = pd.concat(batches, ignore_index=True)
    market_caps["CIK"] = normalize_cik(market_caps["CIK"])
    market_caps["market_cap_usd"] = pd.to_numeric(
        market_caps["market_cap_usd"], errors="coerce"
    )
    market_caps = market_caps.dropna(subset=["CIK"])

    # Merge normalized keys, then append only the new value to the original.
    # This retains original column values, repeated CIK rows, and row order.
    matched = pd.DataFrame({"CIK": normalize_cik(original["CIK"])}).merge(
        market_caps, on="CIK", how="left", sort=False, validate="many_to_one"
    )
    output = original.copy()
    output["market_cap_usd"] = matched["market_cap_usd"].to_numpy()
    output.to_csv(output_path, index=False)

    print(f"\nSaved {len(output):,} rows to: {output_path}")
    print(
        "CIKs with market capitalization returned: "
        f"{market_caps['market_cap_usd'].notna().sum():,} of {len(ciks):,}"
    )


if __name__ == "__main__":
    main()
