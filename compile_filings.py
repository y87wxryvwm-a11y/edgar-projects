"""
compile_filings.py
Fetch EDGAR filings for one or more form types across one or more years and
quarters, enrich with SGML header metadata, and save to CSV.
"""

import os
import edgar_utils as eu

# =============================================================================
# CONFIGURATION — only edit this section
# =============================================================================

FORM_TYPES    = ["20-F"]   # e.g. ["10-K", "20-F", "10-Q", "8-K", "20-F/A"]

YEAR_QUARTERS = {          # map each year to the quarters you want
    2025: [4], 2026: [1],             # e.g. 2025: [1, 2, 3, 4], 2026: [1]
}

# =============================================================================
# END OF CONFIGURATION
# =============================================================================


def main():
    for year, quarters in YEAR_QUARTERS.items():
        qs = "Q" + "Q".join(str(q) for q in sorted(quarters))
        print(f"  {year}  {qs}")
    print(f"Fetching {FORM_TYPES} for:")

    df = eu.get_filings_bulk(FORM_TYPES, YEAR_QUARTERS)
    print(f"{len(df)} filings retrieved")

    df = eu.enrich_with_sgml(df)

    forms_str = "_".join(f.replace("/", "") for f in FORM_TYPES)
    years     = sorted(YEAR_QUARTERS.keys())
    if len(years) == 1:
        year = years[0]
        quarters_str = "Q" + "Q".join(str(q) for q in sorted(YEAR_QUARTERS[year]))
        filename = f"{forms_str}_{year}_{quarters_str}.csv"
    else:
        filename = f"{forms_str}_{years[0]}_{years[-1]}.csv"

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} rows → {out_path}")
    return df


if __name__ == "__main__":
    df = main()  # assigned at module level so it appears in Spyder's Variable Explorer
