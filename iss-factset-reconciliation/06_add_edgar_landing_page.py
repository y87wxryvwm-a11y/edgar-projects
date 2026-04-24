"""Add an edgar_landing_page URL column built from predicted_cik (zfilled to 10)."""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
save_directory = "/Users/avilae/claude-code/projects/edgar-projects/iss-factset-reconciliation"
input_filename = "iss_only_v2_with_predicted_cik.csv"
output_filename = "iss_only_v2_with_edgar_landing_page.csv"
cik_column = "predicted_cik"
url_column = "edgar_landing_page"
# -----------------------------------------------------------------------------

input_path = os.path.join(save_directory, input_filename)
output_path = os.path.join(save_directory, output_filename)

URL_TEMPLATE = "https://www.sec.gov/edgar/browse/?CIK={cik}"

df = pd.read_csv(input_path, dtype={cik_column: str})


def build_url(cik: str) -> str:
    if pd.isna(cik) or str(cik).strip() == "":
        return ""
    cik_clean = str(cik).strip().split(".")[0]  # drop any ".0" float artifact
    return URL_TEMPLATE.format(cik=cik_clean.zfill(10))


df[url_column] = df[cik_column].map(build_url)

built = (df[url_column] != "").sum()
print(f"Input rows:           {len(df):,}")
print(f"URLs built:           {built:,}")
print(f"Blank (no CIK):       {len(df) - built:,}")

df.to_csv(output_path, index=False)
print(f"Wrote {output_path}")
