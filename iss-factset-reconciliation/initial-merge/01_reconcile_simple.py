import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
save_directory = "/Users/avilae/claude-code/projects/edgar-projects/iss-factset-reconciliation"
iss_filename = "ISS.csv"
fs_filename = "factset.csv"
# -----------------------------------------------------------------------------

iss_path = os.path.join(save_directory, iss_filename)
fs_path = os.path.join(save_directory, fs_filename)
iss_only_path = os.path.join(save_directory, "iss_only_v2.csv")
fs_only_path = os.path.join(save_directory, "fs_only_v2.csv")

iss = pd.read_csv(iss_path, dtype={"cusip_6": str})
fs  = pd.read_csv(fs_path, dtype={"cusip_6": str})

iss["Meeting_Date"] = pd.to_datetime(iss["Meeting_Date"])
fs["Meeting_Date"]  = pd.to_datetime(fs["Meeting_Date"])

key = ["cusip_6", "Meeting_Date"]

iss_only = iss.merge(fs[key].drop_duplicates(), on=key, how="left", indicator=True)
iss_only = iss_only[iss_only["_merge"] == "left_only"].drop(columns="_merge")

fs_only = fs.merge(iss[key].drop_duplicates(), on=key, how="left", indicator=True)
fs_only = fs_only[fs_only["_merge"] == "left_only"].drop(columns="_merge")

print(f"ISS total:       {len(iss):,}")
print(f"FactSet total:   {len(fs):,}")
print(f"ISS only:        {len(iss_only):,}")
print(f"FactSet only:    {len(fs_only):,}")

iss_only.to_csv(iss_only_path, index=False)
fs_only.to_csv(fs_only_path, index=False)
