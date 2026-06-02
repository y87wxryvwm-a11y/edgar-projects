# config.example.py — copy this file to config.py and set DATA_DIR.
# config.py is gitignored and never pushed, so each machine keeps its own path
# and a `git pull` never overwrites it. Copy once per machine, then every proxy
# script in this folder runs as-is.
#
# DATA_DIR is the folder that holds the proxy data files the scripts read and
# write (it is separate from this code folder):
#   synthetic_proxy.csv, dataset_shape.csv, proxy_summary_stats.csv,
#   table1_*.csv, table2_*.csv
# Paste the folder path from your file explorer; Windows backslashes are fine
# (the scripts normalize them).

DATA_DIR = r"C:\path\to\your\proxy\data\folder"
