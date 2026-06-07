# config.example.py — copy this file to config.py and fill in both values.
# config.py is gitignored and never pushed, so each machine keeps its own paths
# and a `git pull` never overwrites them. Copy once per machine, then every
# script in this folder runs as-is.
#
# 1) USER_AGENT — the SEC requires a real name + contact email in every request's
#    User-Agent header per https://www.sec.gov/os/accessing-edgar-data .
#    Generic UAs get 403'd.
#
# 2) DATA_DIR — the folder that holds the data files these scripts read and write
#    (separate from this code folder): the filing index, the sample, the
#    extraction results, and the validation outputs. Paste the path from your
#    file explorer; Windows backslashes are fine (the scripts normalize them).

USER_AGENT = "Your Name your.email@example.com"
DATA_DIR = r"C:\path\to\your\shares-outstanding\data\folder"
