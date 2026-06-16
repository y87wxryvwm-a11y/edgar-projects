# Copy this file to config.py (git-ignored) and set the values for this machine.

# SEC requires a real contact in the User-Agent; generic UAs get 403'd.
USER_AGENT = "Your Name your@email.com"

# Folder where outputs and this project's own cache live. Paste from your file
# explorer; backslashes are fine, no trailing slash.
DATA_DIR = r"C:\path\to\registrant-count-data"

# OPTIONAL — reuse an existing cache (its `indexes/` and `headers/` subfolders)
# so the build runs offline instead of re-fetching every header from EDGAR.
# Point this at the shares-outstanding-census cache if you have it; leave the
# list empty to fetch everything fresh into DATA_DIR/cache.
SEED_CACHE_DIRS = [
    # r"C:\path\to\shares-outstanding-census-data\cache",
]

# OPTIONAL — the census population_<year>.csv, used by the verify script for an
# independent completeness + shared-column cross-check. Leave "" to skip it.
CENSUS_POPULATION_CSV = ""

# OPTIONAL — the census public_float_<year>.csv. Public float is the size signal
# behind the rules-defined defaults for the accelerated-filer (afs) and smaller-
# reporting-company (src) columns where a filing leaves the box blank. Leave ""
# to fall back to NAF / not-SRC defaults with no size upgrade.
FLOAT_CSV = ""
