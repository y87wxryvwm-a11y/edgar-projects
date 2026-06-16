# column_config.py
#
# How the dashboard treats each column. This is the ONE place to edit. The
# dashboard ALWAYS shows the original column name and the original values — it
# never renames a variable or relabels a value. This file only assigns a column
# a ROLE (how it's filtered/charted) and, optionally, a fixed value ORDER.
#
# role:
#   "category" — a few-to-many distinct values; gets facets + charts
#   "flag"     — a 0/1 column; treated like a 2-value category
#   "number"   — numeric; range filter + aggregations
#   "date"     — ISO date; range filter
#   "id"       — an identifier; table only, never charted
#   "url"      — a link; table only (rendered clickable), never charted
#   "ignore"   — hide the column entirely
#
# Columns not listed here are auto-detected, so the tool still works on a CSV
# with many columns it has never seen.

# ---- EDIT THIS --------------------------------------------------------------

COLUMN_OVERRIDES = {
    "CIK": {"role": "id"},
    "Company Period": {"role": "date"},
    "Filing Date": {"role": "date"},
    "SIC": {"role": "category"},
    "State": {"role": "category"},
    "State Incorporated": {"role": "category"},
    "Accession Number": {"role": "id"},
    "BDC": {"role": "flag"},
    "ABS": {"role": "flag"},
    "multi": {"role": "flag"},
    "text_url": {"role": "url"},
    "filing_url": {"role": "url"},
    "wksi": {"role": "flag"},
    "shell": {"role": "flag"},
    "afs": {"role": "category", "order": ["LAF", "AF", "NAF"]},
    "src": {"role": "flag"},
    "egc": {"role": "flag"},
    "sec_12b": {"role": "flag"},
    "sec_12g": {"role": "flag"},
    "sec_15d": {"role": "flag"},
}

# Derived columns — computed in the browser, only when ALL their source columns
# are present. Each names a built-in rule.
#   "registration" : sec_12b/12g/15d -> "12(b)" / "12(g)" / "15(d)"
DERIVED_COLUMNS = [
    {
        "name": "Registration", "rule": "registration",
        "sources": ["sec_12b", "sec_12g", "sec_15d"],
        "order": ["12(b)", "12(g)", "15(d)", "(none)"],
    },
]

# Which columns get a prebuilt breakdown card/chart on the Overview tab, in
# order. Names not present in the loaded data are silently skipped. High-
# cardinality breakdowns (e.g. State) show their top 10 with an expand control.
OVERVIEW_BREAKDOWNS = [
    "afs", "Registration", "src", "egc", "wksi", "shell",
    "BDC", "ABS", "multi", "State",
]

# Thresholds the dashboard uses to auto-classify any column NOT listed above.
AUTODETECT = {
    "max_category_cardinality": 25,      # <= this many distinct -> small category
    "max_searchable_cardinality": 2000,  # <= this -> searchable category; above -> id/text
}

# -----------------------------------------------------------------------------


def get_config():
    """Bundle the config for the manifest the dashboard reads."""
    return {
        "column_overrides": COLUMN_OVERRIDES,
        "derived_columns": DERIVED_COLUMNS,
        "overview_breakdowns": OVERVIEW_BREAKDOWNS,
        "autodetect": AUTODETECT,
    }
