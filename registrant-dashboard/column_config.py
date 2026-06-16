# column_config.py
#
# How the dashboard treats each column. This is the ONE place domain knowledge
# lives. Edit it in Spyder/any editor; the launcher reads it and hands it to the
# dashboard. Columns NOT listed here are auto-detected, so the tool still works
# on a CSV with many columns it has never seen.
#
# role:
#   "category" — a few-to-many distinct labels; gets facets + charts
#   "flag"     — a 0/1 (or yes/no) column; shown as Yes/No
#   "number"   — numeric; range filter + aggregations (sum/avg/min/max/median)
#   "date"     — ISO date; range filter + month histogram
#   "id"       — an identifier; table only, never charted
#   "url"      — a link; table only (rendered clickable), never charted
#   "ignore"   — hide the column entirely
#
# Anything you don't set, the dashboard infers. Anything here overrides it.

# ---- EDIT THIS --------------------------------------------------------------

COLUMN_OVERRIDES = {
    "CIK": {
        "role": "id", "label": "CIK",
        "definition": "SEC Central Index Key — the registrant's permanent ID. "
                      "The dataset is one row per CIK.",
    },
    "Company Period": {
        "role": "date", "label": "Period of report",
        "definition": "Fiscal period the annual report covers "
                      "(header CONFORMED PERIOD OF REPORT).",
    },
    "Filing Date": {
        "role": "date", "label": "Filing date",
        "definition": "Date the report was filed on EDGAR (filed-in-year).",
    },
    "SIC": {
        "role": "category", "label": "SIC code",
        "definition": "Standard Industrial Classification code (industry). "
                      "Grouped into divisions by the derived 'Industry' column.",
    },
    "State": {
        "role": "category", "label": "State (location)",
        "definition": "Business-address state/country, EDGAR code "
                      "(falls back to mailing address).",
    },
    "State Incorporated": {
        "role": "category", "label": "State of incorporation",
        "definition": "State/country of incorporation, EDGAR code.",
    },
    "Accession Number": {
        "role": "id", "label": "Accession number",
        "definition": "EDGAR accession (the filing's ID). Shared across "
                      "co-registrants that appear on one combined filing.",
    },
    "BDC": {
        "role": "flag", "label": "Business development company",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "1 if the registrant's SEC file number starts 814- (a BDC).",
    },
    "ABS": {
        "role": "flag", "label": "Asset-backed securities issuer",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "1 if SIC is 6189 (asset-backed securities).",
    },
    "multi": {
        "role": "flag", "label": "Combined (multi-registrant) filing",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "1 if the report carries more than one registrant.",
    },
    "text_url": {
        "role": "url", "label": "Full submission (.txt)",
        "definition": "URL of the full-submission raw .txt.",
    },
    "filing_url": {
        "role": "url", "label": "EDGAR filing index",
        "definition": "URL of the filing's EDGAR index page.",
    },
    "wksi": {
        "role": "flag", "label": "Well-known seasoned issuer",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "dei:EntityWellKnownSeasonedIssuer cover checkbox.",
    },
    "shell": {
        "role": "flag", "label": "Shell company",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "dei:EntityShellCompany cover checkbox.",
    },
    "afs": {
        "role": "category", "label": "Accelerated-filer status",
        "order": ["LAF", "AF", "NAF"],
        "value_labels": {"LAF": "Large accelerated", "AF": "Accelerated",
                         "NAF": "Non-accelerated"},
        "definition": "Accelerated-filer category from dei:EntityFilerCategory: "
                      "LAF (large accelerated), AF (accelerated), NAF "
                      "(non-accelerated). Never blank.",
    },
    "src": {
        "role": "flag", "label": "Smaller reporting company",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "dei:EntitySmallBusiness cover checkbox.",
    },
    "egc": {
        "role": "flag", "label": "Emerging growth company",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "dei:EntityEmergingGrowthCompany cover checkbox.",
    },
    "sec_12b": {
        "role": "flag", "label": "Registered under §12(b)",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "1 if a security is registered under Exchange Act §12(b) "
                      "(exchange-listed). See derived 'Registration'.",
    },
    "sec_12g": {
        "role": "flag", "label": "Registered under §12(g)",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "1 if no §12(b) but a security under §12(g).",
    },
    "sec_15d": {
        "role": "flag", "label": "Reporting under §15(d)",
        "value_labels": {"1": "Yes", "0": "No"},
        "definition": "1 otherwise (the §15(d) reporting default).",
    },
}

# Derived columns — computed in the browser, and only when ALL their source
# columns are present. Each names a built-in rule.
#   "registration"  : sec_12b/12g/15d -> "12(b)" / "12(g)" / "15(d)"
#   "sic_division"  : SIC -> the 10 SIC divisions (Manufacturing, Finance, ...)
DERIVED_COLUMNS = [
    {
        "name": "Registration", "rule": "registration",
        "sources": ["sec_12b", "sec_12g", "sec_15d"],
        "label": "Registration section",
        "order": ["12(b)", "12(g)", "15(d)", "(none)"],
        "definition": "Which Exchange Act section the registrant reports under, "
                      "rolled up from sec_12b/12g/15d (12(b) > 12(g) > 15(d)).",
    },
    {
        "name": "Industry", "rule": "sic_division",
        "sources": ["SIC"],
        "label": "Industry (SIC division)",
        "order": ["Agriculture, Forestry & Fishing", "Mining", "Construction",
                  "Manufacturing", "Transportation & Public Utilities",
                  "Wholesale Trade", "Retail Trade", "Finance, Insurance & Real Estate",
                  "Services", "Public Administration", "(unclassified)"],
        "definition": "SIC code grouped into the standard SIC divisions.",
    },
]

# Which columns get a prebuilt chart on the Overview tab, in order. Names not
# present in the loaded data are silently skipped.
OVERVIEW_BREAKDOWNS = [
    "afs", "Registration", "src", "egc", "wksi", "shell",
    "BDC", "ABS", "multi", "Industry", "State",
]

# Thresholds the dashboard uses to auto-classify any column NOT listed above.
AUTODETECT = {
    "max_category_cardinality": 25,    # <= this many distinct non-blank -> small category (checkbox facet)
    "max_searchable_cardinality": 2000,  # <= this -> searchable category facet; above -> treated as id/text
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
