"""Committed map from cover/XBRL row labels to registrant CIKs for the few
multi-registrant filings whose labels the general name matcher
(census_lib.match_label_to_filer) cannot connect to a FILER block —
abbreviations and initialisms the cover uses but the header doesn't
("OG&E" for OKLAHOMA GAS & ELECTRIC CO). Consumed by 7_build_final.py and
12_build_final_float.py.

Key: (accession, label exactly as it appears in the extraction/override
row). Value: the registrant's own CIK (no leading zeros), as listed in
that filing's SGML header FILER blocks. Every entry carries provenance.

Labels that are NOT registrants (fund series, tracking-stock groups,
former-entity axes) do not belong here — they stay in class_or_series
under the primary filer's CIK.
"""

ENTITY_ALIASES = {
    # Each entry verified against the filing's SGML header FILER blocks
    # (2026-06-12): the label is the cover's/XBRL member's abbreviation of
    # exactly one co-registrant; no other filer comes close.
    ("0000100517-25-000046", "United Air Lines Inc"): "319687",
    # "United Air Lines" (legacy spelling) = UNITED AIRLINES, INC.
    ("0000352541-25-000014", "Wpl"): "107832",
    # WPL = WISCONSIN POWER & LIGHT CO (Alliant combined 10-K)
    ("0000352541-25-000014", "Ipl"): "52485",
    # IPL = INTERSTATE POWER & LIGHT CO (Alliant combined 10-K)
    ("0000950170-25-022560", "Og And E"): "74145",
    # OG&E = OKLAHOMA GAS & ELECTRIC CO (OGE Energy combined 10-K)
    ("0000950170-25-022756", "Cusa"): "885975",
    # CUSA = CINEMARK USA INC /TX (Cinemark combined 10-K)
    ("0000950170-25-026874", "Public Service Electricand Gas Company"):
        "81033",
    # glued XBRL member = PUBLIC SERVICE ELECTRIC & GAS CO (PSEG)
    ("0001130310-25-000040", "Cerc Corp"): "1042773",
    # CERC = CENTERPOINT ENERGY RESOURCES CORP (CenterPoint combined 10-K)
    ("0001271833-25-000005", "CCOHoldings Capital Corp."): "1271834",
    # glued label = CCO HOLDINGS CAPITAL CORP (CCO Holdings co-issuer)
    ("0001326160-25-000072", "Piedmont"): "78460",
    # Piedmont = PIEDMONT NATURAL GAS CO INC (Duke combined 10-K)
    ("0001584207-25-000006", "One Main Finance Corporation"): "25598",
    # spaced label = ONEMAIN FINANCE CORP (OneMain combined 10-K)
    ("0001755672-25-000005", "EIDP"): "30554",
    # EIDP = EIDP, Inc., f/k/a E. I. du Pont (Corteva combined 10-K)
}
