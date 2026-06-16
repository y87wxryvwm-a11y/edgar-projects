"""Cover-page reads that fill or correct the registrant-count status flags.

Each entry is a blind, human-style read of the actual filing cover page (the
73 filings whose dei XBRL tag was absent and whose text scrape missed it, plus
the handful whose as-filed value was an extractor artifact). Reads were done by
independent agents over the cached cover text; HST Global, whose 10-K is a
scanned-image filing, was read from the page images directly.

Keys per filing: afs in {LAF,AF,NAF}; wksi/shell/src/egc in {"0","1"}; reg in
{12b,12g,15d}. Only the cells that DIFFER from the as-filed extraction are
listed, so an entry is a minimal, evidenced correction set. afs="NAF" with the
"no filer-category box checked" note is the user's rule applied (NAF is the
default unless a Large/Accelerated box is marked) — e.g. voting-trust-interest
10-Ks. _evidence quotes the supporting cover text; _name is for humans.
"""

OVERRIDES = {
    "0000205402-25-000010": {  # GRAYBAR ELECTRIC CO INC
        "afs": "NAF",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | This Form 10-K relates to Voting Trust Interests and follows the rescinded Form 16-K item structure; no filer-...",
        },
    },
    "0000797564-25-000003": {  # HST Global, Inc.
        "afs": "NAF",
        "src": "1",
        "egc": "1",
        "reg": "12g",
        "_evidence": {
            "afs": "Non-accelerated filer [x] (image p3)",
            "src": "Smaller reporting company [x] (image p3)",
            "egc": "Emerging growth Company [x] (image p3)",
            "reg": "12(b) None; 12(g) Common Stock (image p2)",
        },
    },
    "0000918545-25-000002": {  # BALTIC INTERNATIONAL USA INC
        "afs": "NAF",
        "shell": "1",
        "src": "1",
        "_evidence": {
            "afs": "\"Non-accelerated filer [X] Smaller reporting company [X]\" (Large accelerated filer [ ] Accelerated filer [ ])",
            "shell": "\"Indicate by check mark whether the registrant is a shell company (as defined in Rule 12b-2 of the Act). Yes [X] No [ ]\"",
            "src": "\"Non-accelerated filer [X] Smaller reporting company [X]\"",
        },
    },
    "0000919574-25-003266": {  # Euroholdings Ltd.
        "afs": "NAF",
        "wksi": "0",
        "shell": "0",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large accelerated filer ☐ / Accelerated filer ☐ / Non-accelerated filer ☐ / Emerging growth company ☒\" — none...",
            "wksi": "\"Indicate by check mark if the registrant is a well-known seasoned issuer, as defined by Rule 405 of the Securities Act. ☐ Yes ☒ No\" — No is checked.",
            "shell": "\"If this is an annual report, indicate by check mark whether the registrant is a shell company (as defined in Rule 12b-2 of the Exchange Act). ☐ Yes ☒ No\" — ...",
        },
    },
    "0000950170-25-040572": {  # Bitwise Bitcoin ETF
        "afs": "NAF",
        "_evidence": {
            "afs": "\"Large Accelerated Filer ☐ ... Accelerated Filer ☐ ... Non-Accelerated Filer ☒\" — only Non-Accelerated Filer is marked with ☒.",
        },
    },
    "0000950170-25-045909": {  # CROSS TIMBERS ROYALTY TRUST
        "afs": "NAF",
        "src": "1",
        "_evidence": {
            "afs": "\"Large accelerated filer ☐ / Accelerated filer ☐ / Non-accelerated filer [U+F0FE checked box] / Smaller reporting company [U+F0FE] / Emerging growth company ...",
            "src": "\"Smaller reporting company\" line is immediately followed by U+F0FE (the filled/checked Wingdings box, same glyph that marks Non-accelerated filer as checked)...",
        },
    },
    "0000950170-25-047645": {  # HUGOTON ROYALTY TRUST
        "afs": "NAF",
        "src": "1",
        "_evidence": {
            "afs": "\"Large accelerated filer / ☐ / Accelerated filer / ☐ / Non-accelerated filer / [blank — glyph where check sits] / Smaller reporting company / [blank] / Emerg...",
            "src": "\"Smaller reporting company\" (line 47) followed by a marked glyph position (line 48 blank, i.e. not the ☐ shown on the three explicitly-unchecked categories),...",
        },
    },
    "0001104659-25-025600": {  # Permianville Royalty Trust
        "afs": "NAF",
        "src": "1",
        "_evidence": {
            "afs": "\"Non-accelerated filer / x\" (lines 66-67); \"Large accelerated filer / ¨\" (62-63) and \"Accelerated filer / ¨\" (64-65) both unchecked",
            "src": "\"Smaller reporting company / x\" (lines 68-69) — marked glyph",
        },
    },
    "0001104659-25-029616": {  # Empire District Bondco, LLC
        "afs": "NAF",
        "_evidence": {
            "afs": "\"¨ Large accelerated filer / ¨ Accelerated filer / x Non-accelerated filer\" — Non-accelerated filer bears the checked \"x\" glyph; the other two carry the unch...",
        },
    },
    "0001104659-25-029919": {  # Franklin BSP Real Estate Debt, Inc.
        "afs": "NAF",
        "src": "1",
        "egc": "1",
        "_evidence": {
            "afs": "\"Non-accelerated filer / x\" (lines 63-64), with \"Large accelerated filer / ¨\" (59-60) and \"Accelerated filer / ¨\" (61-62) both unchecked.",
            "src": "\"Smaller reporting company / x\" (lines 65-66) — checked.",
            "egc": "\"Emerging growth company / x\" (lines 67-68) — checked.",
        },
    },
    "0001104659-25-046353": {  # StratCap Digital Infrastructure REIT, In
        "afs": "NAF",
        "src": "1",
        "egc": "1",
        "_evidence": {
            "afs": "\"Non-Accelerated Filer / x\" (lines 66-67), with \"Large Accelerated Filer / ¨\" (62-63) and \"Accelerated Filer / ¨\" (64-65) unchecked.",
            "src": "\"Smaller reporting company / x\" (lines 68-69).",
            "egc": "\"Emerging growth company / x\" (lines 70-71).",
        },
    },
    "0001193125-25-067248": {  # Brazil Potash Corp.
        "afs": "NAF",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large accelerated filer ☐ / Accelerated filer ☐ / Non-accelerated filer ☐ / Emerging Growth Company ☒\" — all ...",
        },
    },
    "0001193125-25-078014": {  # AUNA S.A.
        "afs": "NAF",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large Accelerated Filer ☐ / Accelerated Filer ☐ / Non-accelerated Filer ☐ / Emerging growth company ☒\" — none...",
        },
    },
    "0001199835-25-000103": {  # SEAFARER EXPLORATION CORP
        "afs": "NAF",
        "src": "1",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large accelerated filer o\" / \"Accelerated filer o\" / \"Non-accelerated Filer o\" (lines 64-72) — all three glyp...",
            "src": "\"Smaller reporting company x\" (lines 73-75) — marked with checked glyph \"x\".",
        },
    },
    "0001199835-25-000302": {  # NAPC Defense, Inc.
        "afs": "NAF",
        "src": "1",
        "reg": "15d",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large accelerated filer o\" / \"Accelerated filer o\" / \"Non-accelerated filer o\" (lines 71-79) — all three boxe...",
            "src": "\"Smaller reporting company\" followed by \"x\" (lines 80-82) — checked.",
            "reg": "\"None / Securities registered under Section 12(b) of the Exchange Act\" (lines 46-48) and \"None / Securities registered under Section 12(g) of the Exchange Ac...",
        },
    },
    "0001213900-25-015041": {  # Click Holdings Ltd.
        "afs": "NAF",
        "wksi": "0",
        "shell": "0",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large accelerated filer ☐ / Accelerated filer ☐ / Non-accelerated filer ☐\" — all three boxes are unchecked; o...",
            "wksi": "\"Indicate by check mark if the registrant is a well-known seasoned issuer... ☐ Yes ☒ No\" — No is checked (☒ next to No).",
            "shell": "\"If this is an annual report, indicate by check mark whether the registrant is a shell company (as defined in Rule 12b-2 of the Exchange Act). ☐ Yes ☒ No\" — ...",
        },
    },
    "0001213900-25-037772": {  # AMTD IDEA GROUP
        "afs": "AF",
        "wksi": "0",
        "shell": "0",
        "_evidence": {
            "afs": "\"Large Accelerated Filer ☐ / Accelerated Filer ☒ / Non-Accelerated Filer ☐ / Emerging Growth Company ☐\" — Accelerated Filer is the marked box.",
            "wksi": "\"Indicate by check mark if the registrant is a well-known seasoned issuer, as defined in Rule 405 of the Securities Act. ☐ Yes ☒ No\" — No is checked.",
            "shell": "\"If this is an annual report, indicate by check mark whether the registrant is a shell company (as defined in Rule 12b-2 of the Exchange Act). ☐ Yes ☒ No\" — ...",
        },
    },
    "0001213900-25-038113": {  # Brooge Energy Ltd
        "afs": "AF",
        "_evidence": {
            "afs": "\"Large accelerated filer ☐\" / \"Accelerated filer ☒\" / \"Non-accelerated filer ☐\" (lines 74-76) — Accelerated filer is the marked box.",
        },
    },
    "0001213900-25-077507": {  # Kyivstar Group Ltd.
        "egc": "1",
        "reg": "12b",
        "_evidence": {
            "egc": "\"☒ Emerging growth company\" (line 84) — marked with ☒.",
            "reg": "\"Securities registered or to be registered, pursuant to Section 12(b) of the Act ... Common Shares / KYIV / The Nasdaq Global Select Market ... Warrants / KY...",
        },
    },
    "0001214659-25-005928": {  # Kuber Resources Corp
        "afs": "NAF",
        "src": "1",
        "egc": "1",
        "_evidence": {
            "afs": "\"Non-accelerated filer / x\" (lines 62-63); Large accelerated filer ¨ (60-61), Accelerated filer ¨ (64-65) both unchecked",
            "src": "\"Smaller reporting company / x\" (lines 66-67)",
            "egc": "\"Emerging growth company / x\" (lines 68-69)",
        },
    },
    "0001262463-25-000185": {  # PREAXIA HEALTH CARE PAYMENT SYSTEMS INC.
        "afs": "NAF",
        "src": "1",
        "_evidence": {
            "afs": "\"Large accelerated filer [ ]\" / \"Accelerated filer [ ]\" / \"Non-accelerated filer [X]\"",
            "src": "\"Smaller reporting company [X]\"",
        },
    },
    "0001477932-25-003940": {  # American Resources Corp
        "afs": "NAF",
        "wksi": "0",
        "shell": "0",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large accelerated filer ☐ ... Accelerated filer ☐ ... Non-accelerated Filer ☐ ... Smaller reporting company ☒...",
            "wksi": "\"Indicate by check mark if the registrant is a well-known seasoned issuer, as defined in Rule 405 of the Securities Act. ☐ Yes ☒ No\"",
            "shell": "\"Indicate by check mark whether the registrant is a shell company (as defined in Rule 12b-2 of the Act). ☐ Yes ☒ No\"",
        },
    },
    "0001477932-25-006766": {  # Karbon-X Corp.
        "afs": "NAF",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large accelerated filer ☐ ... Accelerated filer ☐ ... Non-accelerated filer ☐ ... Smaller reporting company ☒...",
        },
    },
    "0001493152-25-018188": {  # OZ VISION INC.
        "afs": "NAF",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large accelerated filer ☐ Accelerated filer ☐ Non-accelerated filer ☐ Smaller reporting company ☒\" — all thre...",
        },
    },
    "0001641172-25-001291": {  # TOFUTTI BRANDS INC
        "afs": "NAF",
        "_evidence": {
            "afs": "no filer-category box checked -> NAF default | \"Large accelerated filer ☐ ... Accelerated filer ☐ ... Non-accelerated filer ☐ (Do not check if a smaller repo...",
        },
    },
    "0001641172-25-017758": {  # Adapti, Inc.
        "afs": "NAF",
        "wksi": "0",
        "shell": "0",
        "_evidence": {
            "afs": "\"Large accelerated filer ☐ / Accelerated filer ☐ / Non-accelerated filer ☐ / Smaller reporting company ☒ / Emerging Growth Company ☐\" — among the three accel...",
            "wksi": "\"Indicate by check mark whether the registrant is a well-known seasoned issuer, as defined in Rule 405 of the Securities Act. ☐ Yes ☒ No\"",
            "shell": "\"Indicate by check mark whether the registrant is a shell company (as defined in Rule 12b-2 of the Act). ☐ Yes ☒ No\"",
        },
    },
    "0001641172-25-020115": {  # Deep Green Waste & Recycling, Inc.
        "shell": "0",
        "_evidence": {
            "shell": "\"Indicate by check mark whether the registrant is a shell company ... ☐ Yes ☒ No\" — No is checked.",
        },
    },
    "0001999538-25-000012": {  # X1 Capital Inc.
        "afs": "NAF",
        "egc": "1",
        "_evidence": {
            "afs": "\"Large accelerated filer / ¨ / Accelerated filer / ¨ / Non-accelerated filer / þ (do not check if a smaller reporting company)\" — only Non-accelerated filer ...",
            "egc": "\"Emerging growth company / þ\" — marked þ (checked).",
        },
    },
    "0002026478-25-000013": {  # Newsmax Inc.
        "afs": "NAF",
        "src": "1",
        "egc": "1",
        "_evidence": {
            "afs": "Non-accelerated filer / x (Large accelerated filer o; Accelerated filer o; Non-accelerated filer x)",
            "src": "\"Smaller reporting company\" followed by \"x\"",
            "egc": "\"Emerging growth company\" followed by \"x\"",
        },
    },
    "0002030781-25-000011": {  # SailPoint, Inc.
        "afs": "NAF",
        "_evidence": {
            "afs": "\"Large accelerated filer ☐ ... Accelerated filer ☐ ... Non-accelerated filer x\"",
        },
    },
    "0002032966-25-000010": {  # ACUREN CORP
        "afs": "NAF",
        "egc": "1",
        "_evidence": {
            "afs": "\"Large accelerated filer / o / Accelerated filer / o / Non-accelerated filer / x\" (Non-accelerated filer is the marked glyph 'x'; the other two are 'o')",
            "egc": "\"Emerging growth company / x\"",
        },
    },
    "0002036042-25-000005": {  # Sionna Therapeutics, Inc.
        "afs": "NAF",
        "src": "1",
        "egc": "1",
        "_evidence": {
            "afs": "\"Non-accelerated filer / x\" (lines 47-48); \"Large accelerated filer / o\" and \"Accelerated filer / o\" both unchecked",
            "src": "\"Smaller reporting company / x\" (lines 49-50)",
            "egc": "\"Emerging growth company / x\" (lines 51-52)",
        },
    },
}
