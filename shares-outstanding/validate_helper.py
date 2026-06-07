"""validate_helper.py — neutral evidence dump for independent validation.

Usage:  python validate_helper.py <cik> <accession> <form>

Builds an evidence packet for a filing: the cover-page text, every 'outstanding'
context across the whole primary document, and SEC's structured
dei:EntityCommonStockSharesOutstanding fact. It deliberately does NOT call the
project's extractor (extract_* in shares_lib) — only the neutral fetch/text
layer — so a reviewer (human or sub-agent) can judge ground truth independently
of the regex being tested. `build_evidence` is reused by 3_dump_evidence.py.
"""

import re
import sys

import shares_lib as L


def build_evidence(session, cik, accession, form):
    filing = L.Filing(cik=cik, company="", form=form, date_filed="",
                      accession=accession,
                      filename=f"edgar/data/{int(cik)}/{accession}.txt")
    doc_type, raw, period = L.fetch_primary_document(session, filing)
    text = L.html_to_text(raw)
    cover = L.cover_region(text)
    out = []
    out.append(f"FORM={form}  DOC_TYPE={doc_type}  PERIOD_OF_REPORT={period}")
    out.append(f"FULL_TEXT_LEN={len(text):,}  COVER_LEN={len(cover):,}")
    out.append(f"TXT_URL={filing.txt_url}")
    out.append("\n===== COVER REGION (first 6000 chars) =====")
    out.append(cover[:6000])
    out.append("\n===== ALL 'outstanding' CONTEXTS IN FULL DOCUMENT (±200 chars, up to 14) =====")
    hits = list(re.finditer(r"outstanding", text, re.I))
    for i, m in enumerate(hits[:14]):
        a, b = max(0, m.start() - 200), min(len(text), m.end() + 120)
        out.append(f"[{i+1}] ...{re.sub(r'\\s+', ' ', text[a:b])}...")
    if not hits:
        out.append("(no 'outstanding' anywhere — filing may have no shares outstanding, e.g. an ABS trust)")
    out.append("\n===== dei:EntityCommonStockSharesOutstanding (SEC structured fact) =====")
    xbrl = L.fetch_xbrl_shares(session, cik)
    if not xbrl:
        out.append("(none — typical for foreign / multi-class / non-tagging filers)")
    else:
        for v in sorted(xbrl, key=lambda d: d.get("end", ""))[-6:]:
            out.append(f"  {v.get('val'):>18,}  end={v.get('end')}  form={v.get('form')}  fy={v.get('fy')}{v.get('fp','')}")
    return "\n".join(out)


def main():
    if len(sys.argv) != 4:
        print("usage: validate_helper.py <cik> <accession> <form>")
        sys.exit(2)
    session = L.build_session()
    print(build_evidence(session, sys.argv[1], sys.argv[2], sys.argv[3]))


if __name__ == "__main__":
    main()
