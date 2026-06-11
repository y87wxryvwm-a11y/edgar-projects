#!/usr/bin/env python3
"""
Independent reader establishing ground truth for batch_011 share counts.
Reads JSONL, analyzes cover_text for each packet, produces verdicts.
"""
import json
import re
from datetime import datetime
from pathlib import Path

def extract_date_from_period(period_of_report):
    """Convert YYYYMMDD to YYYY-MM-DD."""
    if not period_of_report or len(period_of_report) != 8:
        return ""
    try:
        dt = datetime.strptime(period_of_report, "%Y%m%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return ""

def analyze_packet(packet):
    """Analyze a single filing packet's cover_text for share disclosures."""
    accession = packet.get("accession", "")
    company_name = packet.get("company_name", "")
    form = packet.get("form", "")
    period_of_report = packet.get("period_of_report", "")
    cover_text = packet.get("cover_text", "")
    extracted_rows = packet.get("extracted_rows", [])

    # Convert period to date
    as_of_date = extract_date_from_period(period_of_report)

    # Check if cover_text is empty or truncated
    if not cover_text or len(cover_text) < 100:
        return {
            "accession": accession,
            "verdict": "CANT_TELL",
            "true_rows": [],
            "note": "Cover text missing or too short"
        }

    # Parse ground truth from cover_text
    true_rows = []
    verdict = "CORRECT"  # default
    note = ""

    # Pattern for "number of outstanding shares of each class" phrase
    # 10-K: "As of [date], there were X shares of [Class] outstanding"
    # 20-F/40-F: typically "...number of outstanding shares of each of the issuer's classes..."

    # Look for common disclosure patterns
    # Pattern 1: "X shares of [Class] outstanding as of [date]"
    pattern1 = r'(\d+(?:,\d{3})*(?:\.\d+)?)\s+shares?\s+of\s+([^,.\n]+?)\s+(?:common\s+)?stock\s+outstanding'

    # Pattern 2: "X shares of [Class] outstanding (no date in pattern, use period_of_report)"
    pattern2 = r'(\d+(?:,\d{3})*(?:\.\d+)?)\s+shares?\s+of\s+([^,.\n]+?)\s+(?:out)?standing'

    # Pattern 3: "outstanding shares of each class of [Issuer]'s" - look nearby for counts
    pattern3 = r'outstanding\s+shares?\s+of\s+each\s+(?:of\s+)?the\s+(?:issuer\'s|(?:corporation|company)\'s)\s+classes?'

    matches1 = list(re.finditer(pattern1, cover_text, re.IGNORECASE))

    if matches1:
        for match in matches1:
            value_str = match.group(1).replace(",", "")
            class_label = match.group(2).strip()
            true_rows.append({
                "value": value_str,
                "class_label": class_label,
                "share_type": "common",
                "as_of": as_of_date
            })
    else:
        # Try pattern 2
        matches2 = list(re.finditer(pattern2, cover_text, re.IGNORECASE))
        if matches2:
            for match in matches2:
                value_str = match.group(1).replace(",", "")
                class_label = match.group(2).strip()
                true_rows.append({
                    "value": value_str,
                    "class_label": class_label,
                    "share_type": "common",
                    "as_of": as_of_date
                })

    # If still no matches, look for more generic patterns
    if not true_rows:
        # Look for lines with "shares" and numbers
        lines = cover_text.split('\n')
        for line in lines:
            if 'outstanding' in line.lower() and 'shares' in line.lower():
                # Try to extract number
                num_match = re.search(r'(\d+(?:,\d{3})*(?:\.\d+)?)\s+shares', line, re.IGNORECASE)
                if num_match:
                    value_str = num_match.group(1).replace(",", "")
                    # Try to get class name from same line
                    class_match = re.search(r'of\s+([^,.\n]+?)\s+(?:stock|share)', line, re.IGNORECASE)
                    if class_match:
                        class_label = class_match.group(1).strip()
                    else:
                        class_label = "common stock"
                    true_rows.append({
                        "value": value_str,
                        "class_label": class_label,
                        "share_type": "common",
                        "as_of": as_of_date
                    })

    # Determine verdict
    if not true_rows and not extracted_rows:
        verdict = "CORRECT"
        note = "No shares disclosed; extractor correctly produced empty result"
    elif not true_rows and extracted_rows:
        verdict = "NO_SHARES_DISCLOSED"
        note = "Extractor produced rows but cover discloses no share count"
    elif true_rows and not extracted_rows:
        verdict = "MISSING_ROWS"
        note = "Cover discloses shares but extractor missed them"
    elif true_rows and extracted_rows:
        # Check if rows match
        if rows_match(true_rows, extracted_rows):
            verdict = "CORRECT"
        else:
            # Could be EXTRA_ROWS, MISSING_ROWS, or WRONG_VALUES
            verdict = "WRONG_VALUES"
            note = "Extracted values differ from cover text"

    return {
        "accession": accession,
        "verdict": verdict,
        "true_rows": true_rows,
        "note": note
    }

def rows_match(true_rows, extracted_rows):
    """Simple check if extracted rows match true rows."""
    if len(true_rows) != len(extracted_rows):
        return False
    for true_row in true_rows:
        found = False
        for ext_row in extracted_rows:
            if (ext_row.get("value") == true_row["value"] and
                ext_row.get("class_label") == true_row["class_label"]):
                found = True
                break
        if not found:
            return False
    return True

def main():
    input_file = Path("/Users/avilae/claude-code/projects/edgar-projects/shares-outstanding-census/data/evidence/2025/batch_011.jsonl")
    output_file = Path("/Users/avilae/claude-code/projects/edgar-projects/shares-outstanding-census/data/evidence/2025/verdicts_batch_011.json")

    verdicts = []

    with open(input_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            try:
                packet = json.loads(line.strip())
                verdict = analyze_packet(packet)
                verdicts.append(verdict)
                print(f"Line {line_num}: {packet.get('accession', 'UNKNOWN')} -> {verdict['verdict']}")
            except json.JSONDecodeError as e:
                print(f"Error parsing line {line_num}: {e}")
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")

    # Write output
    with open(output_file, 'w') as f:
        json.dump(verdicts, f, indent=2)

    # Summary
    verdict_counts = {}
    for v in verdicts:
        verdict_type = v["verdict"]
        verdict_counts[verdict_type] = verdict_counts.get(verdict_type, 0) + 1

    print(f"\nBatch: batch_011")
    print(f"Packets analyzed: {len(verdicts)}")
    print(f"Verdicts: {verdict_counts}")
    print(f"Output: {output_file}")

if __name__ == "__main__":
    main()
