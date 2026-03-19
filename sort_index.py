"""
sort_index.py
Reads a company metadata CSV and writes it back with a new `sort_index` column.
Sorting the output by sort_index (ascending) produces the canonical section order.

===============================================================
  CONFIGURATION  —  edit only this block
===============================================================
"""
INPUT_CSV  = "company_metadata.csv"
OUTPUT_CSV = "company_metadata.csv"        # overwrite in place; change if you want a new file
INDEX_COL  = "sort_index"                  # name of the new index column

GROUP_A = {
    "Cayman Islands", "China", "Hong Kong", "Cyprus", "Macau",
    "Guernsey", "Jersey", "Isle of Man", "Luxembourg", "Singapore",
    "Greece", "Gibraltar", "Monaco", "Bermuda",
    "British Virgin Islands", "United States", "Bahamas",
}

# item9_status sort order within TODO sections (lower = higher in output)
ITEM9_ORDER = {"NOT FOUND": 0, "N/A": 1, "found": 2}
"""
===============================================================
  END CONFIGURATION
===============================================================

Section order (sort_index lowest → highest):

  Section 1  foreign_listing != "TODO"
             sorted by hq_country, corp_country

  Section 2  TODO  |  Group B tier  |  NOT FOUND
  Section 3  TODO  |  Group B tier  |  N/A
  Section 4  TODO  |  Group B tier  |  found
  Section 5  TODO  |  Group A tier  |  NOT FOUND
  Section 6  TODO  |  Group A tier  |  N/A
  Section 7  TODO  |  Group A tier  |  found

  Group A tier = either hq_country OR corp_country is in GROUP_A
  Group B tier = both hq_country AND corp_country are NOT in GROUP_A

  Within every section: sorted by hq_country, then corp_country (A→Z).
"""

import csv


def sort_key(row):
    is_todo    = 1 if row.get("foreign_listing", "").strip() == "TODO" else 0
    hq         = row.get("hq_country",   "").strip()
    corp       = row.get("corp_country",  "").strip()
    item9      = row.get("item9_status",  "").strip()

    group_tier = 1 if (hq in GROUP_A or corp in GROUP_A) else 0
    i9_order   = ITEM9_ORDER.get(item9, 99)  # unknown values sort last

    return (is_todo, group_tier, i9_order, hq.lower(), corp.lower())


def main():
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if not rows:
        print("No data found in input file.")
        return

    # Assign a rank to each row based on sort_key, then map it back to the
    # original row order so the file's physical order is preserved.
    indexed = sorted(range(len(rows)), key=lambda i: sort_key(rows[i]))

    rank_map = {}  # original index → sort_index value
    for rank, orig_idx in enumerate(indexed, start=1):
        rank_map[orig_idx] = rank

    # Build output fieldnames: put sort_index first
    out_fields = [INDEX_COL] + [f for f in fieldnames if f != INDEX_COL]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        for i, row in enumerate(rows):
            row[INDEX_COL] = rank_map[i]
            writer.writerow(row)

    print(f"Done. Wrote {len(rows)} rows → {OUTPUT_CSV}")
    print(f"Sort by '{INDEX_COL}' ascending to get canonical order.")


if __name__ == "__main__":
    main()
