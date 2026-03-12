"""
fetch_section_links.py
For a CSV of EDGAR filings (must have an html_url column), fetches each
filing's table of contents and extracts a direct anchor link to a target
section (default: Item 9. The Offer and Listing).

Appends an item9_url column and a status column to the CSV and saves the result.

Approach:
  - Streams the first 1MB of each filing's HTML — enough to cover the TOC
    for virtually all modern 20-F filers without downloading the full document
  - If no <a> tags are found in 1MB the filing is image-based / legacy (LEGACY)
  - Matches by: "Item 9" in link text, "Item 9" in href, or "Offer and Listing"
    in link text — covers both explicitly labelled TOCs and title-only templates
"""

import os
import re
import sys
import time

import pandas as pd
from bs4 import BeautifulSoup

import edgar_utils as eu

# =============================================================================
# CONFIGURATION — only edit this section
# =============================================================================

INPUT_CSV   = "20-F_2025_2026.csv"   # path relative to this script, or absolute
OUTPUT_CSV  = "20-F_2025_2026_item9.csv"

FETCH_BYTES = 1_000_000   # bytes streamed per filing (covers TOC for ~all modern filers)

REQUESTS_PER_MINUTE = 15  # raise to go faster, lower to be safer
                           # 15  → one request every 4s  (conservative, recommended)
                           # 30  → one request every 2s
                           # 60  → one request every 1s

# =============================================================================
# END OF CONFIGURATION
# =============================================================================

_DELAY = 60.0 / REQUESTS_PER_MINUTE   # seconds to sleep between requests

ITEM9_PAT = re.compile(r'item\s*9(?![a-zA-Z])', re.IGNORECASE)
TITLE_PAT = re.compile(r'offer\s+and\s+listing',  re.IGNORECASE)


def find_item9_link(html_url: str) -> tuple[str, str]:
    """
    Return (item9_url, status) for a single filing.

    status values:
      'found'     — fragment located; item9_url is the full clickable link
      'NOT FOUND' — anchors present but no Item 9 match in the fetched portion
      'LEGACY'    — no <a> tags found; image-based or non-navigable filing
      'NO_URL'    — html_url is blank
      'ERROR: …'  — network or parse failure
    """
    if not html_url or str(html_url).strip() in ('', 'nan'):
        return '', 'NO_URL'
    try:
        resp = eu.get(html_url, stream=True)
        content = b''
        for chunk in resp.iter_content(chunk_size=65536):
            content += chunk
            if len(content) >= FETCH_BYTES:
                break
        resp.close()

        soup    = BeautifulSoup(content, 'html.parser')
        anchors = soup.find_all('a', href=True)

        if not anchors:
            return '', 'LEGACY'

        for a in anchors:
            href = a.get('href', '')
            text = a.get_text(strip=True)
            if not href.startswith('#'):
                continue
            if ITEM9_PAT.search(text) or ITEM9_PAT.search(href) or TITLE_PAT.search(text):
                return html_url + href, 'found'

        return '', 'NOT FOUND'

    except Exception as e:
        return '', f'ERROR: {e}'


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    in_path    = INPUT_CSV  if os.path.isabs(INPUT_CSV)  else os.path.join(script_dir, INPUT_CSV)
    out_path   = OUTPUT_CSV if os.path.isabs(OUTPUT_CSV) else os.path.join(script_dir, OUTPUT_CSV)

    df = pd.read_csv(in_path, dtype=str)
    print(f"Loaded {len(df)} rows from {in_path}")

    if 'html_url' not in df.columns:
        raise ValueError("Input CSV must have an 'html_url' column.")

    item9_urls = []
    statuses   = []

    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(df), desc="Fetching Item 9 links", unit="filing",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
    except ImportError:
        pbar = None

    for _, row in df.iterrows():
        url, status = find_item9_link(str(row.get('html_url', '')))
        item9_urls.append(url)
        statuses.append(status)
        if pbar:
            pbar.update(1)
        time.sleep(_DELAY)

    if pbar:
        pbar.close()

    df['item9_url']    = item9_urls
    df['item9_status'] = statuses

    df.to_csv(out_path, index=False)

    found     = statuses.count('found')
    not_found = statuses.count('NOT FOUND')
    legacy    = statuses.count('LEGACY')
    errors    = sum(1 for s in statuses if s.startswith('ERROR'))
    navigable = found + not_found
    hit_rate  = f"{100 * found / navigable:.1f}%" if navigable else "n/a"

    print(f"\nResults: {found} found / {not_found} not found / {legacy} legacy / {errors} errors")
    print(f"Hit rate (excl. legacy): {found}/{navigable} = {hit_rate}")
    print(f"Saved → {out_path}")
    return df


if __name__ == "__main__":
    df = main()
