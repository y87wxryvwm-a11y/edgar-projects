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

FETCH_BYTES      = 1_000_000   # bytes streamed per filing (first pass)
FETCH_BYTES_DEEP = 6_000_000   # retry limit when TOC anchors appear to be beyond first pass
ANCHOR_THRESHOLD = 3           # if anchors ≤ this after first pass, re-fetch at deeper limit

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


def _stream(html_url: str, limit: int) -> bytes:
    """Stream up to `limit` bytes from html_url."""
    resp = eu.get(html_url, stream=True)
    content = b''
    for chunk in resp.iter_content(chunk_size=65536):
        content += chunk
        if len(content) >= limit:
            break
    resp.close()
    return content


def _search_anchors(soup, html_url: str):
    """
    Search all fragment anchors in soup for an Item 9 match.
    Returns (full_url, status) or None if not found.

    Three match strategies (in order):
      1. Anchor text contains "Item 9" or "Offer and Listing"  (standard labelled TOC)
      2. Anchor href contains "item9"                          (href-only reference)
      3. Parent <tr> row text contains "Item 9" or "Offer and  (page-number-only TOC,
         Listing" for anchors whose own text is just a number   e.g. Flex LNG pattern)
    """
    anchors = soup.find_all('a', href=True)
    for a in anchors:
        href = a.get('href', '')
        text = a.get_text(strip=True)
        if not href.startswith('#'):
            continue
        # Strategy 1 & 2: standard text/href match
        if ITEM9_PAT.search(text) or ITEM9_PAT.search(href) or TITLE_PAT.search(text):
            return html_url + href, 'found'
        # Strategy 3: check parent <tr> when anchor text looks like a page number
        if text.isdigit():
            row = a.find_parent('tr')
            if row:
                row_text = row.get_text(separator=' ', strip=True)
                if ITEM9_PAT.search(row_text) or TITLE_PAT.search(row_text):
                    return html_url + href, 'found'
    return None


def find_item9_link(html_url: str) -> tuple[str, str]:
    """
    Return (item9_url, status) for a single filing.

    status values:
      'found'      — fragment located; item9_url is the full clickable link
      'NOT FOUND'  — anchors present but no Item 9 match in the fetched portion
      'CUSTOM_TOC' — anchors present but none use SEC item numbering (e.g. custom
                     annual report format); Item 9 is not identifiable from the TOC
      'LEGACY'     — no <a> tags found; image-based or non-navigable filing
      'NO_URL'     — html_url is blank
      'ERROR: …'   — network or parse failure
    """
    if not html_url or str(html_url).strip() in ('', 'nan'):
        return '', 'NO_URL'
    try:
        # --- First pass (1MB) ---
        content = _stream(html_url, FETCH_BYTES)
        soup    = BeautifulSoup(content, 'html.parser')
        anchors = soup.find_all('a', href=True)

        if not anchors:
            return '', 'LEGACY'

        result = _search_anchors(soup, html_url)
        if result:
            return result

        # --- Adaptive deep pass ---
        # If very few anchors were found the TOC is likely beyond the first fetch
        # window (Lufax pattern). Re-fetch at a higher limit and try again.
        if len(anchors) <= ANCHOR_THRESHOLD:
            content = _stream(html_url, FETCH_BYTES_DEEP)
            soup    = BeautifulSoup(content, 'html.parser')
            result  = _search_anchors(soup, html_url)
            if result:
                return result
            # Re-check anchor count after deep fetch
            anchors = soup.find_all('a', href=True)
            if not anchors:
                return '', 'LEGACY'

        # Anchors exist but no item 9 found — distinguish CUSTOM_TOC from NOT FOUND.
        # CUSTOM_TOC: fragment anchors are present but none contain any SEC item
        # label (no "item" keyword anywhere in text/href of fragment links), which
        # suggests a custom annual report TOC with proprietary section titles.
        frag_anchors = [a for a in soup.find_all('a', href=True)
                        if a.get('href', '').startswith('#')]
        has_any_item_label = any(
            re.search(r'\bitem\b', a.get_text(strip=True), re.IGNORECASE) or
            re.search(r'\bitem\b', a.get('href', ''), re.IGNORECASE)
            for a in frag_anchors
        )
        if frag_anchors and not has_any_item_label:
            return '', 'CUSTOM_TOC'

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

    found      = statuses.count('found')
    not_found  = statuses.count('NOT FOUND')
    custom_toc = statuses.count('CUSTOM_TOC')
    legacy     = statuses.count('LEGACY')
    errors     = sum(1 for s in statuses if s.startswith('ERROR'))
    navigable  = found + not_found + custom_toc
    hit_rate   = f"{100 * found / navigable:.1f}%" if navigable else "n/a"

    print(f"\nResults: {found} found / {not_found} not found / {custom_toc} custom_toc / {legacy} legacy / {errors} errors")
    print(f"Hit rate (excl. legacy + custom_toc): {found}/{navigable} = {hit_rate}")
    print(f"Saved → {out_path}")
    return df


if __name__ == "__main__":
    df = main()
