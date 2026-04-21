"""
fetch_section_links.py
For a CSV of EDGAR filings (must have an html_url column), fetches each
filing's table of contents and extracts a direct anchor link to a target
section (default: Item 9. The Offer and Listing).

Appends an item9_url column and a status column to the CSV and saves the result.

Approach:
  - Streams the first 1MB of each filing's HTML — enough to cover the TOC
    for virtually all modern 20-F filers without downloading the full document
  - Matches by: "Item 9" in link text, "Item 9" in href, or "Offer and Listing"
    in link text — covers both explicitly labelled TOCs and title-only templates

Fallback behaviour for non-found rows:
  - 20-F / 20-F/A with status NOT FOUND → item9_url set to html_url (filing root)
  - 40-F rows → item9_url set to filing_url (index page); no HTML search attempted
  - All other form types → item9_url left blank; status N/A
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
      'NOT FOUND'  — fragment not identified; caller applies fallback url
      'NO_URL'     — html_url is blank
      'ERROR'      — network or parse failure (retried once; html_url used as fallback)
    """
    if not html_url or str(html_url).strip() in ('', 'nan'):
        return '', 'NO_URL'
    try:
        # --- First pass (1MB) ---
        content = _stream(html_url, FETCH_BYTES)
        soup    = BeautifulSoup(content, 'html.parser')
        anchors = soup.find_all('a', href=True)

        result = _search_anchors(soup, html_url)
        if result:
            return result

        # --- Adaptive deep pass ---
        # Triggered when anchors are absent or very sparse after the first fetch —
        # both signal that the TOC is likely beyond the 1MB window.
        if len(anchors) <= ANCHOR_THRESHOLD:
            content = _stream(html_url, FETCH_BYTES_DEEP)
            soup    = BeautifulSoup(content, 'html.parser')
            result  = _search_anchors(soup, html_url)
            if result:
                return result

        return '', 'NOT FOUND'

    except Exception:
        return '', 'ERROR'


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

    SEARCH_FORMS  = {'20-F', '20-F/A'}   # run full Item 9 search
    FALLBACK_FORMS = {'40-F'}             # skip search; use filing_url as item9_url

    for _, row in df.iterrows():
        form_type  = str(row.get('Form Type', '')).strip()
        html_url   = str(row.get('html_url', ''))
        filing_url = str(row.get('filing_url', ''))

        if form_type in FALLBACK_FORMS:
            # 40-F: point directly to the filing index page
            item9_urls.append(filing_url)
            statuses.append('N/A')

        elif form_type in SEARCH_FORMS:
            url, status = find_item9_link(html_url)
            if status == 'NOT FOUND':
                url = html_url      # fall back to filing root when fragment not found
            item9_urls.append(url)
            statuses.append(status)
            time.sleep(_DELAY)

        else:
            item9_urls.append('')
            statuses.append('N/A')

        if pbar:
            pbar.update(1)

    if pbar:
        pbar.close()

    # --- Retry pass for errors ---
    error_indices = [i for i, s in enumerate(statuses) if s == 'ERROR']
    if error_indices:
        print(f"\nRetrying {len(error_indices)} errored row(s)...")
        for i in error_indices:
            time.sleep(_DELAY * 2)   # longer pause before retry
            html_url = str(df.iloc[i].get('html_url', ''))
            url, status = find_item9_link(html_url)
            if status == 'NOT FOUND' or status == 'ERROR':
                url = html_url       # fall back to html_url regardless of outcome
            statuses[i]   = status
            item9_urls[i] = url

    df['item9_url']    = item9_urls
    df['item9_status'] = statuses

    df.to_csv(out_path, index=False)

    found     = statuses.count('found')
    not_found = statuses.count('NOT FOUND')
    skipped   = statuses.count('N/A')
    errors    = statuses.count('ERROR')
    searched  = found + not_found
    hit_rate  = f"{100 * found / searched:.1f}%" if searched else "n/a"

    print(f"\nResults: {found} found / {not_found} not found (fallback to html_url) / {skipped} skipped / {errors} errors")
    print(f"Hit rate: {found}/{searched} = {hit_rate}")
    print(f"Saved → {out_path}")
    return df


if __name__ == "__main__":
    df = main()
