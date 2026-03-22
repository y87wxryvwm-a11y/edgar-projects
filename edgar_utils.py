"""
edgar_utils.py — Utilities for SEC EDGAR data retrieval.
User-Agent is loaded from config.py (gitignored). See config.example.py.
Last verified: 2026-03-22
"""

import time
import threading
import requests
import requests.packages.urllib3
import pandas as pd
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress SSL warnings that arise when verify=False is used (e.g. on networks
# with a self-signed corporate proxy certificate).
requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)

try:
    from config import USER_AGENT
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py "
        "and fill in your name and email."
    )


class _RateLimiter:
    """Lock-based dispatch throttle. Targets 8 req/s to stay under EDGAR's 10 req/s cap."""
    def __init__(self, calls_per_second: float = 8.0):
        self._interval = 1.0 / calls_per_second
        self._lock     = threading.Lock()
        self._last     = 0.0

    def acquire(self):
        with self._lock:
            wait = self._interval - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()

_rate_limiter = _RateLimiter()


def get(url: str, retries: int = 3, **kwargs) -> requests.Response:
    """Rate-limited GET with SEC-required headers and 429 backoff. Use instead of requests.get()."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Host": urlparse(url).netloc,
    }
    for attempt in range(retries):
        _rate_limiter.acquire()
        resp = requests.get(url, headers=headers, verify=False, **kwargs)
        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"429 — waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Failed to fetch {url} after {retries} retries.")


def parallel_apply(df: pd.DataFrame, row_fn, workers: int = 8,
                   desc: str = "Processing") -> list:
    """
    Apply row_fn(row) to every row of df concurrently, returning results in original order.

    Rate limiting is enforced inside get(), so row_fn just calls get() normally.
    More workers don't increase throughput beyond the rate cap — they absorb latency variance.
    """
    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(df), desc=desc, unit="filing",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
    except ImportError:
        pbar = None

    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(row_fn, row): i for i, (_, row) in enumerate(df.iterrows())}
        for future in as_completed(futures):
            i = futures[future]
            try:
                results[i] = future.result()
            except Exception as e:
                print(f"  Row {i} failed: {e}")
            if pbar:
                pbar.update(1)

    if pbar:
        pbar.close()
    return results


_ticker_cache: dict | None = None

def ticker_to_cik(ticker: str) -> str:
    """Return a zero-padded 10-digit CIK for a ticker. Fetches and caches the full map once."""
    global _ticker_cache
    if _ticker_cache is None:
        raw = get("https://www.sec.gov/files/company_tickers.json").json()
        _ticker_cache = {
            v["ticker"].upper(): str(v["cik_str"]).zfill(10)
            for v in raw.values()
        }
    cik = _ticker_cache.get(ticker.upper())
    if cik is None:
        raise ValueError(f"Ticker '{ticker}' not found in EDGAR company list.")
    return cik


def format_accession(accession: str) -> dict:
    """
    Return both forms of an accession number.
        format_accession("0001234567-24-000001")
        → {"dashed": "0001234567-24-000001", "nodashes": "0001234567240000001"}
    """
    dashed = accession if "-" in accession else (
        f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
    )
    return {"dashed": dashed, "nodashes": dashed.replace("-", "")}


def get_filings_by_form(form_type: str, year: int, quarter: int) -> pd.DataFrame:
    """
    Return all filings of form_type for a given year/quarter, sorted by CIK.

    Parses crawler.idx — a fixed-width file (NOT pipe-delimited). Column positions
    are read dynamically from the header line; the URL is split on 'https://' to
    avoid the date field bleeding into it at a fixed offset.
    """
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/crawler.idx"
    lines = get(url).text.splitlines()

    header = lines[7]
    col = {
        'Company Name': 0,
        'Form Type':    header.index('Form Type'),
        'CIK':          header.index('CIK'),
        'Date Filed':   header.index('Date Filed'),
    }

    rows = []
    for line in lines[9:]:
        if not line.strip() or 'https://' not in line:
            continue
        fixed, filing_url = line.split('https://', 1)
        rows.append({
            'Company Name': fixed[col['Company Name']:col['Form Type']].strip(),
            'Form Type':    fixed[col['Form Type']:col['CIK']].strip(),
            'CIK':          fixed[col['CIK']:col['Date Filed']].strip(),
            'Date Filed':   fixed[col['Date Filed']:].strip(),
            'filing_url':   'https://' + filing_url.strip(),
        })

    df = pd.DataFrame(rows)
    df['CIK'] = df['CIK'].str.zfill(10)
    df['Accession Number'] = df['filing_url'].str.extract(r'/(\d{10}-\d{2}-\d{6})-index\.htm')[0]
    df = df[['CIK', 'Company Name', 'Form Type', 'Date Filed', 'Accession Number', 'filing_url']]
    return df[df['Form Type'] == form_type].sort_values('CIK').reset_index(drop=True)


def sgml_header_url(cik: str, accession_dashed: str) -> str:
    """Build the URL for a filing's .hdr.sgml file."""
    acc = format_accession(accession_dashed)
    return (
        f"https://www.sec.gov/Archives/edgar/data/{int(cik)}"
        f"/{acc['nodashes']}/{acc['dashed']}.hdr.sgml"
    )


def _parse_sgml_tag(text: str, tag: str) -> str:
    """Extract the value of a bare SGML tag (e.g. <PERIOD>20250930)."""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith(f"<{tag}>"):
            return line[len(tag) + 2:].strip()
    return ""


def enrich_with_sgml(df: pd.DataFrame, workers: int = 8, retries: int = 2) -> pd.DataFrame:
    """
    Fetch the SGML header for each filing and append Period, SIC, inc_state, state columns.
    Runs concurrently via parallel_apply(). Failed rows get empty strings.
    """
    empty = {"Period": "", "SIC": "", "inc_state": "", "state": ""}

    def fetch_row(row):
        url = sgml_header_url(row["CIK"], row["Accession Number"])
        for attempt in range(1 + retries):
            try:
                text = get(url).text
                return {
                    "Period":    _parse_sgml_tag(text, "PERIOD"),
                    "SIC":       _parse_sgml_tag(text, "ASSIGNED-SIC"),
                    "inc_state": _parse_sgml_tag(text, "STATE-OF-INCORPORATION"),
                    "state":     (_parse_sgml_tag(text, "STATE")
                                  or _parse_sgml_tag(text, "PROVINCE-COUNTRY")),
                }
            except Exception as e:
                if attempt < retries:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  Failed: {row['Accession Number']}: {e}")
        return empty.copy()

    records = parallel_apply(df, fetch_row, workers=workers, desc="Fetching SGML headers")
    records = [r if r is not None else empty.copy() for r in records]
    return pd.concat([df.reset_index(drop=True), pd.DataFrame(records)], axis=1)


def enrich_with_primary_doc(df: pd.DataFrame, workers: int = 8, retries: int = 2) -> pd.DataFrame:
    """
    Add an html_url column with the direct URL to each filing's primary HTML document.

    Fetches data.sec.gov/submissions/CIK{cik}.json — one request per unique CIK,
    regardless of how many filings that CIK has in df. Transient failures are retried
    up to `retries` times with exponential backoff. Accessions not found in the
    submissions 'recent' array (e.g. very old filings for prolific filers) get an
    empty string rather than raising.
    """
    unique_ciks = df['CIK'].unique()
    acc_to_doc: dict = {}

    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(unique_ciks), desc="Fetching primary docs", unit="CIK",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
    except ImportError:
        pbar = None

    def fetch_cik_docs(cik):
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        for attempt in range(1 + retries):
            try:
                recent = get(url).json()['filings']['recent']
                return dict(zip(recent['accessionNumber'], recent['primaryDocument']))
            except Exception as e:
                if attempt < retries:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  Warning: could not fetch submissions for CIK {cik}: {e}")
        return {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_cik_docs, cik): cik for cik in unique_ciks}
        for future in as_completed(futures):
            acc_to_doc.update(future.result())
            if pbar:
                pbar.update(1)

    if pbar:
        pbar.close()

    def build_url(row):
        doc = acc_to_doc.get(row['Accession Number'], '')
        if not doc:
            return ''
        cik_int = str(int(row['CIK']))
        acc_nd  = row['Accession Number'].replace('-', '')
        return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nd}/{doc}"

    out = df.copy()
    out['html_url'] = out.apply(build_url, axis=1)
    return out


def get_filings_bulk(form_types: list, year_quarters: dict) -> pd.DataFrame:
    """
    Fetch filings for one or more form types across one or more years and quarters.

    year_quarters maps each year to the list of quarters to fetch, e.g.:
        {2025: [1, 2, 3, 4], 2026: [1]}

    Returns one row per filing, deduplicated by Accession Number, sorted by CIK.
    Automatically enriches with an html_url column (primary HTML document) via
    enrich_with_primary_doc() — one submissions JSON request per unique CIK.
    """
    chunks = []
    for form_type in form_types:
        for year, quarters in year_quarters.items():
            for quarter in quarters:
                try:
                    chunks.append(get_filings_by_form(form_type, year, quarter))
                except Exception as e:
                    print(f"  Warning: could not fetch {form_type} {year} Q{quarter}: {e}")

    if not chunks:
        raise RuntimeError("No filings retrieved — check form types, year_quarters.")

    df = (
        pd.concat(chunks, ignore_index=True)
        .drop_duplicates(subset="Accession Number")
        .sort_values('CIK')
        .reset_index(drop=True)
    )
    return enrich_with_primary_doc(df)
