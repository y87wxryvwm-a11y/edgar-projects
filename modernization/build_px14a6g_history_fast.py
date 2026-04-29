"""Fast PX14A6G history extractor — uses urllib3 directly, no throttling.

Walks EDGAR backward from the current year, collects every PX14A6G filing,
extracts subject + filer + filing date from the SGML header, and writes a
CSV with one row per filing.

Same logic as build_px14a6g_history_subject_filer.py, but:
  - urllib3.PoolManager (HTTP keepalive, no per-request session overhead)
  - no time.sleep between requests
  - no retry backoff (urllib3 default retry only)
"""

from __future__ import annotations

import csv
import datetime as dt
import os
import re
from dataclasses import dataclass
from pathlib import Path

import urllib3

# ---------------------------------------------------------------------------
# OUTPUT CONFIG — change these to redirect where the CSV is written.
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path(__file__).parent
OUTPUT_FILENAME = "px14a6g_history_fast.csv"
# ---------------------------------------------------------------------------

FORM_TYPE = "PX14A6G"
EDGAR_FLOOR_YEAR = 1993
QUARTERS = (1, 2, 3, 4)

USER_AGENT = os.environ.get(
    "EDGAR_USER_AGENT",
    "Evan Avila evan.avila10@gmail.com",
)

HERE = Path(__file__).parent
IDX_CACHE = HERE / ".cache" / "edgar"
HEADER_CACHE = HERE / ".cache" / "edgar" / "headers"
OUTPUT_CSV = OUTPUT_DIR / OUTPUT_FILENAME

PATH_RE = re.compile(r"edgar/data/(\d+)/(\S+)\.txt")
SECTION_RE = re.compile(
    r"^(SUBJECT COMPANY|FILED BY|FILER|REPORTING-OWNER|ISSUER):\s*$",
    re.MULTILINE,
)
NAME_RE = re.compile(r"COMPANY CONFORMED NAME:\s*(.+?)\s*$", re.MULTILINE)
CIK_RE = re.compile(r"CENTRAL INDEX KEY:\s*(\d+)\s*$", re.MULTILINE)
DATE_RE = re.compile(r"FILED AS OF DATE:\s*(\d{8})\s*$", re.MULTILINE)


@dataclass
class FilingRef:
    cik: str
    accession: str

    @property
    def headers_url(self) -> str:
        no_dashes = self.accession.replace("-", "")
        return (
            f"https://www.sec.gov/Archives/edgar/data/{self.cik}/"
            f"{no_dashes}/{self.accession}-index-headers.html"
        )

    @property
    def txt_url(self) -> str:
        return f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{self.accession}.txt"


def build_pool() -> urllib3.PoolManager:
    return urllib3.PoolManager(
        num_pools=4,
        maxsize=20,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov",
        },
        timeout=urllib3.Timeout(connect=10, read=30),
    )


def http_get(pool: urllib3.PoolManager, url: str) -> tuple[int, str]:
    r = pool.request("GET", url, preload_content=True)
    body = r.data.decode("utf-8", errors="replace")
    return r.status, body


def fetch_form_idx(pool: urllib3.PoolManager, year: int, quarter: int) -> str | None:
    IDX_CACHE.mkdir(parents=True, exist_ok=True)
    cache_path = IDX_CACHE / f"form_{year}_QTR{quarter}.idx"
    miss_marker = IDX_CACHE / f"form_{year}_QTR{quarter}.404"
    if cache_path.exists():
        return cache_path.read_text(encoding="latin-1")
    if miss_marker.exists():
        return None
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/form.idx"
    status, body = http_get(pool, url)
    if status == 404:
        miss_marker.write_text("")
        return None
    if status != 200:
        raise RuntimeError(f"unexpected status {status} for {url}")
    cache_path.write_text(body, encoding="latin-1")
    return body


def extract_filing_refs(idx_text: str, form_type: str) -> list[FilingRef]:
    refs: list[FilingRef] = []
    seen: set[str] = set()
    for ln in idx_text.splitlines():
        if not re.match(rf"^{re.escape(form_type)}\s", ln):
            continue
        m = PATH_RE.search(ln)
        if not m:
            continue
        cik, accession = m.group(1), m.group(2)
        if accession in seen:
            continue
        seen.add(accession)
        refs.append(FilingRef(cik=cik, accession=accession))
    return refs


def fetch_header(pool: urllib3.PoolManager, ref: FilingRef) -> str:
    HEADER_CACHE.mkdir(parents=True, exist_ok=True)
    html_path = HEADER_CACHE / f"{ref.accession}.html"
    txt_path = HEADER_CACHE / f"{ref.accession}.txt"
    if html_path.exists():
        return html_path.read_text(encoding="utf-8", errors="replace")
    if txt_path.exists():
        return txt_path.read_text(encoding="utf-8", errors="replace")

    status, body = http_get(pool, ref.headers_url)
    if status == 404:
        status2, body2 = http_get(pool, ref.txt_url)
        if status2 != 200:
            raise RuntimeError(f"status {status2} for {ref.txt_url}")
        end = body2.find("</SEC-HEADER>")
        if end != -1:
            body2 = body2[: end + len("</SEC-HEADER>")]
        txt_path.write_text(body2, encoding="utf-8")
        return body2
    if status != 200:
        raise RuntimeError(f"status {status} for {ref.headers_url}")
    html_path.write_text(body, encoding="utf-8")
    return body


def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s)


def parse_header(text: str) -> tuple[str, list[tuple[str, str]], list[tuple[str, str]]]:
    plain = strip_html(text)
    date_match = DATE_RE.search(plain)
    if date_match:
        d = date_match.group(1)
        date_iso = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
    else:
        date_iso = ""

    headings = list(SECTION_RE.finditer(plain))
    subjects: list[tuple[str, str]] = []
    filers: list[tuple[str, str]] = []
    for i, m in enumerate(headings):
        kind = m.group(1)
        start = m.end()
        end = headings[i + 1].start() if i + 1 < len(headings) else len(plain)
        body = plain[start:end]
        name_m = NAME_RE.search(body)
        cik_m = CIK_RE.search(body)
        if not (name_m and cik_m):
            continue
        entry = (name_m.group(1).strip(), cik_m.group(1).strip())
        if kind in ("SUBJECT COMPANY", "ISSUER"):
            subjects.append(entry)
        else:
            filers.append(entry)
    return date_iso, subjects, filers


def discover_refs(pool: urllib3.PoolManager) -> list[tuple[int, FilingRef]]:
    current_year = dt.date.today().year
    out: list[tuple[int, FilingRef]] = []
    for year in range(current_year, EDGAR_FLOOR_YEAR - 1, -1):
        year_count = 0
        for q in QUARTERS:
            idx = fetch_form_idx(pool, year, q)
            if idx is None:
                continue
            refs = extract_filing_refs(idx, FORM_TYPE)
            year_count += len(refs)
            for r in refs:
                out.append((year, r))
        print(f"  {year}: {year_count} {FORM_TYPE} filings")
        if year_count == 0 and year < current_year:
            print(f"  -> no filings in {year}; treating as historical floor")
            break
    return out


def main() -> None:
    if "@" not in USER_AGENT:
        raise SystemExit("Set EDGAR_USER_AGENT to 'Your Name your@email'.")

    pool = build_pool()

    print("Discovering filings via quarterly form.idx files...")
    year_refs = discover_refs(pool)
    seen: set[str] = set()
    unique: list[tuple[int, FilingRef]] = []
    for year, ref in year_refs:
        if ref.accession in seen:
            continue
        seen.add(ref.accession)
        unique.append((year, ref))
    print(f"\nTotal unique filings: {len(unique)}\n")

    print("Fetching SGML headers...")
    rows: list[list[str]] = []
    failures: list[tuple[str, str]] = []
    for i, (idx_year, ref) in enumerate(unique, 1):
        try:
            text = fetch_header(pool, ref)
            date_iso, subjects, filers = parse_header(text)
            year = date_iso[:4] if date_iso else str(idx_year)
            rows.append(
                [
                    ref.accession,
                    year,
                    date_iso,
                    "; ".join(n for n, _ in subjects),
                    "; ".join(c for _, c in subjects),
                    "; ".join(n for n, _ in filers),
                    "; ".join(c for _, c in filers),
                ]
            )
        except Exception as e:  # noqa: BLE001
            failures.append((ref.accession, str(e)))
        if i % 100 == 0:
            print(f"  processed {i}/{len(unique)}")

    rows.sort(key=lambda r: (r[1], r[2]))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "accession",
                "year",
                "date_filed",
                "subject_company_name",
                "subject_cik",
                "filer_company_name",
                "filer_cik",
            ]
        )
        w.writerows(rows)

    print(f"\nWrote {len(rows)} rows to {OUTPUT_CSV}")
    if failures:
        print(f"{len(failures)} failures:")
        for acc, err in failures[:10]:
            print(f"  {acc}: {err}")


if __name__ == "__main__":
    main()
