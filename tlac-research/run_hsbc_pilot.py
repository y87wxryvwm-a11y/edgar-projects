"""Reproduce the 30 June 2025 HSBC instrument and US registered-fund pilot.

Requires Python 3.10+ and the pdftotext executable from Poppler. Reads SEC bulk
archives and HSBC's dated disclosure; writes JSON evidence, never a global estimate.
"""
# ---- EDIT THIS --------------------------------------------------------------
download_missing = True
# -----------------------------------------------------------------------------
try:
    from config import DATA_DIR, USER_AGENT
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set DATA_DIR and USER_AGENT."
    )

from pathlib import Path
import csv, io, json, zipfile, collections, datetime, re
import urllib.request, subprocess, hashlib, time

directory = DATA_DIR.replace("\\", "/")
P = Path(directory)
P.mkdir(parents=True, exist_ok=True)
SOURCE_PDF = "https://www.hsbc.com/-/files/hsbc/investors/hsbc-results/2025/interim/pdfs/hsbc-holdings-plc/250806-capital-and-other-tlac-eligible-instruments-main-features-30-june-2025.pdf"
source_files = {"hsbc.pdf": SOURCE_PDF}
source_files.update({f"2025q{q}_nport.zip": f"https://www.sec.gov/files/dera/data/form-n-port-data-sets/2025q{q}_nport.zip" for q in [2,3,4]})
for filename, url in source_files.items():
    target = P/filename
    if target.exists():
        continue
    if not download_missing:
        raise FileNotFoundError(f"Download {url} to {target}, or set download_missing=True.")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    temporary = target.with_suffix(target.suffix + ".part")
    with urllib.request.urlopen(request, timeout=60) as response, temporary.open("wb") as stream:
        while chunk := response.read(1024*1024):
            stream.write(chunk)
    temporary.replace(target)
    time.sleep(0.2)

subprocess.run(["pdftotext", "-layout", str(P/"hsbc.pdf"), str(P/"hsbc.txt")], check=True)
inventory_rows = []
for page_number, page in enumerate((P/"hsbc.txt").read_text().split("\f"), 1):
    if not 4 <= page_number <= 43 or page_number == 15:
        continue
    fields = {}
    for line in page.splitlines():
        found = re.match(r"^(1|2|8|9|11)\s+", line)
        if found and int(found[1]) not in fields:
            fields[int(found[1])] = re.split(r"\s{2,}", line)[2:]
    assert set(fields) == {1,2,8,9,11}, (page_number, fields)
    assert len({len(value) for value in fields.values()}) == 1, page_number
    for column, isin in enumerate(fields[2]):
        nominal = fields[9][column]
        usd = re.search(r"USD ([0-9,]+)m", nominal)
        assert usd, (page_number, nominal)
        inventory_rows.append({
            "page": page_number,
            "category": "AT1" if page_number <= 7 else "Tier 2" if page_number <= 14 else "Other eligible liabilities",
            "issuer": fields[1][column], "isin": isin,
            "issue_date": fields[11][column], "nominal": nominal,
            "nominal_usd_m": float(usd[1].replace(",", "")),
            "recognised_usd_m": float(re.search(r"USD ([0-9,]+)m", fields[8][column])[1].replace(",", "")),
        })
assert len(inventory_rows) == len({r["isin"] for r in inventory_rows}) == 129
assert all(r["issuer"] == "HSBC Holdings plc" for r in inventory_rows)
for row in inventory_rows:
    digits = "".join(str(int(c,36)) for c in row["isin"])
    checksum = sum(int(c) if i%2 == 0 else sum(map(int,str(int(c)*2))) for i,c in enumerate(digits[::-1]))
    assert checksum%10 == 0, row["isin"]
(P/"hsbc-extract.json").write_text(json.dumps(inventory_rows, indent=2))
TARGET = '30-JUN-2025'
inventory = json.loads((P/'hsbc-extract.json').read_text())
by_isin = {r['isin']: r for r in inventory}
by_cusip = {s[2:-1]: s for s in by_isin if s.startswith('US')}
for r in inventory:
    m = re.match(r'([A-Z]{3}) ([\d,]+)m', r['nominal'])
    r['currency'] = m[1]
    r['nominal_native'] = float(m[2].replace(',', ''))*1e6
    r['nominal_usd'] = r['nominal_usd_m']*1e6

def read(z, n):
    return csv.DictReader(io.TextIOWrapper(z.open(n+'.tsv')), delimiter='\t')

metadata = {}
quarters = [2,3,4]
for q in quarters:
    z = zipfile.ZipFile(P/f'2025q{q}_nport.zip')
    sub = {r['ACCESSION_NUMBER']: r for r in read(z,'SUBMISSION')}
    reg = {r['ACCESSION_NUMBER']: r for r in read(z,'REGISTRANT')}
    fund = {r['ACCESSION_NUMBER']: r for r in read(z,'FUND_REPORTED_INFO')}
    for acc in sub:
        metadata[acc] = dict(sub[acc], **{k:v for k,v in reg[acc].items() if k!='ACCESSION_NUMBER'},
                             **{k:v for k,v in fund[acc].items() if k!='ACCESSION_NUMBER'}, quarter=q)
    z.close()

def fundkey(m):
    return (m['CIK'], m['SERIES_ID'] or m['SERIES_LEI'] or m['SERIES_NAME'])
latest = {}
for acc,m in metadata.items():
    if m['REPORT_DATE'] != TARGET or m['COUNTRY']!='US': continue
    key=fundkey(m)
    order=(datetime.datetime.strptime(m['FILING_DATE'],'%d-%b-%Y'),acc)
    if key not in latest or order>latest[key][0]:latest[key]=(order,acc)
chosen={v[1] for v in latest.values()}
matched=[]; candidate=[]; match_conflicts=[]; scan_stats=[]
for q in quarters:
    z=zipfile.ZipFile(P/f'2025q{q}_nport.zip')
    isin_matches=collections.defaultdict(set)
    for r in read(z,'IDENTIFIERS'):
        if r['IDENTIFIER_ISIN'] in by_isin:isin_matches[r['HOLDING_ID']].add(r['IDENTIFIER_ISIN'])
    count=0;eligible_count=0
    for r in read(z,'FUND_REPORTED_HOLDING'):
        count+=1
        if r['ACCESSION_NUMBER'] not in chosen:continue
        eligible_count+=1
        matches=set(isin_matches.get(r['HOLDING_ID'],set()))
        if r['ISSUER_CUSIP'] in by_cusip:matches.add(by_cusip[r['ISSUER_CUSIP']])
        if len(matches)>1:match_conflicts.append(r);continue
        if matches:
            r['isin']=next(iter(matches));r['quarter']=q;matched.append(r)
        elif 'HSBC' in r['ISSUER_NAME'].upper() or r['ISSUER_LEI']=='MLU0ZO3ML4LN2LL2TL39':candidate.append(r)
    print('scanned',q,count,'chosen holdings',eligible_count,'cumulative matches',len(matched),flush=True)
    scan_stats.append({'quarter':q,'all_holdings_scanned':count,'chosen_fund_holdings':eligible_count})
    z.close()

assert not match_conflicts
assert len({(r['ACCESSION_NUMBER'],r['HOLDING_ID']) for r in matched})==len(matched)
excluded=[];included=[]
for r in matched:
    m=metadata[r['ACCESSION_NUMBER']]; ins=by_isin[r['isin']]
    r.update({k:m[k] for k in ['CIK','REGISTRANT_NAME','SERIES_NAME','SERIES_ID','SERIES_LEI','COUNTRY','REPORT_DATE','FILING_DATE','NET_ASSETS']})
    r['category']=ins['category']
    r['filing_url']=f"https://www.sec.gov/Archives/edgar/data/{int(m['CIK'])}/{r['ACCESSION_NUMBER'].replace('-','')}/{r['ACCESSION_NUMBER']}-index.htm"
    reasons=[]
    if r['UNIT']!='PA': reasons.append('balance not reported as principal')
    if r['CURRENCY_CODE']!=ins['currency']:reasons.append('currency differs from issuer principal currency')
    if r['PAYOFF_PROFILE']!='Long':reasons.append('not long')
    if r['ASSET_CAT'] not in ['DBT','EP']:reasons.append('not debt or preferred equity')
    if float(r['BALANCE'])<0:reasons.append('negative balance')
    if reasons:r['exclusion']='; '.join(reasons);excluded.append(r)
    else:
        r['principal_usd_at_issuer_fx']=float(r['BALANCE'])*ins['nominal_usd']/ins['nominal_native']
        included.append(r)

instrument_results=[]
for ins in inventory:
    a=[r for r in included if r['isin']==ins['isin']]
    x=dict(ins)
    x['reported_principal_native']=sum(float(r['BALANCE']) for r in a)
    x['reported_principal_usd']=sum(r['principal_usd_at_issuer_fx'] for r in a)
    x['reported_market_value_usd']=sum(float(r['CURRENCY_VALUE']) for r in a)
    x['ownership_pct']=100*x['reported_principal_native']/ins['nominal_native']
    x['funds']=len({fundkey(metadata[r['ACCESSION_NUMBER']]) for r in a})
    instrument_results.append(x)
assert max(r['ownership_pct'] for r in instrument_results)<=100

stats={
 'report_date':'2025-06-30','filing_archives':['2025Q2','2025Q3','2025Q4'],
 'all_filing_count':len(metadata),'exact_date_US_filings_before_dedup':sum(m['REPORT_DATE']==TARGET and m['COUNTRY']=='US' for m in metadata.values()),
 'exact_date_US_funds':len(chosen), 'matched_rows':len(matched),'matched_instruments':len({r['isin'] for r in matched}),
 'matched_funds':len({fundkey(metadata[r['ACCESSION_NUMBER']]) for r in matched}),
 'matched_market_value_usd':sum(float(r['CURRENCY_VALUE']) for r in matched),
 'included_rows':len(included),'included_funds':len({fundkey(metadata[r['ACCESSION_NUMBER']]) for r in included}),
 'included_principal_usd':sum(r['principal_usd_at_issuer_fx'] for r in included),
 'included_market_value_usd':sum(float(r['CURRENCY_VALUE']) for r in included),
 'excluded_rows':len(excluded),'excluded_market_value_usd':sum(float(r['CURRENCY_VALUE']) for r in excluded),
 'denominator_nominal_usd':sum(r['nominal_usd'] for r in inventory),
 'scan_stats':scan_stats,
 'all_US_funds_in_archives':len({fundkey(m) for m in metadata.values() if m['COUNTRY']=='US'}),
}
stats['ownership_pct']=100*stats['included_principal_usd']/stats['denominator_nominal_usd']
stats['by_category']=[]
for cat in ['AT1','Tier 2','Other eligible liabilities']:
    a=[r for r in instrument_results if r['category']==cat]
    stats['by_category'].append({'category':cat,'instruments':len(a),'denominator_nominal_usd':sum(r['nominal_usd'] for r in a),
     'reported_principal_usd':sum(r['reported_principal_usd'] for r in a),'reported_market_value_usd':sum(r['reported_market_value_usd'] for r in a),
     'ownership_pct':100*sum(r['reported_principal_usd'] for r in a)/sum(r['nominal_usd'] for r in a)})
for name,obj in [('pilot-summary',stats),('pilot-instruments',instrument_results),('pilot-holdings',included),('pilot-excluded',excluded),('pilot-unmatched-hsbc',candidate),('pilot-selected-filings',[metadata[a] for a in sorted(chosen)])]:
    (P/(name+'.json')).write_text(json.dumps(obj,indent=2))
print(json.dumps(stats,indent=2),flush=True)
print('TOP INSTRUMENTS',[(r['isin'],r['ownership_pct'],r['reported_principal_usd']) for r in sorted(instrument_results,key=lambda r:r['reported_principal_usd'],reverse=True)[:8]])

# Keep loaned holdings as gross fund exposure, but reject securities merely
# received as non-cash collateral; that would not establish outright ownership.
wanted_ids = {r["HOLDING_ID"] for r in included}
lending = []
for q in quarters:
    with zipfile.ZipFile(P/f"2025q{q}_nport.zip") as z:
        lending.extend(r for r in read(z,"SECURITIES_LENDING") if r["HOLDING_ID"] in wanted_ids)
assert len(lending) == len(included)
assert not any(r["IS_NON_CASH_COLLATERAL"] == "Y" for r in lending)
(P/"pilot-lending.json").write_text(json.dumps(lending, indent=2))
manifest = []
for filename,url in source_files.items():
    digest = hashlib.sha256()
    with (P/filename).open("rb") as stream:
        while chunk := stream.read(1024*1024): digest.update(chunk)
    manifest.append({"file": filename, "url": url, "sha256": digest.hexdigest(), "bytes": (P/filename).stat().st_size})
(P/"pilot-source-manifest.json").write_text(json.dumps(manifest, indent=2))
print("Verified source hashes and securities-lending flags. Results:", P/"pilot-summary.json")
