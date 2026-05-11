# How we got the PX14A6G dataset

A plain-English walkthrough of how the historical PX14A6G filing dataset was assembled, with enough technical detail that another analyst can reproduce or audit the work.

## What's in the dataset

One row per PX14A6G filing — these are the "Notice of Exempt Solicitation" filings made under Exchange Act Rule 14a-6(g) by people who own (or have proxies for) more than $5M of an issuer's stock and want to share their views with other shareholders without going through the full proxy-statement machinery. Each row has:

- `accession` — SEC's unique filing ID
- `year` and `date_filed` — when the filing was accepted by EDGAR
- `subject_company_name` and `subject_cik` — the public company being talked about
- `filer_company_name` and `filer_cik` — the activist, fund, or organization doing the talking

Coverage runs from 1997 (the earliest year EDGAR has any PX14A6G filings — 1996 returns zero, which is the historical floor) through the current year.

## Where the data comes from

Everything comes from SEC EDGAR. Two specific endpoints do the work.

### 1. Quarterly form indexes

For every year, SEC publishes a fixed-width text file at:

```
https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{1..4}/form.idx
```

This file lists every filing accepted that quarter, sorted by form type. We download four of these per year. For each line that starts with `PX14A6G`, we pull out the path, which always has the shape `edgar/data/{CIK}/{accession}.txt` — that's enough to identify the filing.

We anchor the regex to the start of the line (`^PX14A6G\s`) so that amendments (`PX14A6G/A`) and other neighboring form types don't sneak in.

### 2. The per-filing SGML header

The form index tells us *which* filings exist but not who the subject and filer are. For that we fetch the filing's metadata header. Modern filings (roughly 2014 onward) live in a per-filing subfolder and expose the header at:

```
https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_no_dashes}/{accession}-index-headers.html
```

Older filings don't have that subfolder layout, so the URL 404s. As a fallback we fetch the full filing at:

```
https://www.sec.gov/Archives/edgar/data/{CIK}/{accession}.txt
```

and slice off everything before `</SEC-HEADER>`. Either route gives us the same SGML block, which looks like this:

```
SUBJECT COMPANY:
    COMPANY DATA:
        COMPANY CONFORMED NAME:    HORMEL FOODS CORP /DE/
        CENTRAL INDEX KEY:         0000048465
FILED BY:
    COMPANY DATA:
        COMPANY CONFORMED NAME:    ACCOUNTABILITY BOARD, INC.
        CENTRAL INDEX KEY:         0001947330
```

Three regexes pull out the section headings (`SUBJECT COMPANY`, `FILED BY`, etc.), the conformed company name, and the CIK. A fourth regex pulls `FILED AS OF DATE`, which is the canonical filing date.

## Things that tripped us up

A few things are worth knowing if you want to extend this.

**Each filing appears twice in form.idx** — once under the subject's CIK, once under the filer's. The accession number is the same. We dedupe by accession before fetching headers, which cuts the request count in half. (An early version of the script counted 840 filings for 2024; the correct unique count is 420.)

**Multiple subjects or co-filers.** A single PX14A6G can target several issuers or be filed jointly by several activists. We collect all of them and join names/CIKs with `; ` in the CSV row, so you don't lose any party.

**Year overflow.** Going back to 1997 means ~3,400 filings. The discovery phase is cheap (~120 idx requests, all small text files) but the per-filing header phase is the long pole. We cache every fetch under `.cache/edgar/`, so the second run costs nothing.

**The historical floor.** PX14A6G as a form designation first appears in EDGAR in 1997 (form code came online with SEC's 1996 proxy rule revisions). We walk backward year by year and stop the first time a year returns zero filings.

## What the CSV looks like

Sorted by year then date. Each row is one accession:

```
accession,year,date_filed,subject_company_name,subject_cik,filer_company_name,filer_cik
0000892918-97-000002,1997,1997-01-15,CONRAIL INC,0000897732,WYSER PRATTE GUY P,0000939173
...
0001214659-24-020658,2024,2024-12-18,HORMEL FOODS CORP /DE/,0000048465,"ACCOUNTABILITY BOARD, INC.",0001947330
```

That's the whole pipeline. Two SEC endpoints, three regexes, one CSV.
