"""Generate synthetic ISS and FactSet shareholder proposal datasets with
deliberate, plausible mismatches for testing the reconciliation script."""

import random
from datetime import date, timedelta

import numpy as np
import pandas as pd

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

OUT_ISS = "ISS_proposals_2016to2025.xlsx"
OUT_FS = "Factset_proposals_2016to2025.xlsx"

YEARS = list(range(2016, 2026))

# Pools drawn from real-looking values (per the handoff spec).
INDEXES = ["S&P 500", "S&P 400", "S&P 600", "Russell 3000 less S&P 1500", ""]
INDEX_WEIGHTS = [0.35, 0.18, 0.18, 0.25, 0.04]

RESOLUTIONS = [
    ("Require Independent Board Chair", "GOV"),
    ("Adopt Proxy Access Right", "GOV"),
    ("Reduce Supermajority Vote Requirement", "GOV"),
    ("Right to Call Special Meeting", "GOV"),
    ("Report on Political Contributions", "SRI"),
    ("Report on Lobbying Activities", "SRI"),
    ("Report on Climate Change Risk", "SRI"),
    ("Report on Gender Pay Gap", "SRI"),
    ("Report on Human Rights Impact", "SRI"),
    ("Adopt GHG Emissions Reduction Targets", "SRI"),
]

SPONSORS = [
    ("Chevedden, John", "Individual"),
    ("Mercy Investment Services", "religious"),
    ("New York City Pension Fund", "public pension"),
    ("Trillium Asset Management", "SRI fund"),
    ("Green Century Capital Management", "SRI fund"),
    ("Northstar Asset Management", "SRI fund"),
    ("As You Sow", "SRI fund"),
    ("CalSTRS", "public pension"),
    ("Sisters of St. Francis", "religious"),
    ("McRitchie, James", "Individual"),
]

OMIT_REASONS = [
    ("I-7", "Rule 14a-8(i)(7) - ordinary business"),
    ("I-9", "Rule 14a-8(i)(9) - conflict with company proposal"),
    ("I-10", "Rule 14a-8(i)(10) - substantially implemented"),
    ("E-2", "Rule 14a-8(e)(2) - late submission"),
    ("I-3", "Rule 14a-8(i)(3) - materially false or misleading"),
]


def make_company_pool(n, start_id=100):
    """Build a pool of companies with stable cusip/ticker/name/iss_id."""
    companies = []
    for i in range(n):
        # cusip_6: mix of all-numeric and letter-containing, leading zeros.
        if i % 11 == 0:
            # letter inside — e.g., 00130H
            cusip_6 = f"{random.randint(0, 9999):04d}{random.choice('HKQXYZ')}"
            cusip_6 = cusip_6[:6].zfill(6)
        else:
            cusip_6 = f"{random.randint(1, 999999):06d}"
        # 9-digit cusip
        cusip = cusip_6 + f"{random.randint(100, 999)}"
        # ticker
        ticker = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=random.choice([3, 4])))
        # company name
        suffix = random.choice(["Inc.", "Corporation", "Corp.", "Co.", "Incorporated", "Holdings Inc.", "Group Inc."])
        base = random.choice([
            "Aflac", "AGCO", "AES", "Apex", "Axiom", "Baxter", "Beacon", "Cadence", "Cascade",
            "Delta", "Emerald", "Fairmont", "Granite", "Harbor", "Ironwood", "Juniper",
            "Keystone", "Lakeside", "Meridian", "Northgate", "Oakwood", "Pioneer", "Quartz",
            "Redstone", "Summit", "Thornton", "Union", "Vantage", "Westbrook", "Zenith",
        ])
        company_name = f"{base} {suffix}"
        iss_id = str(start_id + i)
        entity_id = f"{random.randint(0, 999):03d}{random.choice('WXYZ')}{random.choice('YZA')}{random.choice('YZA')}-E"
        cik = f"{random.randint(10000, 1999999):07d}"
        sedol = "".join(random.choices("0123456789ABCDEFGH", k=7))
        companies.append({
            "cusip_6": cusip_6,
            "cusip": cusip,
            "ticker": ticker,
            "company_name": company_name,
            "iss_companyid": iss_id,
            "entity_id": entity_id,
            "cik": cik,
            "sedol": sedol,
            "indexname": random.choices(INDEXES, weights=INDEX_WEIGHTS, k=1)[0],
        })
    return companies


def random_meeting_date(year):
    """Most meetings April–June; some spread through the year."""
    r = random.random()
    if r < 0.7:
        # Apr–Jun
        start = date(year, 4, 1)
        end = date(year, 6, 30)
    else:
        start = date(year, 1, 15)
        end = date(year, 12, 15)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def make_iss_row(company, meeting_date, meeting_year, meeting_code, meetingid,
                 resolution, resolution_type, status, sponsor_name, sponsor_type,
                 agenda_seq):
    """Build one ISS proposal row."""
    omit_reason = ""
    omit_reason_text = ""
    level_of_support = np.nan
    support_fa = np.nan
    support_faa = np.nan
    support_out = np.nan
    requirement = np.nan
    base = ""
    passed = ""
    itemonagendaid = "0"
    issagendaitemid = f"S{random.randint(1, 9999):04d}"

    if status == "final vote":
        level_of_support = round(random.uniform(5.0, 75.0), 2)
        support_fa = round(level_of_support + random.uniform(-1.5, 1.5), 2)
        support_faa = round(level_of_support, 2)
        support_out = round(level_of_support * random.uniform(0.55, 0.85), 2)
        requirement = 0.5
        base = random.choice(["F+A+A", "F+A"])
        passed = "Y" if level_of_support > 50 else "N"
        itemonagendaid = str(random.randint(10_000_000, 15_999_999))
    elif status == "omitted":
        omit = random.choice(OMIT_REASONS)
        omit_reason = omit[0]
        omit_reason_text = omit[1]
    # withdrawn / mtg cancelled / not in proxy — leave vote fields blank

    return {
        "company_name": company["company_name"],
        "Meeting_Year": str(meeting_year),
        "Meeting_Date": meeting_date,
        "cusip_6": company["cusip_6"],
        "cusip": company["cusip"],
        "iss_companyid": company["iss_companyid"],
        "indexname": company["indexname"],
        "ticker": company["ticker"] if random.random() > 0.02 else "",
        "meeting_code": meeting_code,
        "resolution": resolution,
        "resolution_type": resolution_type,
        "other_status": status,
        "foot_note": "",
        "omit_reason": omit_reason,
        "sponsor_name": sponsor_name,
        "sponsor_type": sponsor_type,
        "requirement": requirement,
        "passed": passed,
        "levelofsupport": level_of_support,
        "meetingid": str(meetingid) if status not in ("omitted", "not in proxy") else "0",
        "issagendaitemid": issagendaitemid,
        "itemonagendaid": itemonagendaid,
        "base": base,
        "support_for_against": support_fa,
        "support_for_against_abstain": support_faa,
        "support_outstanding": support_out,
        "foot_note_text": "",
        "omit_reason_text": omit_reason_text,
    }


def make_fs_row(company, meeting_date, meeting_year):
    return {
        "CompanyName": company["company_name"],
        "Meeting_Year": str(meeting_year),
        "Meeting_Date": meeting_date,
        "cusip_6": company["cusip_6"],
        "CIK_from_url": company["cik"],
        "Symbol": company["ticker"],
        "EntityID": company["entity_id"],
        "Sedol": company["sedol"],
        "Cusip": company["cusip"],
        "Ticker": f"{company['ticker']}-US",
    }


def build():
    # Four non-overlapping company pools, each keyed by cusip_6.
    # (A) Shared universe — appears in both ISS and FactSet.
    # (B) ISS-only Russell 3000: in ISS, not in FactSet (index coverage gap).
    # (C) FactSet-only non-ISS-index companies.
    # (D) Shared-universe companies where ISS carries special meetings
    #     that FactSet doesn't cover.
    shared = make_company_pool(180, start_id=1000)
    iss_only_russell = make_company_pool(25, start_id=5000)
    # Force their index to Russell 3000 less S&P 1500.
    for c in iss_only_russell:
        c["indexname"] = "Russell 3000 less S&P 1500"

    fs_only_pool = make_company_pool(20, start_id=7000)
    # These companies are outside ISS's coverage universe.

    iss_rows = []
    fs_rows = []

    # ---- (A) Shared universe: annual meetings in both sources. ----
    meetingid_counter = 1_000_000
    for company in shared:
        for year in YEARS:
            # Most companies have an annual meeting each year (90% chance).
            if random.random() > 0.9:
                continue
            mdate = random_meeting_date(year)
            meetingid_counter += 1
            # 1–5 proposals per meeting, with realistic status distribution.
            n_props = random.choices([1, 2, 3, 4, 5], weights=[0.35, 0.3, 0.2, 0.1, 0.05])[0]
            for seq in range(n_props):
                res_name, res_type = random.choice(RESOLUTIONS)
                sponsor_name, sponsor_type = random.choice(SPONSORS)
                # Matched meetings skew heavily toward "final vote".
                status = random.choices(
                    ["final vote", "withdrawn", "omitted", "not in proxy"],
                    weights=[0.80, 0.10, 0.07, 0.03],
                )[0]
                iss_rows.append(make_iss_row(
                    company, mdate, year, "Annual", meetingid_counter,
                    res_name, res_type, status, sponsor_name, sponsor_type, seq,
                ))
            # FactSet row for the meeting (one row per meeting in FS).
            fs_rows.append(make_fs_row(company, mdate, year))

    # ---- (B) ISS-only Russell 3000 meetings (index-coverage gap). ----
    for company in iss_only_russell:
        for year in YEARS:
            if random.random() > 0.6:
                continue
            mdate = random_meeting_date(year)
            meetingid_counter += 1
            n_props = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
            for seq in range(n_props):
                res_name, res_type = random.choice(RESOLUTIONS)
                sponsor_name, sponsor_type = random.choice(SPONSORS)
                status = random.choices(
                    ["final vote", "withdrawn", "omitted"],
                    weights=[0.80, 0.12, 0.08],
                )[0]
                iss_rows.append(make_iss_row(
                    company, mdate, year, "Annual", meetingid_counter,
                    res_name, res_type, status, sponsor_name, sponsor_type, seq,
                ))
            # No FactSet row for these.

    # ---- (C) FactSet-only companies (outside ISS's universe). ----
    for company in fs_only_pool:
        for year in YEARS:
            if random.random() > 0.5:
                continue
            mdate = random_meeting_date(year)
            fs_rows.append(make_fs_row(company, mdate, year))

    # ---- (D) Special meetings present only in ISS, on shared companies. ----
    # A chunk of the shared pool has additional special/court meetings.
    special_sample = random.sample(shared, 30)
    for company in special_sample:
        # 1–2 extra special meetings somewhere in the window.
        for _ in range(random.choice([1, 2])):
            year = random.choice(YEARS)
            mdate = random_meeting_date(year)
            meetingid_counter += 1
            # Special meetings tend to have fewer proposals.
            n_props = random.choices([1, 2], weights=[0.7, 0.3])[0]
            code = random.choices(["Special", "Court"], weights=[0.9, 0.1])[0]
            for seq in range(n_props):
                res_name, res_type = random.choice(RESOLUTIONS)
                sponsor_name, sponsor_type = random.choice(SPONSORS)
                status = random.choices(
                    ["final vote", "mtg cancelled"],
                    weights=[0.85, 0.15],
                )[0]
                iss_rows.append(make_iss_row(
                    company, mdate, year, code, meetingid_counter,
                    res_name, res_type, status, sponsor_name, sponsor_type, seq,
                ))
            # No FactSet row (FactSet is annual-meetings-only in this dataset).

    # ---- (E) Deliberately introduce an extra cluster of withdrawn/omitted
    # proposals on meetings that FactSet doesn't carry (ISS appears to include
    # proposals whose underlying meeting row never landed in FactSet — e.g.,
    # no-action-letter-only filings). ----
    withdrawn_pool = make_company_pool(15, start_id=9000)
    for company in withdrawn_pool:
        for year in YEARS[-4:]:  # concentrated in recent years
            if random.random() > 0.5:
                continue
            mdate = random_meeting_date(year)
            meetingid_counter += 1
            n_props = random.choices([1, 2], weights=[0.7, 0.3])[0]
            for seq in range(n_props):
                res_name, res_type = random.choice(RESOLUTIONS)
                sponsor_name, sponsor_type = random.choice(SPONSORS)
                status = random.choices(
                    ["withdrawn", "omitted", "not in proxy"],
                    weights=[0.45, 0.40, 0.15],
                )[0]
                iss_rows.append(make_iss_row(
                    company, mdate, year, "Annual", meetingid_counter,
                    res_name, res_type, status, sponsor_name, sponsor_type, seq,
                ))

    iss_df = pd.DataFrame(iss_rows)
    fs_df = pd.DataFrame(fs_rows)

    # --- Introduce realistic date-format noise in FactSet (stored as strings). ---
    def fs_date_str(d):
        fmt = random.choice([
            d.strftime("%m/%d/%Y"),
            d.strftime("%-m/%-d/%Y"),
            d.strftime("%Y-%m-%d"),
        ])
        return fmt
    fs_df["Meeting_Date"] = fs_df["Meeting_Date"].apply(fs_date_str)

    # ISS keeps real datetimes (Excel will store them as such).
    iss_df["Meeting_Date"] = pd.to_datetime(iss_df["Meeting_Date"])

    # Simulate Excel numeric-cast corruption on cusip_6 for the ISS file only:
    # for *all-digit* cusips, drop leading zeros in the source so the reconciler
    # has to repair them.  Letter-containing cusips stay intact.
    def corrupt_cusip6(v):
        if v.isdigit():
            stripped = v.lstrip("0") or "0"
            # Sometimes Excel even appends a trailing ".0"
            if random.random() < 0.3:
                return stripped + ".0"
            return stripped
        return v
    iss_df["cusip_6"] = iss_df["cusip_6"].apply(corrupt_cusip6)

    # Same treatment on FactSet but a little less aggressive, for realism.
    def corrupt_cusip6_fs(v):
        if v.isdigit() and random.random() < 0.7:
            return v.lstrip("0") or "0"
        return v
    fs_df["cusip_6"] = fs_df["cusip_6"].apply(corrupt_cusip6_fs)

    # Shuffle rows so the file order isn't suspiciously grouped.
    iss_df = iss_df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    fs_df = fs_df.sample(frac=1, random_state=SEED).reset_index(drop=True)

    iss_df.to_excel(OUT_ISS, index=False, engine="openpyxl")
    fs_df.to_excel(OUT_FS, index=False, engine="openpyxl")

    print(f"Wrote {OUT_ISS}: {len(iss_df):,} rows")
    print(f"Wrote {OUT_FS}: {len(fs_df):,} rows")


if __name__ == "__main__":
    build()
