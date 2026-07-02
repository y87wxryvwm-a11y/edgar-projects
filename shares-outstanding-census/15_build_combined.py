"""
15_build_combined.py — the combined registrant-level dataset.

Deterministic assembly of the completed shares and float censuses into one
published, Stata-safe relational pair joined by (accession, cik):

  registrants_{year}.csv    one row per (accession, cik) — every entity with
                            its own CIK inside an annual filing, INCLUDING ABS
                            issuers and no-measure co-filers, each kept with an
                            explicit status rather than dropped. Carries the
                            identity, a shares summary, and a float summary.
  share_classes_{year}.csv  one row per (accession, cik, class) — the
                            shares_outstanding rows plus a per-class public
                            float column where the cover breaks the value out.
  registrants_{year}.jsonl  the same, classes nested per registrant, for
                            Python users (relational tables stay the source).

Pure function of the committed censuses + population + cached SGML headers +
combined_overrides.py. No network, no sub-agents: any machine reproduces it.
Primary key of registrants_{year}.csv is (accession, cik); an accession recurs
across its co-filer rows.

Design decisions (Evan, 2026-07-02):
  A. shares_total is left empty across MIXED share types (shares_status
     DISCLOSED_MIXED_TYPES) — summing common + preferred is misleading; the
     per-class numbers stay in share_classes. Fractional counts are kept
     (shares_status DISCLOSED_FRACTIONAL).
  B. The 170 filings whose cover states no positive non-affiliate float (None /
     $0 / N-A / no-public-market / indeterminable, XBRL corroborating) get
     float_status FLOAT_STATED_NONE + float_status_detail (the reason). NOT the
     same as NOT_DISCLOSED (silence). Classified from the cover field,
     adversarially read-audited (48 covers, 0 hidden positive floats). Map:
     combined_overrides.py.
  C. Per-class public float joins onto share classes by class_designator (A/B),
     attached only when the designator maps to exactly one float class.

float_status is ALWAYS populated (DISCLOSED when public_float carries a value),
so the column has no blank category.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
# -----------------------------------------------------------------------------

import os
import re
import json
import pandas as pd

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set DATA_DIR."
    )

from combined_overrides import FLOAT_STATED_NONE  # {accession: sub_reason}

directory = DATA_DIR.replace("\\", "/")
D = lambda *p: os.path.join(directory, *p)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _num(x):
    """Numeric share count (int or fraction), or None if not a number."""
    s = str(x).strip().replace(",", "")
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _fmt(total, fractional):
    """Format a shares total: plain integer, or a trimmed decimal if fractional."""
    if not fractional:
        return str(int(round(total)))
    return ("%f" % total).rstrip("0").rstrip(".")


_HDR_RE = re.compile(
    r"COMPANY CONFORMED NAME:\s*(.+?)\s*\n(?:.*?\n)*?\s*CENTRAL INDEX KEY:\s*(\d+)"
)


def header_names(accession):
    """{cik(str, unpadded): conformed name} for every FILER in the cached header."""
    p = D("cache", "headers", f"{accession}.hdr.txt")
    if not os.path.exists(p):
        return {}
    with open(p, encoding="latin-1") as fh:
        txt = fh.read()
    out = {}
    for m in _HDR_RE.finditer(txt):
        out.setdefault(str(int(m.group(2))), m.group(1).strip())
    return out


def desig_from_member(member):
    """Class letter from a public_float class_or_series member string, or ''."""
    m = re.search(r"\bclass\s+([A-Z0-9]+)\b", member, re.I)
    return m.group(1).upper() if m else ""


# ---------------------------------------------------------------------------
# load the source datasets
# ---------------------------------------------------------------------------
pop = pd.read_csv(D(f"population_{year}.csv"), dtype=str).fillna("")
shares = pd.read_csv(D(f"shares_outstanding_{year}.csv"), dtype=str).fillna("")
floats = pd.read_csv(D(f"public_float_{year}.csv"), dtype=str).fillna("")
fcov = pd.read_csv(D(f"filing_coverage_{year}.csv"), dtype=str).fillna("")
flcov = pd.read_csv(D(f"float_coverage_{year}.csv"), dtype=str).fillna("")

shares_disp = dict(zip(fcov["accession"], fcov["disposition"]))
float_disp = dict(zip(flcov["accession"], flcov["disposition"]))

shares_by = {k: g for k, g in shares.groupby(["accession", "cik"])}
floats_by = {k: g for k, g in floats.groupby(["accession", "cik"])}

row_name = {}
for _, r in shares.iterrows():
    row_name[(r["accession"], r["cik"])] = r["registrant"]
for _, r in floats.iterrows():
    row_name.setdefault((r["accession"], r["cik"]), r["registrant"])


# ---------------------------------------------------------------------------
# 1. base registrant index — expand population on all_ciks / all_sics
# ---------------------------------------------------------------------------
base = []
for _, p in pop.iterrows():
    acc = p["accession"]
    ciks = (p["all_ciks"] or p["cik"]).split(";")
    sics = (p["all_sics"] or p["sic"]).split(";")
    hdr = None
    for i, c in enumerate(ciks):
        c = c.strip()
        if not c:
            continue
        name = row_name.get((acc, c))
        if not name:
            name = p["company_name"] if c == p["cik"] else ""
        if not name:
            if hdr is None:
                hdr = header_names(acc)
            name = hdr.get(str(int(c)) if c.isdigit() else c, "")
        base.append({
            "accession": acc, "cik": c, "registrant": name,
            "form": p["form"], "date_filed": p["date_filed"],
            "sic": sics[i].strip() if i < len(sics) else "",
            "is_primary_filer": (c == p["cik"]),
            "status": "EXCLUDED_ABS" if p["excluded_abs"] == "True" else "IN_SCOPE",
            "filing_index_url": p["filing_index_url"],
        })


# ---------------------------------------------------------------------------
# 2. shares summary per registrant
# ---------------------------------------------------------------------------
def shares_summary(acc, cik, is_abs, is_primary):
    g = shares_by.get((acc, cik))
    if g is None:
        if is_abs:
            return 0, "", "EXCLUDED_ABS", "", ""
        disp = shares_disp.get(acc, "")
        if disp.startswith("ROWS_") and not is_primary:
            return 0, "", "CO_FILER_NO_ROWS", "", ""
        return 0, "", "NO_COUNT_DISCLOSED", "", ""
    n = len(g)
    dates = [d for d in g["as_of_date"] if d]
    dmin, dmax = (min(dates), max(dates)) if dates else ("", "")
    types = {t for t in g["share_type"] if t}
    vals = [_num(v) for v in g["shares_outstanding"]]
    if any(v is None for v in vals):
        return n, "", "DISCLOSED_PARTIAL", dmin, dmax   # a member has no count
    if len(types) > 1:
        return n, "", "DISCLOSED_MIXED_TYPES", dmin, dmax  # decision A
    total = sum(vals)
    fractional = any(v != int(v) for v in vals)
    status = "DISCLOSED_FRACTIONAL" if fractional else "DISCLOSED"
    return n, _fmt(total, fractional), status, dmin, dmax


# ---------------------------------------------------------------------------
# 3. float summary per registrant  (float_status ALWAYS populated)
# ---------------------------------------------------------------------------
def float_summary(acc, cik, is_abs, is_primary, form):
    g = floats_by.get((acc, cik))
    if g is not None:
        vals = [_num(v) for v in g["public_float"]]
        bases = list(g["float_basis"])
        dates = [d for d in g["public_float_date"] if d]
        if len(g) == 1:
            return (g.iloc[0]["public_float"], bases[0], "DISCLOSED", "",
                    g.iloc[0]["public_float_date"])
        # multiple float rows: the cover broke the value out by class -> the
        # registrant total is their sum (aggregate non-affiliate market value)
        if all(v is not None for v in vals):
            basis = bases[0] if len(set(bases)) == 1 else "STATED_VALUE"
            return (_fmt(sum(vals), False), basis, "DISCLOSED", "",
                    dates[0] if dates else "")
        return "", "", "DISCLOSED", "AGGREGATION_INCOMPLETE", (dates[0] if dates else "")

    if is_abs:
        return "", "", "EXCLUDED_ABS", "", ""
    if acc in FLOAT_STATED_NONE and is_primary:
        return "", "", "FLOAT_STATED_NONE", FLOAT_STATED_NONE[acc], ""
    disp = float_disp.get(acc, "")
    if disp.startswith("ROWS_") and not is_primary:
        return "", "", "CO_FILER_NO_FLOAT", "", ""
    if "STATED_ON_COVER" in disp and is_primary:
        return "", "", "FLOAT_STATED_NONE", "UNCLASSIFIED", ""
    if "NA_STATED" in disp:
        return "", "", "FLOAT_STATED_NONE", "NOT_APPLICABLE", ""
    if "NO_PUBLIC_MARKET" in disp:
        return "", "", "FLOAT_STATED_NONE", "NO_PUBLIC_MARKET", ""
    if "TAGGED_ZERO_COVER_SILENT" in disp:
        return "", "", "TAGGED_ZERO_COVER_SILENT", "", ""
    if form in ("20-F", "40-F"):
        return "", "", "NOT_REQUIRED_FORM", "", ""
    return "", "", "NOT_DISCLOSED", "", ""


# ---------------------------------------------------------------------------
# assemble registrants_{year}.csv
# ---------------------------------------------------------------------------
reg_rows = []
for b in base:
    acc, cik = b["accession"], b["cik"]
    is_abs = b["status"] == "EXCLUDED_ABS"
    n_cls, sh_total, sh_status, sh_dmin, sh_dmax = shares_summary(
        acc, cik, is_abs, b["is_primary_filer"])
    pf, basis, fstatus, fdetail, pfdate = float_summary(
        acc, cik, is_abs, b["is_primary_filer"], b["form"])
    reg_rows.append({
        "accession": acc, "cik": cik, "registrant": b["registrant"],
        "form": b["form"], "fiscal_year": year, "date_filed": b["date_filed"],
        "sic": b["sic"], "is_primary_filer": b["is_primary_filer"],
        "status": b["status"], "n_share_classes": n_cls,
        "shares_total": sh_total, "shares_status": sh_status,
        "shares_min_date": sh_dmin, "shares_max_date": sh_dmax,
        "public_float": pf, "float_basis": basis, "float_status": fstatus,
        "float_status_detail": fdetail, "public_float_date": pfdate,
        "filing_index_url": b["filing_index_url"],
    })

REG_COLS = ["accession", "cik", "registrant", "form", "fiscal_year",
            "date_filed", "sic", "is_primary_filer", "status",
            "n_share_classes", "shares_total", "shares_status",
            "shares_min_date", "shares_max_date", "public_float",
            "float_basis", "float_status", "float_status_detail",
            "public_float_date", "filing_index_url"]
registrants = pd.DataFrame(reg_rows, columns=REG_COLS).sort_values(
    ["accession", "cik"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# assemble share_classes_{year}.csv — shares rows + per-class float
# ---------------------------------------------------------------------------
pc_float = {}
for (acc, cik), g in floats_by.items():
    if len(g) < 2:
        continue
    by_desig = {}
    for _, r in g.iterrows():
        d = desig_from_member(r["class_or_series"])
        if d:
            by_desig.setdefault(d, []).append((r["public_float"], r["float_basis"]))
    # attach only when a designator maps to exactly one float class; a collision
    # (e.g. "Class M" vs "Class M-I" both reducing to M) is ambiguous -> empty
    for d, vals in by_desig.items():
        if len(vals) == 1:
            pc_float[(acc, cik, d)] = vals[0]

sc = shares.copy()
sc["fiscal_year"] = year
pcf_val, pcf_basis = [], []
for _, r in sc.iterrows():
    v, bsis = pc_float.get((r["accession"], r["cik"], (r["class_designator"] or "").upper()),
                           ("", ""))
    pcf_val.append(v)
    pcf_basis.append(bsis)
sc["per_class_public_float"] = pcf_val
sc["per_class_float_basis"] = pcf_basis
share_classes = sc.sort_values(
    ["accession", "cik", "share_class_label"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# validation assertions
# ---------------------------------------------------------------------------
n_pairs = len({(b["accession"], b["cik"]) for b in base})
assert len(registrants) == n_pairs, (len(registrants), n_pairs)
assert registrants.duplicated(["accession", "cik"]).sum() == 0, "duplicate primary key"
base_keys = {(b["accession"], b["cik"]) for b in base}
assert not ({(r["accession"], r["cik"]) for _, r in shares.iterrows()} - base_keys)
assert not ({(r["accession"], r["cik"]) for _, r in floats.iterrows()} - base_keys)
assert (registrants["registrant"] == "").sum() == 0, "registrants without a name"
assert (registrants["float_status"] == "").sum() == 0, "blank float_status"
# public_float is non-empty exactly for DISCLOSED rows (bar the rare
# AGGREGATION_INCOMPLETE case where per-class floats could not be summed)
has_val = registrants["public_float"] != ""
should_val = ((registrants["float_status"] == "DISCLOSED") &
              (registrants["float_status_detail"] != "AGGREGATION_INCOMPLETE"))
assert (has_val == should_val).all(), \
    f"{int((has_val != should_val).sum())} rows: float value/status mismatch"
# shares_total present iff a DISCLOSED* status
sd = registrants["shares_status"].isin(["DISCLOSED", "DISCLOSED_FRACTIONAL"])
assert (registrants.loc[~sd, "shares_total"] == "").all(), "shares_total set on non-DISCLOSED"
assert len(share_classes) == len(shares)


# ---------------------------------------------------------------------------
# write outputs
# ---------------------------------------------------------------------------
registrants.to_csv(D(f"registrants_{year}.csv"), index=False, lineterminator="\n")
share_classes.to_csv(D(f"share_classes_{year}.csv"), index=False, lineterminator="\n")

cls_by = {k: g for k, g in share_classes.groupby(["accession", "cik"])}
CLASS_FIELDS = ["class_or_series", "share_class_label", "share_type",
                "class_designator", "shares_outstanding", "as_of_date",
                "per_class_public_float", "per_class_float_basis", "validation",
                "quality_flags"]
with open(D(f"registrants_{year}.jsonl"), "w", newline="\n") as fh:
    for _, r in registrants.iterrows():
        rec = r.to_dict()
        g = cls_by.get((r["accession"], r["cik"]))
        rec["classes"] = ([] if g is None
                          else g[CLASS_FIELDS].to_dict(orient="records"))
        fh.write(json.dumps(rec) + "\n")


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------
print(f"registrants_{year}.csv     {len(registrants):>6} rows "
      f"({int((registrants['is_primary_filer']).sum())} primary, "
      f"{int((~registrants['is_primary_filer']).sum())} co-filers)")
print(f"  status       : {dict(registrants['status'].value_counts())}")
print(f"  shares_status: {dict(registrants['shares_status'].value_counts())}")
print(f"  float_status : {dict(registrants['float_status'].value_counts())}")
print(f"share_classes_{year}.csv   {len(share_classes):>6} rows "
      f"({int((share_classes['per_class_public_float'] != '').sum())} with per-class float)")
