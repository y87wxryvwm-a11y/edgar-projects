"""Cover-page public-float extractor — the documented methodology.

Extracts the aggregate market value of common equity held by non-affiliates
(the "public float") from the cover region of SEC annual filings: the dollar
value as printed, its printed precision, and the as-of date (the last
business day of the registrant's most recently completed second fiscal
quarter, per the 10-K cover instruction).

Every rule in this file is general: it describes a property of how filings
are written, never a fix for one particular filing. The shares-outstanding
extractor (cover_extractor.py) treats this very sentence as a decoy; here it
is the target, and the decoys are inverted — per-share closing prices, par
values, and share-count bases are what must be rejected.

The disclosure is a 10-K cover requirement (Rule 12b-2 accelerated-filer
determination); 20-F / 40-F covers carry no such line, so for those forms an
empty result is the expected outcome, not a miss. The extractor still scans
every form: a minority of FPIs disclose or tag a float voluntarily.

Shapes handled (each observed across many filings):

* prose — "... held by non-affiliates ... was [approximately] $X [million]"
* instruction + colon — "State the aggregate market value ...: $X" or the
  value on its own labeled line ("Common Stock: $263,926,859")
* value-first — "As of June 28, 2024, ... the aggregate market value ... was"
* no-float statements — "Not applicable", "no public trading market",
  "no voting or non-voting common equity", "wholly-owned subsidiary",
  "unable to determine"; a stated $0 is a value, not a statement.

Rows carry flags rather than silent judgment calls; the validation stage
(scrape vs the filer's own dei:EntityPublicFloat tags) decides what is
trusted.
"""

import re

from cover_extractor import (_DATE_RE, _SLASH_DATE_RE, _parse_date_match,
                             _PART_I_RE)

# ------------------------------------------------------------------- anchors

# The value head noun. Filers write "aggregate market value", "aggregate
# fair value", "aggregate market fair value", "aggregate stated value",
# plain "aggregate value", "aggregate quoted market price", or the plural
# ("aggregate market values ... were:" on per-class tables).
_MV_RE = re.compile(
    r"(?i)\b(?:aggregate\s+(?:quoted\s+)?(?:market\s+|fair\s+|stated\s+|"
    r"market\s+fair\s+)?|market\s+)(?:values?|prices?)\b")

# the equity noun that lets a market-value mention anchor WITHOUT an
# affiliate phrase (a minority of covers omit it: "the aggregate market
# value of its common stock was $42,171,430")
_EQUITY_NOUN_RE = re.compile(
    r"(?i)\bcommon\s+(?:stock|equity|shares)|\bvoting\s+stock|"
    r"\bcapital\s+stock|\bordinary\s+shares|\bshares\b|\bunits\b")

# The affiliate-exclusion phrase that makes the value the public float.
# "non-affiliates" is only one phrasing; "other than shares held by persons
# who may be deemed affiliates", "not held by affiliates", "stockholders who
# were not affiliates" are equally common, so the test is any affiliate word
# — plus the rare cover that spells out the exclusion as "not including
# ... held by directors and executive officers" without the word at all.
# NOTE: no \b before "affiliat" — "nonaffiliates" is routinely written as
# one unhyphenated word (KeyCorp, Truist, Thermo Fisher, ...)
_AFFIL_RE = re.compile(
    r"(?i)affiliat|\bunaffiliated\b|"
    r"not\s+including\s+(?:voting\s+)?(?:common\s+)?stock\s+held\s+by\s+"
    r"directors")

# How far the affiliate phrase may sit from "market value" and still be the
# same disclosure: covers interpose long parentheticals ("(assuming for
# these purposes, but without conceding, that all executive officers ...)")
# and whole sentences (Ford's Class B explanation).
_ANCHOR_REACH_BACK = 250
_ANCHOR_REACH_FWD = 650

# ------------------------------------------------------------------- numbers

_DOLLAR_RE = re.compile(
    r"\$\s*(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"
    r"(?:\s*(trillion|billion|million|thousand))?\b", re.I)

# a float stated with no dollar sign at all ("was approximately
# 6,514,462,779", "as of June 28, 2024: 182,778,968,633") — only
# comma-grouped or scale-worded numbers qualify, and only when bound
_BARE_NUM_RE = re.compile(
    r"(?<![\w.,$])(\d{1,3}(?:,\d{3})+(?:\.\d+)?)"
    r"(?:\s*(trillion|billion|million|thousand))?\b", re.I)

_SCALES = {"thousand": 1e3, "million": 1e6, "billion": 1e9,
           "trillion": 1e12}


def _value_and_tol(num_text, scale_word):
    """Dollar value and the half-step of its printed precision: '$5.6
    billion' is exact to ±0.05 billion; an unscaled figure is exact."""
    n = float(num_text.replace(",", ""))
    if not scale_word:
        return n, 0.0
    scale = _SCALES[scale_word.lower()]
    frac = len(num_text.split(".")[1]) if "." in num_text else 0
    return n * scale, 0.5 * scale / (10 ** frac)


# ------------------------------------------------------------ decoy defense

def _dollar_decoy(scope, start, end, value=None):
    """Why a dollar amount inside the float passage is NOT the float, or ''.
    The decoys are the float sentence's own furniture: the per-share price
    the value was computed from, par values, carved-out amounts."""
    pre = scope[max(0, start - 100):start]
    post = scope[end:end + 60]
    # "closing price ... of $16.25", "per share closing price on that date of
    # $10.29", "the last reported sale price ..., which was $159.45" — a
    # price head noun reaching the amount through "of" / "which was"
    if re.search(r"(?i)\bprices?\b[^.;$]{0,90}(?:\bof|which\s+was|"
                 r"which\s+is)\s*$", pre):
        return "PRICE_OF"
    # "last sale price of its stock was $0.0740" — price reaching a SMALL
    # amount through a bare "was". Commas and parens break the link ("(based
    # upon the closing sales price) was $1,017,910,995" is the float), and a
    # float-sized amount is never a per-share price
    if (value is None or value < 100000) and \
            re.search(r"(?i)\bprices?\b[^.;$,()]{0,40}\bwas\s*$", pre):
        return "PRICE_OF"
    # the market-value share basis: "(7,903,489 shares at $30.15)"
    if re.search(r"(?i)\bshares?\s+at\s*$", pre):
        return "MV_BASIS_PRICE"
    # "$19.80 closing price", "$0.59 per share", "$3.12. ..." apposition
    if re.match(r"(?i)\s*(?:\(?\s*per\s+share|closing|sales?\s+price|"
                r"bid|asked)", post):
        return "PER_SHARE"
    if re.search(r"(?i)\bpar\s+value\s+(?:of\s+)?$", pre) or \
            re.match(r"(?i)\s*par\s+value", post):
        return "PAR_VALUE"
    # carve-outs: "excludes $X of ..." (dollar exclusions are rare but real)
    if re.search(r"(?i)\b(?:excludes?|excluding|exclusive\s+of|"
                 r"after\s+deducting|net\s+of)\s*$", pre):
        return "EXCLUDED_AMOUNT"
    return ""


# --------------------------------------------------------- value binding

# the verb/colon/dash bridge that ties the amount to "market value was ..."
# — including "value of $X" itself, "approximated", and the instruction
# form's "(as of June 28, 2024) - $4,465,018,220" (the dash must not follow
# a digit: "March 3, 2025-11,853,489 shares" is date-glued, not bound)
_BIND_RE = re.compile(
    r"(?i)(?:\b(?:was|is|were|equaled|equalled|totaled|totalled|"
    r"amounted\s+to|aggregated|approximated)\b[^.;:$]{0,40}|"
    r"\bvalues?\s+of\s*|\bnon-?\s?affiliates?\s+of\s*|"
    r"\bapproximately\s*[:,]?\s*|[:=]\s*|"
    r"(?<![\d])[-–—]\s*)$")

# what a bare (dollar-sign-less) number must NOT run into: share counts and
# percentages are the bare numbers that live in the same passage
_BARE_POST_REJECT_RE = re.compile(
    r"(?i)^\s*(?:shares?|units?|%|percent|votes?|holders?|classes)\b")

# a comma in the wrong place ("$46,6133,592", "$2,700,697,72") — the digits
# are the disclosure, the grouping is a typo
_MALFORMED_NUM_RE = re.compile(r"\$\s*(\d{1,3}(?:,\d{2,4})+)\b")

# "(equivalent to US$17,351,769)" after a foreign-currency float
_USD_EQUIV_RE = re.compile(r"(?i)equivalent\s+to\s+(?:US|U\.S\.)\s*$")
# the letter glued to the $ in "A$26,195,303" / "C$1.2 billion" / "HK$..."
# — US$/U.S.$ stays domestic
_FOREIGN_PREFIX_RE = re.compile(r"(?<![A-Za-z.])(?!US\$|U\.S\.\$)"
                                r"[A-Z]{1,2}\$$")


def _scan_values(scope, anchor_off):
    """Every acceptable dollar amount in the passage, with binding class.
    Only when the passage has no dollar-signed amount at all may a bound
    comma-grouped bare number stand in — covers do print the float with the
    $ omitted ("was approximately 6,514,462,779") — and then only inside
    the anchor's own sentence, never across a boundary (where the bare
    numbers are the next disclosure's share counts)."""
    cands = []
    for m in _DOLLAR_RE.finditer(scope):
        value, tol = _value_and_tol(m.group(1), m.group(2))
        if _dollar_decoy(scope, m.start(), m.end(), value):
            continue
        pre = scope[max(0, m.start() - 60):m.start()]
        flags = []
        if _USD_EQUIV_RE.search(pre):
            flags.append("USD_EQUIVALENT")
        elif _FOREIGN_PREFIX_RE.search(scope[max(0, m.start() - 5):
                                             m.start() + 1]):
            flags.append("FOREIGN_CURRENCY_PREFIX")
        cands.append({
            "value": value, "tol": tol, "raw": m.group(0),
            "start": m.start(), "end": m.end(), "cflags": flags,
            "bound": bool(_BIND_RE.search(pre)) or "USD_EQUIVALENT" in flags,
        })
    # malformed comma grouping: the standard pattern stops at the typo, so
    # repair runs whenever a malformed span subsumes a parsed candidate
    for m in _MALFORMED_NUM_RE.finditer(scope):
        groups = m.group(1).split(",")
        if all(len(g) == 3 for g in groups[1:]):
            continue
        digits = float(m.group(1).replace(",", ""))
        pre = scope[max(0, m.start() - 60):m.start()]
        cands = [c for c in cands
                 if not (m.start() <= c["start"] < m.end())]
        cands.append({
            "value": digits, "tol": 0.0, "raw": m.group(0),
            "start": m.start(), "end": m.end(),
            "cflags": ["MALFORMED_NUMBER"],
            "bound": bool(_BIND_RE.search(pre)),
        })
    # coordinated lists ("was $X and $Y, respectively" / "$X, $Y and $Z"):
    # a value joined to a bound one by a bare conjunction inherits binding
    changed = True
    while changed:
        changed = False
        bound_ends = [c["end"] for c in cands if c["bound"]]
        for c in cands:
            if c["bound"]:
                continue
            gaps = [scope[e:c["start"]] for e in bound_ends
                    if 0 < c["start"] - e <= 30]
            if any(re.fullmatch(r"\s*(?:,|,?\s*and)\s*(?:approximately\s*)?",
                                g) for g in gaps):
                c["bound"] = True
                c["cflags"] = c.get("cflags", []) + ["COORDINATED"]
                changed = True
    # when the passage's only dollar amounts are price-sized ("...of
    # $1.07"), the real value may be a bare number ("was 2,228,681 based
    # upon the closing price ... of $1.07") — scan for one, and only if it
    # exists do the small dollar amounts get displaced
    small_only = bool(cands) and all(0 < c["value"] < 1000 for c in cands)
    if not cands or small_only:
        # a corporate-name period ("Inc.") is not a sentence boundary
        # (period swapped for a space so offsets stay aligned)
        plain = re.sub(r"\b(Inc|Corp|Co|Ltd|No)\.", r"\1 ", scope)
        bare = []
        for m in _BARE_NUM_RE.finditer(scope):
            if m.start() < anchor_off or \
                    re.search(r"[.;]", plain[anchor_off:m.start()]):
                continue
            if _BARE_POST_REJECT_RE.match(scope[m.end():m.end() + 30]):
                continue
            value, tol = _value_and_tol(m.group(1), m.group(2))
            if value < 10000:
                continue
            if _dollar_decoy(scope, m.start(), m.end(), value):
                continue
            pre = scope[max(0, m.start() - 60):m.start()]
            if _BIND_RE.search(pre):
                bare.append({
                    "value": value, "tol": tol, "raw": m.group(0),
                    "start": m.start(), "end": m.end(), "bound": True,
                    "cflags": ["NO_DOLLAR_SIGN"],
                })
        if bare:
            if small_only:
                cands = []
            cands += bare
    # a stated foreign-currency amount with a printed USD equivalent: the
    # USD figure is the comparable disclosure
    if any("USD_EQUIVALENT" in c["cflags"] for c in cands):
        cands = [c for c in cands
                 if "FOREIGN_CURRENCY_PREFIX" not in c["cflags"]]
    # a bound sub-$1,000 amount next to a bound real float is a per-share
    # price that slipped the guards, never the disclosure
    big = [c for c in cands if c["bound"] and c["value"] >= 100000]
    if big:
        cands = [c for c in cands
                 if not (c["bound"] and 0 < c["value"] < 1000)]
    return cands


# ------------------------------------------------- labeled-line table form

# "the aggregate market values ... were: / Class A / $ / 5,603,520,725 /
# Class B / 92,655,504,471 / $ / 98,259,025,196", per-registrant
# ("PINNACLE WEST CAPITAL CORPORATION / $ / 8,663,553,568 / as of June 30,
# 2024 / ARIZONA PUBLIC SERVICE COMPANY / $ / 0 ...") and per-fund
# (ProShares) tables — one value per labeled line after the instruction
# sentence's colon (or "in the table below"). Where a table carries TWO
# numeric columns (float + share count), the float is the FIRST number
# after each label; later numbers in the same label group are taken only
# when introduced by their own "$" line (Costco's total). "None" / "No
# established market" lines are stated no-floats (kept as zero rows).
_TABLE_NUM_RE = re.compile(
    r"(\$)?\s*(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"
    r"\s*(trillions?|billions?|millions?|thousands?)?"
    r"\s*(?:\(\d\))?\*?", re.I)
_TABLE_NONE_RE = re.compile(
    r"(?i)(?:none|n/a|not\s+applicable|no\s+established\s+(?:public\s+)?"
    r"(?:trading\s+)?market|wholly\s+owned(?:\s+by\s+.{0,50})?)"
    r"\s*(?:\(\d\))?")


def _scan_table(scope, anchor_off, require_colon=True):
    if require_colon:
        cm = re.search(r"(?i)\b(?:were|was|as\s+follows?|approximately)"
                       r"\s*:\s*\n|\btable\s+below\s*[.:]\s*\n?",
                       scope[anchor_off:])
        if not cm:
            return []
        between = re.sub(r"\b(Inc|Corp|Co|Ltd|No)\.", r"\1 ",
                         scope[anchor_off:anchor_off + cm.start()])
        if re.search(r"[.;]", between):
            return []  # the intro belongs to a later sentence
        base = anchor_off + cm.end()
    else:
        nl = scope.find("\n", anchor_off)
        if nl < 0:
            return []
        base = nl + 1
    rows = []
    label = ""
    after_label = False  # a number was already taken for this label
    dollar_line = False
    pos = base
    header_budget = 8    # header lines tolerated before the first value
    for line in scope[base:].split("\n")[:40]:
        start = pos
        pos += len(line) + 1
        line = line.strip()
        if line == "$":
            dollar_line = True
            continue
        if line == "":
            continue
        m = _TABLE_NUM_RE.fullmatch(line)
        # a number line is a value only when it looks like money: its own
        # "$" cell, comma grouping, a scale word, or a bare 0 — TOC page
        # numbers and year fragments ("3", "2024") never qualify
        if m and (m.group(1) or dollar_line or "," in m.group(2) or
                  m.group(3) or m.group(2) == "0"):
            if after_label and not dollar_line and not m.group(1):
                continue       # second numeric column (share counts)
            value, tol = _value_and_tol(m.group(2), m.group(3))
            rows.append({"value": value, "tol": tol, "raw": line,
                         "start": start, "end": start + len(line),
                         "bound": True, "label": label, "cflags": []})
            after_label = True
            dollar_line = False
            continue
        dollar_line = False
        if m:
            if not rows and header_budget > 0:
                header_budget -= 1
            continue           # unqualified number (page no., bare year)
        if _TABLE_NONE_RE.fullmatch(line):
            rows.append({"value": 0.0, "tol": 0.0, "raw": line,
                         "start": start, "end": start + len(line),
                         "bound": True, "label": label,
                         "cflags": ["NONE_STATED"]})
            after_label = True
            continue
        if _DATE_RE.fullmatch(line) or re.match(r"(?i)as\s+of\b", line) or \
                re.fullmatch(r"\(?\$?[\d.,]+\s*par\s+value\)?|"
                             r"\(no\s+par\s+value\)", line, re.I):
            continue           # as-of / par-value annotation columns
        if re.fullmatch(r"[A-Za-z0-9($-][\w .,'&%()$-]{0,70}", line) and \
                not re.match(r"(?i)documents?\s+incorporated|"
                             r"table\s+of\s+contents|indicate\b", line):
            label = line
            after_label = False
            continue
        if not rows and header_budget > 0:
            header_budget -= 1
            continue           # long header line before the values
        break
    return rows if len(rows) >= 2 else []


# ------------------------------------------------------------ label hygiene

# A candidate label that is really a fragment of the disclosure sentence
# itself (instruction-form covers put the whole sentence before the colon;
# windowed grabs clip it mid-phrase) — never a class, series, or registrant
# designation. Genuine labels (class names, fund series, registrant names)
# never contain the disclosure's own machinery words.
_LABEL_JUNK_RE = re.compile(
    r"(?i)affiliat|\bregistrant\b|\bheld\b|market\s+value|\bas\s+of\b|"
    r"\bsold\b|\bbid\b|\basked\b|\breported\b|\bclosing\b|\bsale\s+price\b|"
    r"\bfiscal\b|\bquarter\b|\bwas\b|\bon\s+(?:that|such)\s+date\b|"
    r"\bsuch\s+date\b|\bfollowing\b|\bbased\s+(?:on|upon)\b|"
    r"\bper\s+share\b|"
    # a label that STARTS with a function word is a clipped phrase from the
    # disclosure sentence, never a designation
    r"^(?:of|on|at|by|in|to|for|with|and|or|the|was|such|per)\b")


def _clean_label(label):
    """'' for disclosure-sentence fragments and bare dates; the label
    otherwise."""
    label = (label or "").strip()
    if not label:
        return ""
    if _LABEL_JUNK_RE.search(label) or _DATE_RE.fullmatch(label):
        return ""
    return label


# ------------------------------------------------------------- date binding

# context that makes a date the price-observation date, not the as-of date
_PRICE_CTX_RE = re.compile(
    r"(?i)\b(?:clos\w*|sales?|sold|prices?[d]?|bid|asked|quoted|reported|"
    r"trading|traded)\b[^.;]{0,60}$")
# context that makes a date something else entirely (split effectiveness,
# meeting dates, incorporation-by-reference deadlines)
_OTHER_CTX_RE = re.compile(
    r"(?i)\b(?:effective|split|meeting|amendment|dividend|filed|deadline|"
    r"incorporat\w+)\b[^.;]{0,50}$")
_ASOF_CTX_RE = re.compile(r"(?i)\b(?:as\s+of|on|at)\s*$")


# a date separated from the value by a sentence end (or a structural marker
# like a table-of-contents line) belongs to the NEXT disclosure — almost
# always the shares-outstanding as-of date
_SENT_BOUNDARY_RE = re.compile(r"[.;]\s|\ntable\s+of\s+contents\n", re.I)


def _float_date(cover, lo, hi, ref_pos):
    """The as-of date of the float passage cover[lo:hi]: nearest date to
    ref_pos, preferring in-sentence as-of-bound dates over price-observation
    dates over unrelated-context dates, with any date across a sentence
    boundary from the value demoted below all of those.
    Returns (iso_date, flag)."""
    window = cover[lo:hi]
    cands = []
    for m in list(_DATE_RE.finditer(window)) + \
            list(_SLASH_DATE_RE.finditer(window)):
        iso = _parse_date_match(m)
        if not iso:
            continue
        pre = window[max(0, m.start() - 70):m.start()]
        if _OTHER_CTX_RE.search(pre):
            rank = 3
        elif _PRICE_CTX_RE.search(pre):
            rank = 2
        elif _ASOF_CTX_RE.search(pre):
            rank = 0
        else:
            rank = 1
        a, b = sorted((lo + m.start(), ref_pos))
        between = cover[max(lo, a):min(hi, b)]
        if _SENT_BOUNDARY_RE.search(between):
            rank += 4
        cands.append((rank, abs(lo + m.start() - ref_pos), iso))
    if not cands:
        return "", "NO_DATE_STATED"
    cands.sort()
    rank, _, iso = cands[0]
    if rank >= 4:
        # only across-boundary dates exist — the float sentence itself is
        # undated; better no date (the tag instant fills it) than the next
        # disclosure's date
        return "", "NO_DATE_STATED"
    flag = {0: "", 1: "DATE_LOOSE", 2: "DATE_FROM_PRICE_CONTEXT",
            3: "DATE_OTHER_CONTEXT"}[rank]
    return iso, flag


# ------------------------------------------------------ no-float statements

_NO_FLOAT_RES = [
    ("NA_STATED", re.compile(
        r"(?i)\bnot?\s+applicable\b|\bn/a\b|(?<![\w$])none(?![\w])")),
    ("NO_PUBLIC_MARKET", re.compile(
        r"(?i)\bno\s+(?:established\s+|active\s+)?public\s+"
        r"(?:trading\s+)?market\b|\bnot\s+publicly\s+traded\b|"
        r"\bnot\s+held\s+by\s+any\s+non-?\s?affiliat|"
        r"\bno\s+(?:of\s+)?(?:its|the)?\s*(?:voting\s+or\s+non-?voting\s+)?"
        r"common\s+equity\s+(?:is|was)\s+held\s+by\s+non-?\s?affiliat")),
    ("NO_COMMON_EQUITY", re.compile(
        r"(?i)\b(?:has|have)\s+no\s+(?:voting\s+or\s+non-?voting\s+)?"
        r"(?:common\s+(?:equity|stock)|publicly\s+traded\s+"
        r"(?:common\s+)?(?:equity|stock|shares))")),
    ("WHOLLY_OWNED", re.compile(
        r"(?i)\bwholly[\s-]owned\s+(?:direct\s+|indirect\s+)?subsidiar")),
    ("INDETERMINABLE", re.compile(
        r"(?i)\b(?:unable\s+to\s+(?:calculate|determine|compute)|"
        r"can\s*not\s+(?:be\s+)?calculate[d]?|"
        r"cannot\s+be\s+(?:calculated|determined|computed)|"
        r"not\s+(?:be\s+)?(?:reasonably\s+)?(?:calculable|determinable|"
        r"determined|calculated)|"
        r"was\s+not\s+a\s+public\s+company|"
        r"did\s+not\s+have\s+an?\s+aggregate\s+market\s+value)")),
    ("ZERO_WORD", re.compile(
        r"(?i)\b(?:was|is)\s+(?:\$\s?0(?:\.0+)?\b|zero\b|nil\b|"
        r"\$?\s*-\s*0\s*-)|"
        r"estimated\s+to\s+have\s+no\s+value")),
]


_STATEMENT_END_RE = re.compile(
    r"(?i)documents?\s+incorporated|indicate\s+the\s+number|"
    r"the\s+number\s+of\s+shares|number\s+of\s+shares\s+outstanding")


def _no_float_statement(scope, anchor_off):
    """A no-float statement is only believed when it sits in the anchor's
    own passage — not in the boilerplate beyond it ('Documents Incorporated
    by Reference: None' must never read as a stated no-float)."""
    zone = scope[anchor_off:anchor_off + 520]
    em = _STATEMENT_END_RE.search(zone)
    if em:
        zone = zone[:em.start()]
    zone = scope[max(0, anchor_off - 160):anchor_off] + zone
    for kind, rx in _NO_FLOAT_RES:
        if rx.search(zone):
            return kind
    return ""


# ------------------------------------------------------------- cover region

def _cover_region(text, flags):
    """Same scoping as the shares extractor: everything before the first
    PART I heading, capped at 15k chars (some filers put financials early;
    the legally required cover is always at the very top)."""
    m = _PART_I_RE.search(text) or re.search(r"(?im)^\s*part\s+i\b", text)
    if m:
        cover = text[:m.start()]
        if len(cover) > 15000:
            cover = text[:15000]
            flags.append("COVER_CAPPED")
    else:
        # no PART I heading: end the cover at "documents incorporated by
        # reference" — the one marker that reliably follows the cover
        # disclosures. ("Table of Contents" is useless: iXBRL documents
        # print it as a breadcrumb at every page break, including mid-cover.)
        e = re.search(r"(?im)documents\s+incorporated\s+by\s+reference",
                      text[:15000])
        cover = text[:e.end() + 200] if e else text[:15000]
        flags.append("NO_COVER_MARKERS")
    return cover


# ------------------------------------------------------------------ extract

def extract_float(text, form, period_of_report="", multi_registrant=False,
                  filed_date=""):
    """Extract the public-float disclosure from one filing's clean text.

    Returns (rows, filing_flags). Each row:
        value     float dollars as printed (0.0 is a stated zero)
        tol       half-step of the printed precision (0.0 = exact figure)
        raw       the matched dollar text
        as_of     ISO date or ""
        label     the labeled line's label for colon-bound values, else ""
        method    how the value was found
        flags     row-level flags

    filing_flags carries NO_FLOAT_STATEMENT:<KIND> when the cover addresses
    the disclosure without a number, and NO_FLOAT_ANCHOR when the cover never
    mentions it (the norm on 20-F / 40-F)."""
    flags = []
    cover = _cover_region(text, flags)
    # date-glue repair: "$ 610,681on June 30, 2024" — a missing space
    # between the value and its as-of phrase
    cover = re.sub(
        r"(?i)(\d)(on\s+(?:january|february|march|april|may|june|july|"
        r"august|september|october|november|december)\s)", r"\1 \2", cover)

    # anchor: every "market value" tied to an affiliate-exclusion phrase;
    # failing that, tied to an equity noun (a minority of covers state the
    # float without mentioning affiliates at all)
    anchors = []
    for require_affil in (True, False):
        for m in _MV_RE.finditer(cover):
            lo = max(0, m.start() - _ANCHOR_REACH_BACK)
            hi = min(len(cover), m.end() + _ANCHOR_REACH_FWD)
            near = cover[lo:hi]
            if require_affil:
                ok = _AFFIL_RE.search(near)
            else:
                ok = _EQUITY_NOUN_RE.search(
                    cover[max(0, m.start() - 60):m.end() + 120])
            if ok:
                if anchors and m.start() - anchors[-1][1] < 200:
                    anchors[-1] = (anchors[-1][0], m.end())  # same passage
                else:
                    anchors.append((m.start(), m.end()))
        if anchors:
            break
    if not anchors:
        flags.append("NO_FLOAT_ANCHOR")
        return [], flags
    if not _AFFIL_RE.search(cover):
        flags.append("NO_AFFILIATE_PHRASE")

    rows = []
    seen = set()
    for a_start, a_end in anchors:
        scope_lo = max(0, a_start - 160)
        scope_hi = min(len(cover), a_end + 800)
        scope = cover[scope_lo:scope_hi]
        anchor_off = a_start - scope_lo

        table = _scan_table(scope, anchor_off)
        cands = table or _scan_values(scope, anchor_off)
        if not table and not any(c["bound"] for c in cands):
            # colon-less registrant tables (AEP's reduced-format header)
            table = _scan_table(scope, anchor_off, require_colon=False)
            if table:
                cands = table
        method = "FLOAT_COVER_TABLE" if table else "FLOAT_COVER_SCAN"

        bound = [c for c in cands if c["bound"]]
        row_flags = []
        if bound:
            picks = [bound[0]]
            # additional bound values in the same passage: per-class
            # components or a second registrant — keep them, visibly
            for c in bound[1:]:
                if all(c["value"] != p["value"] for p in picks):
                    picks.append(c)
            if len(picks) > 1:
                row_flags.append("MULTI_VALUE")
        elif cands:
            # an unbound pick below $1,000 is a per-share price or par value
            # whose guard context fell outside the scope slice, never a
            # float (real sub-$1,000 floats are always verb-bound)
            loose = [c for c in cands if c["value"] >= 1000]
            if not loose:
                kind = _no_float_statement(scope, anchor_off)
                if kind and kind != "ZERO_WORD":
                    flags.append("NO_FLOAT_STATEMENT:" + kind)
                else:
                    flags.append("ANCHOR_NO_VALUE")
                continue
            picks = [max(loose, key=lambda c: c["value"])]
            row_flags.append("LOOSE_BIND")
        else:
            kind = _no_float_statement(scope, anchor_off)
            if kind == "ZERO_WORD":
                # a stated zero is a value, not a missing disclosure
                as_of, date_flag = _float_date(
                    cover, max(0, a_start - 160),
                    min(len(cover), a_end + 400), a_start)
                if (0.0, as_of) not in seen:
                    seen.add((0.0, as_of))
                    rows.append({
                        "value": 0.0, "tol": 0.0, "raw": "zero",
                        "as_of": as_of, "label": "",
                        "method": "FLOAT_STATED_ZERO",
                        "flags": ["ZERO_STATED"] +
                                 ([date_flag] if date_flag else []),
                    })
            elif kind:
                flags.append("NO_FLOAT_STATEMENT:" + kind)
            else:
                flags.append("ANCHOR_NO_VALUE")
            continue

        # the per-share prices the guards rejected in this passage: a kept
        # "float" equal to one of them is the filer printing the share price
        # where the aggregate belongs (a cover defect — the true float is
        # not recoverable from the filing)
        price_vals = set()
        for pm in _DOLLAR_RE.finditer(scope):
            pv, _ = _value_and_tol(pm.group(1), pm.group(2))
            if _dollar_decoy(scope, pm.start(), pm.end(), pv) in (
                    "PRICE_OF", "PER_SHARE", "MV_BASIS_PRICE"):
                price_vals.add(pv)

        for c in picks:
            v_end_abs = scope_lo + c["end"]
            date_lo = max(0, a_start - 160)
            date_hi = min(len(cover), v_end_abs + 220)
            as_of, date_flag = _float_date(
                cover, date_lo, date_hi, scope_lo + c["start"])
            # a colon-bound value may sit on a labeled line — keep the label
            label = c.get("label", "")
            s0 = scope[max(0, c["start"] - 110):c["start"]]
            lm = re.search(r"([A-Za-z][\w .,'&-]{0,90}?)\s*:\s*$", s0)
            if not label and lm and not re.search(
                    r"(?i)quarter|follows|reference", lm.group(1)):
                label = lm.group(1).strip(" ,.")
                if lm.start(1) > 0 and s0[lm.start(1) - 1].isalnum():
                    # the window clipped the label mid-word — drop the
                    # partial first token rather than keep a fragment
                    label = label.split(" ", 1)[1] if " " in label else ""
            label = _clean_label(label)
            if c["value"] in price_vals and 0 < c["value"] < 100000:
                c["cflags"] = c.get("cflags", []) + [
                    "FLOAT_EQUALS_STATED_PRICE"]
            r_flags = sorted(set(row_flags + c.get("cflags", []) +
                                 ([date_flag] if date_flag else [])))
            key = (c["value"], as_of)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "value": c["value"], "tol": c["tol"], "raw": c["raw"],
                "as_of": as_of, "label": label,
                "method": method, "flags": r_flags,
            })

    # the same value seen with and without a date (two phrasings of one
    # disclosure) is one row — the dated one
    dated_values = {r["value"] for r in rows if r["as_of"]}
    rows = [r for r in rows if r["as_of"] or r["value"] not in dated_values]

    # covers that print a voluntary "current" value alongside the required
    # second-fiscal-quarter one ("$82.6 billion as of January 31, 2025
    # (approximately $56.1 billion as of June 30, 2024, the last business
    # day ...)"): the disclosure of record is the older, quarter-end value;
    # a value dated within ~60 days of filing is the courtesy update
    if filed_date and len({r["value"] for r in rows}) > 1:
        try:
            fd = _date_ord(filed_date)
        except ValueError:
            fd = None
        if fd is not None:
            dated = [r for r in rows if r["as_of"]]
            early = [r for r in dated if fd - _date_ord(r["as_of"]) > 90]
            late = [r for r in dated if fd - _date_ord(r["as_of"]) <= 60]
            if early and late:
                rows = [r for r in rows if r not in late]
                flags.append("CURRENT_FLOAT_DROPPED")

    # per-class/per-registrant components printed with their total: mark
    # both so the assembly stage can prefer the total (the float concept is
    # an entity-level aggregate)
    if len(rows) >= 3:
        for r in rows:
            others = sum(o["value"] for o in rows if o is not r)
            tol = max(max((o["tol"] for o in rows), default=0),
                      0.005) * len(rows)
            if abs(r["value"] - others) <= tol:
                r["flags"] = sorted(set(r["flags"] + ["TOTAL_OF_COMPONENTS"]))
                if r["label"].strip().lower() == "total":
                    r["label"] = ""  # "Total" names the sum, not a class
                for o in rows:
                    if o is not r:
                        o["flags"] = sorted(set(o["flags"] + ["COMPONENT"]))
                break

    if len({r["value"] for r in rows}) > 1:
        flags.append("MULTI_FLOAT")
    return rows, flags


def _date_ord(iso):
    import datetime
    return datetime.date(*map(int, iso.split("-"))).toordinal()
