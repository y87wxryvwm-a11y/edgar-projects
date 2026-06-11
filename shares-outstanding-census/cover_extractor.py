"""Cover-page shares-outstanding extractor — the documented methodology.

Every rule in this file is general: it describes a property of how SEC annual
filings are written, never a fix for one particular filing. The extractor
reads the clean text of a filing's primary document (see
census_lib.doc_to_text) and returns one row per share class found on the
cover, plus filing-level flags that make misses and doubts visible.

Two strategies, dispatched by form type:

* 10-K — cover-window scan. There is no fixed phrase, so the scan is
  restricted to the cover region (everything before the first "PART I"
  heading) and, around every occurrence of "outstanding", looks for a number
  tied to a share-class noun while rejecting the known decoy shapes
  (dollar amounts, authorized/treasury/weighted-average counts, record-holder
  counts, vote counts, calendar dates).

* 20-F / 40-F — regulatory anchor. Both covers carry a fixed instruction
  ("... number of outstanding shares of each of the issuer's classes of
  capital or common stock as of the close of the period ..."); the
  class/number listing that follows is parsed in either orientation
  (label-first or number-first). When no date is printed, the form's own
  rule supplies it: the count is "as of the close of the period covered by
  the annual report", i.e. the filing's CONFORMED PERIOD OF REPORT
  (flagged DATE_FROM_PERIOD so the provenance stays visible).

Rows carry flags rather than silent judgment calls; the validation stage
(scrape vs the filer's own inline-XBRL tags) decides what is trusted.
"""

import re

# --------------------------------------------------------------- vocabulary

# Words that may precede the share-noun core as part of the class label.
_QUALIFIER_WORDS = {
    "class", "series", "common", "ordinary", "preferred", "preference",
    "cumulative", "redeemable", "convertible", "exchangeable", "voting",
    "non-voting", "nonvoting", "subordinate", "subordinated", "multiple",
    "limited", "restricted", "deferred", "special", "participating",
    "senior", "junior", "perpetual", "depositary", "depository", "american",
    "capital", "registered", "bearer", "first", "second", "new", "publicly",
    "held", "no", "par", "value", "without", "of", "and", "the", "its",
}

# The noun core a share-count must attach to.
_NOUN_CORE = re.compile(r"""(?ix)
    \b(
        class\s+[a-z0-9]{1,4}\s+
            (?:(?:exchangeable|common|ordinary|preferred|preference|special|
                convertible|redeemable|cumulative|voting|non[\s-]?voting|
                subordinate|multiple|limited)\s+){0,3}(?:stock|shares?) |
        common\s+stock | common\s+shares? | ordinary\s+shares? |
        preferred\s+stock | preferred\s+shares? | preference\s+shares? |
        capital\s+stock | capital\s+shares? | equity\s+shares? |
        american\s+depositary\s+shares? | depositar?y\s+shares? | adss? |
        (?:subordinate|multiple|limited|restricted|super)[\s-]+voting\s+shares? |
        non[\s-]?voting\s+(?:common\s+)?shares? |
        common\s+units? | ordinary\s+units? | limited\s+partner(?:ship)?\s+units? |
        units?\s+representing\s+assignments?\s+of\s+beneficial\s+ownership |
        shares?\s+of\s+(?:the\s+)?(?:registrant|company|issuer)\W{0,3}s\s+
            (?:class\s+[a-z0-9]{1,4}\s+)?(?:common|capital|preferred)\s+stock |
        shares?\s+of\s+(?:class\s+[a-z0-9]{1,4}\s+)?(?:common|capital|preferred)\s+stock |
        shares?\s+of\s+(?:class\s+[a-z0-9]{1,4}\s+)?(?:common|ordinary|preferred)\s+shares? |
        registered\s+shares? | bearer\s+shares? |
        shares? | stock | units?
    )\b""")

_STRONG_NOUNS = ("stock", "share", "unit", "ads")

# Number: thousands-separated, or plain digits, or decimal + scale word.
_NUM_RE = re.compile(
    r"(?<![\w.,])(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"
    r"(?:\s*(million|billion|thousand))?\b", re.I)

_SCALES = {"thousand": 1e3, "million": 1e6, "billion": 1e9, None: 1, "": 1}

_MONTHS = {m: i + 1 for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"])}
_MONTH_ABBR = {m[:3]: i for m, i in _MONTHS.items()}

_DATE_RE = re.compile(r"""(?ix)
    \b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun e?|jul y?|
       aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)
      \.?\s+(\d{1,2})\s*,\s*(\d{4})\b
  | \b(\d{1,2})\s+(january|february|march|april|may|june|july|august|
       september|october|november|december)\s*,?\s*(\d{4})\b""".replace(" e?", "e?").replace(" y?", "y?"))


_SLASH_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")


def _parse_date_match(m):
    if m.re is _SLASH_DATE_RE:
        mon, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    elif m.group(1):
        mon = _MONTH_ABBR.get(m.group(1).lower()[:3])
        day, year = int(m.group(2)), int(m.group(3))
    else:
        mon = _MONTHS.get(m.group(5).lower())
        day, year = int(m.group(4)), int(m.group(6))
    if not mon or not (1 <= int(mon) <= 12) or not (1 <= day <= 31) \
            or not (1900 <= year <= 2100):
        return None
    return "%04d-%02d-%02d" % (year, mon, day)


def _num_value(num_text, scale_word):
    v = float(num_text.replace(",", "")) * _SCALES[(scale_word or "").lower() or None]
    return int(round(v))


# ------------------------------------------------------------ decoy defense

def _decoy(text, start, end):
    """Why a number near 'outstanding' is NOT a share count, or ''. Each test
    is a documented trap observed across many filings, not a one-off fix."""
    pre = text[max(0, start - 80):start]
    post = text[end:end + 80]
    if re.search(r"[\$£€¥₩₹]\s*\(?\s*$", pre):
        return "DOLLAR"                      # market/par value, any currency
    if re.search(r"(?i)\bweighted[\s-]+average\b", pre):
        return "WEIGHTED"                    # EPS share counts
    if re.search(r"(?i)^\s*(?:record\s+holders?|holders?\s+of\s+record|"
                 r"(?:share|stock)holders?\s+of\s+record)", post):
        return "HOLDERS"                     # record-holder counts
    # "authorized to issue N ..." and "N shares authorized" are capacity, not
    # count — but "N shares authorized, issued and outstanding" is both
    if re.search(r"(?i)\bauthori(?:zed|ty)\s+to\s+issue\s*$", pre):
        return "AUTHORIZED"
    if re.search(r"(?i)^\s*(?:shares?\s+)?authorized\b", post) and not \
            re.search(r"(?i)^\s*shares?\s+authorized[,;]?\s+"
                      r"(?:issued\s+and\s+|and\s+)?outstanding", post):
        return "AUTHORIZED"
    if re.search(r"(?i)^\s*votes?\b", post):
        return "VOTES"                       # votes-per-share tables
    if post[:2].strip().startswith("%"):
        return "PERCENT"
    # treasury: reject only when treasury labels THIS number — "N shares held
    # in treasury" — not when the count merely follows an "(exclusive of
    # treasury shares)" clause
    if re.search(r"(?i)\b(?:in\s+)?treasury\s*(?:shares?|stock)?\s*"
                 r"(?:was|were|:)?\s*$", pre[-40:]) or \
            re.search(r"(?i)^\s*(?:shares?\s+)?(?:held\s+in|of|in)?\s*treasury\b", post):
        return "TREASURY"
    # carve-outs: "including N shares held by ..." / "excluding N shares ..."
    # — but "TOTAL, including N Class A ... and M Class B ..." introduces the
    # per-class breakdown we want; a Class/Series-led phrase stays a candidate
    if re.search(r"(?i)\b(?:includ(?:ing|es)|exclud(?:ing|es)|exclusive\s+of|"
                 r"after\s+deducting|net\s+of|does\s+not\s+include)\s*$", pre) \
            and not re.match(r"(?i)\s*(?:class|series)\s+[a-z0-9]", post):
        return "SUBSET"
    # "75,000,000 shares issued and 60,000,000 shares outstanding" — the
    # issued figure is capacity history, not the count; the fused phrase
    # "issued and outstanding" (no second number) stays untouched
    if re.match(r"(?i)\s*(?:shares?\s+)?(?:were\s+)?issued[,;]?\s+and\s+[\d,]+",
                post):
        return "ISSUED_SPLIT"
    # derivative counts ("4,018,384 Warrants to purchase ...") — but only
    # when the derivative word is what the number binds to; "59,888,304
    # common shares and 53,900,329 warrants" must keep the share count
    if re.search(r"(?i)^\s*(?:\w+\s+){0,2}(?:options?|warrants?|rsus?|"
                 r"restricted\s+stock\s+units?)\b", post[:60]) and not \
            _NOUN_CORE.match(post.lstrip()[:60]):
        return "DERIVATIVE"
    if re.search(r"(?i)\b(?:section|rule)\s*$", pre) or \
            re.match(r"\s*\(\s*[a-z]\d?\s*\)", post):
        return "STATUTE_REF"                 # the 12 in "Section 12(b)"
    if re.search(r"(?i)\bfile\s+(?:number|no\.?)\s*:?\s*$", pre):
        return "FILE_NUMBER"
    # ownership breakdowns ("19,222,141 held by the ESOP and ...") and
    # carve-outs the cover itself excludes from the outstanding count
    if re.match(r"(?i)\s*(?:[\w']+\s+){0,3}held\s+(?:directly\s+|indirectly\s+)?"
                r"(?:by|as|in)\b", post) and not \
            re.match(r"(?i)\s*(?:[\w']+\s+){0,3}held\s+(?:directly\s+)?by\s+non",
                     post) and not \
            re.search(r"(?i)\boutstanding\b", post[:40]):
        return "HELD_BY_BREAKDOWN"
    if re.search(r"(?i)\b(?:also\s+)?holds?\s*$", pre):
        return "HELD_BY_BREAKDOWN"           # "... also hold N units in a sub"
    if re.match(r"(?i)\s*(?:[\w']+\s+){0,2}at\s+a\s+closing\s+price\b", post):
        return "MV_BASIS"                    # "(N shares at a closing price of $...)"
    if re.search(r"(?i)\bauditor\s+firm\s+id\s*:?\s*$", pre) or \
            re.match(r"(?i)\s*auditor\s+name", post):
        return "AUDITOR_ID"                  # the PCAOB firm ID under the count
    if re.search(r"(?i)\breserved\s+for\b", post[:60]) or \
            re.search(r"(?i)^\s*(?:shares?\s+)?(?:repurchased|reserved)\b", post):
        return "RESERVED"
    if re.search(r"(?i)\brepurchased\s*$", pre):
        return "RESERVED"
    if re.search(r"(?i)(?:january|february|march|april|may|june|july|august|"
                 r"september|october|november|december)\.?\s*$", pre):
        return "DATE_DAY"                    # the 17 in "October 17, 2025"
    # the share basis of the market-value line ("based on N shares held by
    # non-affiliates") — but only when no sentence break separates the phrase
    # from the number, else the adjacent market-value SENTENCE would condemn
    # a legitimate count in the next sentence
    # a sentence boundary is [.;:] or a newline followed by a capital — the
    # latter catches market-value sentences that start on their own line
    _boundary = r"[.;:]|\n(?=[A-Z])"
    mpre = re.search(r"(?i)\bheld\s+by\s+non-?\s?affiliates\b", pre[-90:])
    if mpre and not re.search(_boundary, pre[-90:][mpre.end():]):
        return "NONAFFILIATE"
    if re.search(r"(?i)\bnon-?\s?affiliates\s+held\s*$", pre):
        return "NONAFFILIATE"                # "non-affiliates held N shares ..."
    mpost = re.search(r"(?i)\bheld\s+by\s+non-?\s?affiliates\b", post[:90])
    if mpost and not re.search(_boundary, post[:90][:mpost.start()]):
        return "NONAFFILIATE"
    if re.search(r"(?i)^\s*classes\b", post):
        return "CLASS_COUNT"                 # "3 classes of common stock"
    if re.search(r"(?i)\b(?:class|series)\s*$", pre):
        return "CLASS_NUMERAL"               # the 1 in "Class 1 Common Stock"
    return ""


def _is_bare_year(num_text, scale_word):
    return (not scale_word and "," not in num_text and "." not in num_text
            and re.fullmatch(r"(19|20)\d{2}", num_text))


# ----------------------------------------------------------- label building

def _expand_label(text, noun_start, noun_end):
    """Walk back from the share-noun over qualifier words ('Class A',
    'Series B', '5.25%', 'cumulative redeemable') to the full class label."""
    label_start = noun_start
    pos = noun_start
    while True:
        m = re.search(r"([A-Za-z][\w.%-]*|\d+(?:\.\d+)?%?)\s+$",
                      text[max(0, pos - 40):pos])
        if not m:
            break
        word = m.group(1)
        lw = word.lower().rstrip(".,;:")
        is_rate = bool(re.fullmatch(r"\d+(?:\.\d+)?%", lw))
        ok = (lw in _QUALIFIER_WORDS
              or re.fullmatch(r"[a-z0-9]{1,4}", lw)  # the A in "Class A", 1 in "Class 1"
              or is_rate)                            # rate of a preferred series
        if not ok:
            break
        pos -= len(m.group(0))
        if (lw in ("class", "series") or is_rate
                or lw in _QUALIFIER_WORDS - {"of", "and", "the", "its"}):
            label_start = pos
    label = text[label_start:noun_end]
    label = re.sub(r"\s+", " ", label).strip(" ,.;:")
    return label


def classify_label(label):
    """Map a class label to (share_type, class_designator)."""
    low = " " + label.lower() + " "
    if "preferred" in low or "preference" in low:
        share_type = "preferred"
    elif "depositary" in low or "depository" in low or " ads" in low:
        share_type = "depositary"
    elif " unit" in low or " units" in low:
        share_type = "unit"
    elif "ordinary" in low:
        share_type = "ordinary"
    elif "common" in low:
        share_type = "common"
    else:
        share_type = "other"
    desig = ""
    m = re.search(r"(?i)\bclass\s+([a-z0-9]{1,2}\d?)\b", label)
    if m:
        desig = m.group(1).upper()
    else:
        m = re.search(r"(?i)\bseries\s+([a-z0-9]{1,6})\b", label)
        if m:
            desig = m.group(1).upper()
    return share_type, desig


_BARE_NOUNS = ("shares", "share", "stock", "units", "unit")


def _nearest_noun(text, num_start, num_end, reach=120):
    """The share-noun phrase a number belongs to. A qualified noun ("Common
    Stock") wins over a nearer bare one ("share" from "per share"); a bare
    noun only counts when adjacent (within 40 chars).

    Direction: the canonical phrasing is "N shares of CLASS", so an
    after-noun connected by nothing but "shares of ..." BINDS the number —
    in "N1 shares of X and N2 shares of Y", Y owns N2, not X. But an
    after-noun on a NEW LINE that starts a fresh label ("Class B Common
    Stock" in a label-first table) belongs to the next row, never to this
    number."""
    strong_before = strong_after = bare_best = None
    # the window is by phrase END for before-nouns, so a long phrase whose
    # start falls outside num_start-reach still counts
    lo = max(0, num_start - reach - 80)
    hi = min(len(text), num_end + reach)
    for m in _NOUN_CORE.finditer(text, lo, hi):
        after = m.start() >= num_end
        dist = (m.start() - num_end) if after else (num_start - m.end())
        if dist < 0:
            dist = 0
        if dist > reach:
            continue
        if m.group(1).lower() in _BARE_NOUNS:
            if dist <= 40 and (bare_best is None or dist < bare_best[0]):
                bare_best = (dist, m)
        elif after:
            if strong_after is None or dist < strong_after[0]:
                strong_after = (dist, m)
        else:
            if strong_before is None or dist < strong_before[0]:
                strong_before = (dist, m)

    binds = False
    if strong_after:
        gap_text = text[num_end:strong_after[1].start()]
        binds = re.fullmatch(
            r"(?is)\s*(?:shares?\s+of\s+(?:the\s+)?"
            r"(?:registrant|company|issuer)\W{0,3}s\s+|shares?\s+of\s+|of\s+)?",
            gap_text) is not None
        if "\n" in gap_text and not \
                strong_after[1].group(1).lower().startswith("share"):
            binds = False
    if strong_after and binds:
        return strong_after[1]
    if strong_before:
        return strong_before[1]
    if strong_after:
        return strong_after[1]
    return bare_best[1] if bare_best else None


_MAX_DATE = [""]  # filing-date ceiling, set per filing by extract_cover


def _nearest_date(text, anchor_pos, prefer_as_of=True, reach=400):
    """The nearest cover date to a position; 'as of <date>' wins ties.
    Dates after the filing date (bond maturities, meeting dates) and dates
    preceded by 'due' are never a share count's as-of date."""
    lo, hi = max(0, anchor_pos - reach), min(len(text), anchor_pos + reach)
    best = None
    matches = list(_DATE_RE.finditer(text, lo, hi)) + \
        list(_SLASH_DATE_RE.finditer(text, lo, hi))
    for m in matches:
        iso = _parse_date_match(m)
        if not iso:
            continue
        if _MAX_DATE[0] and iso > _MAX_DATE[0]:
            continue                          # "Notes due October 16, 2080"
        if re.search(r"(?i)\bdue\s*$", text[max(0, m.start() - 8):m.start()]):
            continue
        dist = abs(m.start() - anchor_pos)
        as_of = bool(re.search(r"(?i)\b(?:as\s+of|as\s+at|dated|"
                               r"outstanding\s+(?:on|at))\s*$",
                               text[max(0, m.start() - 18):m.start()]))
        rank = (0 if (as_of and prefer_as_of) else 1, dist)
        if best is None or rank < best[0]:
            best = (rank, iso)
    return best[1] if best else ""


# ------------------------------------------------------- candidate scanning

_NUM_SPACED_RE = re.compile(r"(?<![\w.,])(\d{1,3}(?: \d{3})+)(?![\d,])")


def _scan_candidates(text, lo, hi, allow_space_groups=False):
    """Share-count candidates in text[lo:hi]: a number that survives the
    decoy tests and sits near a share-class noun. Returns dicts keyed by
    number position (so overlapping windows dedupe naturally).

    allow_space_groups additionally accepts European space-grouped digits
    ("5 605 850 345") — used on 20-F/40-F covers only, where a fixed anchor
    bounds the window and adjacent table columns can't glue together."""
    out = {}

    def consider(start, end, num_text, scale_word, extra_flags):
        if _is_bare_year(num_text, scale_word):
            return
        # bare decimals are ratios/prices — EXCEPT a genuine fractional count
        # ("935.51 shares outstanding" at parent-owned subs, fractional fund
        # units), recognized below by noun adjacency + an outstanding nearby
        is_decimal = "." in num_text and not scale_word
        if is_decimal and float(num_text.split(".")[0].replace(",", "") or 0) < 100:
            return
        if text[end:end + 1] == "/" or text[max(0, start - 1):start] == "/":
            return                            # component of a 3/20/2025 date
        if text[end:end + 1] == "-" or text[max(0, start - 1):start] == "-":
            return                            # commission file numbers (1-10257)
        if _decoy(text, start, end):
            return
        value = _num_value(num_text, scale_word)
        if value <= 0:
            return
        # footnote markers: a tiny number alone in parentheses
        if value < 1000 and re.search(r"\(\s*$", text[max(0, start - 3):start]) \
                and text[end:end + 2].lstrip().startswith(")"):
            return
        # "outstanding (in thousands): 597,441" or "597,441 shares (in
        # thousands)" — a parenthesized scale phrase before or just after
        scale_flags = []
        if not scale_word:
            sp = re.search(r"(?i)\(\s*in\s+(thousands|millions)\s*\)",
                           text[max(0, start - 130):start]) or \
                 re.match(r"(?i)\s*shares?\s*\(\s*in\s+(thousands|millions)\s*\)",
                          text[end:end + 40])
            if sp:
                value *= 1000 if sp.group(1).lower() == "thousands" else 1000000
                scale_flags.append("SCALE_PHRASE_USED")
        noun = _nearest_noun(text, start, end)
        if noun is None:
            return
        if is_decimal:
            if not (0 <= noun.start() - end <= 25 and
                    re.search(r"(?i)\boutstanding\b",
                              text[max(0, start - 120):end + 120])):
                return
            value = float(num_text.replace(",", ""))
            extra_flags = list(extra_flags) + ["FRACTIONAL_SHARES"]
        if value < 1000 and "," not in num_text and not scale_word:
            # small bare integers need the share noun adjacent (either side):
            # "100 shares" / "common stock was 100"
            gap = (noun.start() - end) if noun.start() >= end else (start - noun.end())
            if not (0 <= gap <= 25):
                return
        label = _expand_label(text, noun.start(), noun.end())
        if re.search(r"(?i)\bvalue\s+per\s+share$|^(?:par\s+)?value$", label):
            return                            # "$0.005 par value per share"
        if re.search(r"(?i)%\s+or\s+more|or\s+more\s+of\s+its", label):
            label = text[noun.start():noun.end()]  # affiliate-clause boilerplate
            label = re.sub(r"\s+", " ", label).strip(" ,.;:")
        # "Class A Preference Shares, Series 24" — the series rides AFTER
        # the noun; without it multi-series preferreds are indistinguishable
        sm = re.match(r"(?i),?\s*(series\s+[a-z0-9]{1,6})\b",
                      text[noun.end():noun.end() + 20])
        if sm and "series" not in label.lower():
            label = label + ", " + sm.group(1)
        share_type, desig = classify_label(label)
        flags = list(scale_flags) + list(extra_flags)
        if scale_word:
            flags.append("SCALE_WORD_USED")
        if value < 1000:
            flags.append("SMALL_NUMBER")
        if value > 5e13:
            flags.append("NUMBER_OUT_OF_RANGE")
        out[start] = {
            "value": value, "label": label, "share_type": share_type,
            "class_designator": desig, "pos": start, "flags": flags,
        }

    # pad the regex region so a number straddling the window edge completes
    # instead of being clipped mid-digits; matches must still START inside
    pad_hi = min(len(text), hi + 30)
    for m in _NUM_RE.finditer(text, lo, pad_hi):
        if m.start() >= hi:
            continue
        consider(m.start(), m.end(), m.group(1), m.group(2), [])
    if allow_space_groups:
        for m in _NUM_SPACED_RE.finditer(text, lo, pad_hi):
            if m.start() < hi and m.start() not in out:
                consider(m.start(), m.end(), m.group(1).replace(" ", ""),
                         None, ["SPACE_GROUPED_NUMBER"])
    return out


def _dedupe_rows(cands):
    """One row per (value, label). A cover may state the same class twice
    (once in the market-value sentence, once as the current count) — the
    occurrence with the LATEST as-of date wins, so a stale duplicate can't
    pin an old date on a current number; ties go to the earliest position."""
    seen = {}
    for c in sorted(cands.values(),
                    key=lambda c: (c.get("as_of") or "", -c["pos"]),
                    reverse=True):
        key = (c["value"], c["label"].lower())
        if key not in seen:
            seen[key] = c
    return sorted(seen.values(), key=lambda c: c["pos"])


_COLON_BOUND_RE = re.compile(
    r"(?is)\bnumber\s+of\s+([^\n:]{3,90}?)\s+(?:issued\s+and\s+)?outstanding\b"
    r"[^:]{0,100}:\s*\$?\s*(\d[\d,]*)")


def _colon_bound_candidates(text, lo, hi):
    """ "Number of Shares of the registrant outstanding as of <date>: N" —
    the count follows a colon at the end of the phrase, too far from its
    noun for proximity rules. The phrase IS the binding."""
    out = {}
    for m in _COLON_BOUND_RE.finditer(text, lo, hi):
        raw = m.group(2).rstrip(",")
        if not raw or "," not in raw and len(raw) > 9:
            continue
        value = int(raw.replace(",", ""))
        if value <= 0:
            continue
        label = re.sub(r"\s+", " ", m.group(1)).strip(" ,.;:")
        share_type, desig = classify_label(label)
        if share_type == "other" and not re.search(
                r"(?i)\bshares?\b|\bstock\b|\bunits?\b", label):
            continue
        num_start = m.start(2)
        out[num_start] = {
            "value": value, "label": label, "share_type": share_type,
            "class_designator": desig, "pos": num_start,
            "flags": ["COLON_BOUND"],
        }
    return out


def _drop_superseded(rows):
    """The cover's market-value line dates its share basis to the prior
    second-quarter close; the real count is 'as of the latest practicable
    date'. Within one class, a dated row strictly older than another dated
    row of that class is the stale basis — drop it."""
    by_class = {}
    for r in rows:
        by_class.setdefault((r["share_type"], r["class_designator"]), []).append(r)
    keep = []
    for grp in by_class.values():
        dates = {r["as_of"] for r in grp if r["as_of"]}
        if len(dates) >= 2:
            latest = max(dates)
            grp = [r for r in grp if not r["as_of"] or r["as_of"] == latest]
        keep.extend(grp)
    return sorted(keep, key=lambda r: r["pos"])


def _drop_weak_total(rows):
    """Covers often print 'N shares outstanding, of which X ... and Y ...'.
    A row equal to the sum of the others is that total, not a class — drop
    it when its label is weak OR duplicates another row's label (the class
    rows carry the information)."""
    if len(rows) < 3:
        return rows
    for i, r in enumerate(rows):
        others = [x for j, x in enumerate(rows) if j != i]
        weak = r["label"].lower() in _BARE_NOUNS or "total" in r["label"].lower() \
            or any(x["label"].lower() == r["label"].lower() for x in others) \
            or (not r["class_designator"] and
                all(x["class_designator"] for x in others))
        if weak and r["value"] == sum(x["value"] for x in others):
            return others
    return rows


# ----------------------------------------------------------------- 10-K path

_PART_I_RE = re.compile(r"(?m)(?:^\s*PART\s+I\b[.:]?\s*$|^\s*PART\s+I\b\s*-)")

_REG_TABLE_HEADER_RE = re.compile(
    r"(?is)number\s+of\s+shares\s+of\s+common\s+stock\s*\n?\s*outstanding"
    r"(?:\s+of\s+the\s+registrants?)?"
    r"|number\s+of\s+shares\s+outstanding\s+of\s+each\s+registrant\W{0,3}s?"
    r"\s+(?:classes\s+of\s+)?common\s+stock")
_PURE_NUM_RE = re.compile(r"\$?\s*(\d{1,3}(?:,\d{3})+|\d+)\s*$")


def _extract_registrant_table(cover):
    """Combined (multi-registrant) 10-Ks print a cover table: one row per
    registrant, columns for the market value and the share count, the count
    in the LAST numeric column (both observed shapes — [MV, count] and
    [MV, older count, current count] — put the current count last). Returns
    rows or None when no such table parses."""
    hm = _REG_TABLE_HEADER_RE.search(cover)
    if not hm:
        return None
    stop = re.search(r"(?i)documents\s+incorporated", cover[hm.end():])
    seg = cover[hm.end():hm.end() + (stop.start() if stop else len(cover))]

    header_dates, rows = [], []
    cur_name, cur_label, nums = None, "", []

    def flush():
        if cur_name and nums:
            name = re.sub(r"\s*\([a-z]\)\s*$", "", cur_name).strip()
            # "Exelon Corporation Common Stock, without par value" — the
            # class label rides on the name line; split it off
            label = cur_label
            nm = re.search(r"(?i)\s+(common\s+stock\b.*)$", name)
            if nm and not label:
                label = nm.group(1)
                name = name[:nm.start()].strip(" ,")
            label = re.sub(r",?\s*(?:\$[\d.]+\s+par\s+value.*|no\s+par\s+"
                           r"value.*|without\s+par\s+value.*|par\s+value.*)$",
                           "", label, flags=re.I).strip(" ,") or "common stock"
            share_type, desig = classify_label(label)
            rows.append({
                "value": nums[-1], "label": label,
                "share_type": share_type, "class_designator": desig,
                "registrant": name, "pos": hm.start(),
                "flags": ["REGISTRANT_TABLE"],
            })

    for line in seg.split("\n"):
        l = line.strip()
        if not l:
            continue
        dm = _DATE_RE.fullmatch(l) or _SLASH_DATE_RE.fullmatch(l)
        if dm:
            iso = _parse_date_match(dm)
            if iso:
                header_dates.append(iso)
            continue
        if l.upper().rstrip(".") in ("NONE", "NA", "N/A", "NOT APPLICABLE"):
            continue
        if l.startswith("("):
            continue                       # par-value notes, footnotes
        if re.match(r"(?i)^(?:registrant|title\s+of\s+each\s+class|shares)\b"
                    r"[^\d]*$", l):
            continue                       # column headers
        nm = _PURE_NUM_RE.fullmatch(l)
        if nm:
            if not l.lstrip().startswith("$"):
                nums.append(int(nm.group(1).replace(",", "")))
            continue
        if _NOUN_CORE.match(l):
            cur_label = l                  # the class-title line of this row
            continue
        if l.endswith(".") and len(l.split()) >= 6:
            continue                       # description sentence ("All of the
                                           # registrant's ... owned by ...")
        if re.search(r"[A-Za-z]{3}", l):
            flush()
            cur_name, cur_label, nums = l, "", []
    flush()

    rows = [r for r in rows if r["value"] > 0]
    if len(rows) < 2:
        return None
    as_of = max(header_dates) if header_dates else \
        _nearest_date(cover, hm.start(), reach=600)
    for r in rows:
        r["as_of"] = as_of
        r["method"] = "10K_REGISTRANT_TABLE"
        if not as_of:
            r["flags"].append("NO_DATE")
    return rows


def _extract_10k(text, multi_registrant=False):
    flags = []
    m = _PART_I_RE.search(text) or re.search(r"(?im)^\s*part\s+i\b", text)
    if m:
        cover = text[:m.start()]
        if len(cover) < 1500:
            flags.append("SHORT_COVER")
        if len(cover) > 15000:
            # some filers put financial statements before the PART I heading;
            # the legally required cover page is always at the very top
            cover = text[:15000]
            flags.append("COVER_CAPPED")
    else:
        # no PART I heading (reduced-format subsidiaries) — end the cover at
        # the next structural marker so body tables can't leak in
        e = re.search(r"(?im)documents\s+incorporated\s+by\s+reference|"
                      r"^\s*table\s+of\s+contents\s*$", text[:15000])
        cover = text[:e.end() + 200] if e else text[:15000]
        flags.append("NO_COVER_MARKERS")

    if multi_registrant:
        table_rows = _extract_registrant_table(cover)
        if table_rows:
            if len(table_rows) > 1:
                flags.append("MULTI_CLASS")
            return table_rows, flags

    cands = {}
    for om in re.finditer(r"(?i)\boutstanding\b", cover):
        lo, hi = max(0, om.start() - 400), min(len(cover), om.end() + 400)
        cands.update(_scan_candidates(cover, lo, hi))
    for pos, c in _colon_bound_candidates(cover, 0, len(cover)).items():
        cands.setdefault(pos, c)
    for c in cands.values():
        c["as_of"] = _nearest_date(cover, c["pos"])
    rows = _drop_weak_total(_dedupe_rows(cands))
    for r in rows:
        r["method"] = "10K_COVER_SCAN"
        if not r["as_of"]:
            # "as of the date of this filing, there were N shares ..."
            if _MAX_DATE[0] and re.search(
                    r"(?i)\bas\s+of\s+the\s+date\s+(?:of|hereof)\s*(?:of\s+)?"
                    r"this\s+(?:filing|report|annual\s+report)",
                    cover[max(0, r["pos"] - 250):r["pos"] + 250]):
                from datetime import date, timedelta
                y, mo, dy = (int(_MAX_DATE[0][:4]), int(_MAX_DATE[0][5:7]),
                             int(_MAX_DATE[0][8:10]))
                r["as_of"] = (date(y, mo, dy) - timedelta(days=14)).isoformat()
                r["flags"].append("DATE_IS_FILING_DATE")
            else:
                r["flags"].append("NO_DATE")
    rows = _drop_superseded(rows)
    if not rows:
        flags.append("NO_MATCH")
        # integral counts only is a documented rule; when a cover shows a
        # fractional count ("935.51 shares" — parent-owned subs), route it
        # to review instead of silently missing it
        if re.search(r"(?i)\b\d[\d,]*\.\d+\s+(?:shares?|units?)\b[^.]{0,120}"
                     r"\boutstanding\b", cover) or \
                re.search(r"(?i)\boutstanding\b[^.]{0,80}\b\d[\d,]*\.\d+\s+"
                          r"(?:shares?|units?)\b", cover):
            flags.append("FRACTIONAL_COUNT_SUSPECTED")
    if len(rows) > 1:
        flags.append("MULTI_CLASS")
    return rows, flags


# ------------------------------------------------------------ 20-F/40-F path

_FPI_ANCHOR_RE = re.compile(
    r"(?is)number\s+of\s+(?:issued\s+and\s+)?outstanding\s+shares\s+of\s+each"
    r"\s+of\s+the\s+(?:issuer|registrant|company)\W{0,3}s\s+classes\s+of")
_FPI_ANCHOR_FALLBACK_RE = re.compile(
    r"(?is)number\s+of\s+(?:issued\s+and\s+)?outstanding\s+(?:shares|share\s+capital)")
_NEXT_ITEM_RE = re.compile(r"(?i)indicate\s+by\s+check\s+mark")


def _drop_non_period_fpi(rows, period_iso):
    """20-F/40-F counts are required as of the period close. When the cover
    also prints a filing-date count of the same class, the period-close row
    is the regulatory one — drop the other (mirror image of the 10-K
    latest-practicable-date rule)."""
    if not period_iso:
        return rows
    by_class = {}
    for r in rows:
        by_class.setdefault((r["share_type"], r["class_designator"]), []).append(r)
    keep = []
    for grp in by_class.values():
        dates = {r["as_of"] for r in grp if r["as_of"]}
        if len(dates) >= 2 and period_iso in dates:
            grp = [r for r in grp if not r["as_of"] or r["as_of"] == period_iso]
        keep.extend(grp)
    return sorted(keep, key=lambda r: r["pos"])


def _extract_fpi(text, period_of_report):
    flags = []
    period_iso = ""
    if re.fullmatch(r"\d{8}", period_of_report or ""):
        period_iso = "%s-%s-%s" % (period_of_report[:4], period_of_report[4:6],
                                   period_of_report[6:8])
    head = text[:60000]
    am = _FPI_ANCHOR_RE.search(head)
    method = "FPI_ANCHOR"
    if not am:
        am = _FPI_ANCHOR_FALLBACK_RE.search(head)
        method = "FPI_ANCHOR_LOOSE"
        if am:
            flags.append("LOOSE_ANCHOR")
    if not am:
        # no regulatory anchor — some covers state the count free-form
        # ("On September 30, 2024, the issuer had N shares outstanding.")
        flags.append("ANCHOR_NOT_FOUND")
        cands = {}
        scan_hi = min(len(head), 20000)
        for om in re.finditer(r"(?i)\boutstanding\b", head[:scan_hi]):
            lo, hi = max(0, om.start() - 400), min(scan_hi, om.end() + 400)
            cands.update(_scan_candidates(head, lo, hi, allow_space_groups=True))
        for pos, c in _colon_bound_candidates(head, 0, scan_hi).items():
            cands.setdefault(pos, c)
        rows = _drop_weak_total(_dedupe_rows(cands))
        rows = [r for r in rows if not re.search(
            r"(?i)\b(treasury|warrant|option)\b", r["label"])]
        for r in rows:
            r["as_of"] = _nearest_date(head, r["pos"])
            r["method"] = "FPI_FREEFORM"
            r["flags"].append("FPI_FREEFORM")
            if not r["as_of"]:
                r["flags"].append("NO_DATE")
        rows = _drop_non_period_fpi(rows, period_iso)
        if not rows:
            flags.append("NO_MATCH")
        if len(rows) > 1:
            flags.append("MULTI_CLASS")
        return rows, flags

    win_lo = am.end()
    nm = _NEXT_ITEM_RE.search(head, win_lo)
    win_hi = min(nm.start() if nm else win_lo + 3000, win_lo + 3000)

    cands = _scan_candidates(head, win_lo, win_hi, allow_space_groups=True)
    rows = _drop_weak_total(_dedupe_rows(cands))

    # numbers whose label betrays a non-class line item
    rows = [r for r in rows if not re.search(
        r"(?i)\b(treasury|warrant|option)\b", r["label"])]

    as_of = _nearest_date(head, am.start(), reach=600)
    date_flag = []
    if not as_of and re.fullmatch(r"\d{8}", period_of_report or ""):
        as_of = "%s-%s-%s" % (period_of_report[:4], period_of_report[4:6],
                              period_of_report[6:8])
        date_flag = ["DATE_FROM_PERIOD"]
    for r in rows:
        r["as_of"] = as_of
        r["method"] = method
        r["flags"].extend(date_flag)
        if not as_of:
            r["flags"].append("NO_DATE")
    if not rows:
        flags.append("NO_MATCH")
    if len(rows) > 1:
        flags.append("MULTI_CLASS")
    return rows, flags


# -------------------------------------------------------------- entry point

def _repair_text(text):
    """Two glue repairs observed across many filings:
    - sentence-glued numbers: "...annual report.281,382,906 Class B..." —
      a period directly between a letter and a digit gets a space;
    - footnote-glued counts: "12,000,0002" (footnote 2 fused onto
      12,000,000) — a 1-2 digit tail that breaks the comma grouping is
      stripped."""
    text = re.sub(r"(?<=[A-Za-z])\.(?=\d)", ". ", text)
    # comma-glued dates ("March 28,2025") must be spaced BEFORE the footnote
    # stripper below, which would otherwise read 28,2025 as 28,202+footnote
    text = re.sub(r"(?i)((?:january|february|march|april|may|june|july|august|"
                  r"september|october|november|december)\.?\s+\d{1,2}),(\d{4})",
                  r"\1, \2", text)
    text = re.sub(r"(\d{1,3}(?:,\d{3})+)(\d{1,2})(?=[^\d,]|$)", r"\1", text)
    # "6,903,056shares" — a word glued straight onto a grouped number
    text = re.sub(r"(\d{1,3}(?:,\d{3})+)(?=[A-Za-z])", r"\1 ", text)
    return text


def extract_cover(text, form, period_of_report="", multi_registrant=False,
                  filed_date=""):
    """(rows, filing_flags) for one filing's clean text. Each row:
    value, label, share_type, class_designator, as_of, method, flags, and
    registrant (non-empty only for multi-registrant cover tables).
    filed_date (YYYY-MM-DD) caps as-of dates: a count is never as of a
    future date."""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", filed_date or ""):
        from datetime import date, timedelta
        y, mo, dy = int(filed_date[:4]), int(filed_date[5:7]), int(filed_date[8:10])
        _MAX_DATE[0] = (date(y, mo, dy) + timedelta(days=14)).isoformat()
    else:
        _MAX_DATE[0] = ""
    text = _repair_text(text)
    if form == "10-K":
        rows, flags = _extract_10k(text, multi_registrant)
    else:
        rows, flags = _extract_fpi(text, period_of_report)
    for r in rows:
        r.setdefault("registrant", "")
    return rows, flags
