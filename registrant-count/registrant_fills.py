"""Deterministic 100%-fill layer for the registrant-count status columns.

Every status flag — wksi, shell, src, egc, afs, and the 12(b)/12(g)/15(d)
registration choice — is resolved to a concrete value (never blank) together
with a recorded METHOD, in this strict priority order:

  1. AGENT_READ     A blind human-style read of the actual cover page
                    (`registrant_overrides.py`). Used where the form's own dei
                    XBRL tag was absent AND the text scrape missed it, OR where
                    an as-filed value was an extractor artifact that the cover
                    read corrects. A read entry always wins — it is the closest
                    thing to ground truth we have.
  2. AS_FILED       The filing's own dei XBRL tag, or a cover-text scrape of the
                    printed checkbox (whatever `cover_facts_<year>.csv` holds).
                    This is the authoritative as-filed disclosure.
  3. DEFINITIONAL   A status the filing structurally cannot carry:
                      * ABS issuers (SIC 6189): asset-backed trusts have no
                        common equity -> wksi/shell/src/egc = 0, afs = NAF.
                      * Foreign private issuers (Form 20-F / 40-F) are not
                        eligible to be smaller reporting companies -> src = 0.
                      * Form 40-F (MJDS) covers carry no WKSI / shell /
                        accelerated-filer box -> wksi/shell = 0; afs falls to
                        the size baseline below.
  4. SIZE_HEURISTIC Where a value is genuinely undisclosed, fall to the
                    rules-defined size baseline from the public-float census:
                      * afs: float >= $700M -> LAF; >= $75M -> AF; else NAF.
                      * src: float <  $250M -> 1;   >= $700M -> 0.
                    (A first annual report is Non-Accelerated by rule regardless
                    of float, so the size upgrade is only a fallback for filers
                    the cover read did not resolve; the read decides the
                    new-issuer cases.)
  5. DEFAULT        The residual baseline — the user's rule, mirroring §15(d) as
                    the default registration: afs = NAF, and wksi/shell/src/egc
                    = 0 (a filer is presumed NOT to hold a special status absent
                    evidence). NAF is the default unless there is evidence of AF
                    or LAF.

A final COHERENCE guard prevents a heuristic/default fill from ever creating a
logically impossible pair (e.g. a Large Accelerated Filer that is also a Smaller
Reporting Company): a filled src is forced to 0 when afs resolves to LAF. As-
filed disclosures are never altered by the guard — if a filer literally checked
both boxes we report what they filed, and the build logs it.
"""

# Accelerated-filer / SRC public-float thresholds (Securities Exchange Act
# Rule 12b-2), in dollars.
LAF_FLOAT = 700_000_000   # Large Accelerated Filer:  worldwide common-equity float >= $700M
AF_FLOAT = 75_000_000     # Accelerated Filer:        float >= $75M
SRC_FLOAT = 250_000_000   # Smaller Reporting Company: float < $250M qualifies on the float test

ABS_SIC = "6189"
FPI_FORMS = ("20-F", "40-F")

_AFS_VALS = ("LAF", "AF", "NAF")
_BOOL_VALS = ("0", "1")


def _afs(raw, form, is_abs, override, float_val):
    """Resolve afs -> (LAF|AF|NAF, method). Never blank."""
    if override and override.get("afs") in _AFS_VALS:
        return override["afs"], "AGENT_READ"
    if raw in _AFS_VALS:
        return raw, "AS_FILED"
    if is_abs:
        return "NAF", "ABS_DEFINITIONAL"
    if form == "40-F":
        # 40-F covers have no accelerated-filer box and the census carries no
        # public float for MJDS filers -> NAF by the default rule.
        return "NAF", "FORM40F_DEFAULT"
    if float_val is not None:
        if float_val >= LAF_FLOAT:
            return "LAF", "SIZE_HEURISTIC"
        if float_val >= AF_FLOAT:
            return "AF", "SIZE_HEURISTIC"
        return "NAF", "SIZE_HEURISTIC"
    return "NAF", "DEFAULT"


def _bool(raw, key, form, is_abs, override, float_val):
    """Resolve one 0/1 flag (wksi|shell|src|egc) -> (value, method). Never blank."""
    if override and override.get(key) in _BOOL_VALS:
        return override[key], "AGENT_READ"
    if raw in _BOOL_VALS:
        return raw, "AS_FILED"
    if is_abs:
        return "0", "ABS_DEFINITIONAL"
    if key == "src" and form in FPI_FORMS:
        return "0", "FPI_NOT_SRC"
    if key in ("wksi", "shell") and form == "40-F":
        return "0", "FORM40F_DEFAULT"
    if key == "src" and float_val is not None:
        if float_val < SRC_FLOAT:
            return "1", "SIZE_HEURISTIC"
        if float_val >= LAF_FLOAT:
            return "0", "SIZE_HEURISTIC"
        return "0", "SIZE_DEFAULT"   # $250M-$700M depends on revenue; not-small is the safe call
    return "0", "DEFAULT"


def resolve_flags(raw, form, is_abs, float_val, override):
    """Resolve all five status flags for one filing to concrete, non-blank
    values plus a per-flag method dict.

    raw      : dict from cover_facts (keys wksi/shell/src/egc/afs; "" if absent).
    form     : "10-K" | "20-F" | "40-F".
    is_abs   : True if the filing is an ABS issuer (SIC 6189).
    float_val: public float in dollars (float) or None if unknown.
    override : dict of cover-read values for this accession (or None).

    Returns (values, methods) — each a dict over wksi/shell/src/egc/afs.
    """
    raw = raw or {}
    values, methods = {}, {}
    for key in ("wksi", "shell", "src", "egc"):
        values[key], methods[key] = _bool(raw.get(key, ""), key, form, is_abs,
                                           override, float_val)
    values["afs"], methods["afs"] = _afs(raw.get("afs", ""), form, is_abs,
                                          override, float_val)

    # Coherence guard: a FILLED src may not coexist with a Large Accelerated
    # Filer (LAF float >= $700M cannot qualify as a Smaller Reporting Company).
    # Only adjust heuristic/default fills — never an as-filed or read disclosure.
    if values["afs"] == "LAF" and values["src"] == "1" \
            and methods["src"] in ("SIZE_HEURISTIC", "SIZE_DEFAULT", "DEFAULT"):
        values["src"], methods["src"] = "0", "COHERENCE_LAF"

    return values, methods


def resolve_registration(has_12b, has_12g, override):
    """12(b) > 12(g) > 15(d). A cover read can correct the scrape/XBRL.
    Returns (sec_12b, sec_12g, sec_15d, choice, method)."""
    if override and override.get("reg") in ("12b", "12g", "15d"):
        choice, method = override["reg"], "AGENT_READ"
    elif has_12b:
        choice, method = "12b", "AS_FILED"
    elif has_12g:
        choice, method = "12g", "AS_FILED"
    else:
        choice, method = "15d", "DEFAULT_15D"
    return (("1" if choice == "12b" else "0"),
            ("1" if choice == "12g" else "0"),
            ("1" if choice == "15d" else "0"),
            choice, method)
