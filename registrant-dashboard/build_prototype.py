# build_prototype.py
#
# Bakes the data folder + column_config into a SINGLE self-contained HTML file
# that runs by double-clicking it — no Python, no server, no internet. Use this
# to hand someone a one-click prototype. Re-run it whenever the data changes.
#
# Output: Registrant_Count_Dashboard_prototype.html (next to this script).

# ---- EDIT THIS --------------------------------------------------------------
DATA_FOLDER = "data"
SOURCE_HTML = "dashboard.html"
OUTPUT_HTML = "Registrant_Count_Dashboard_prototype.html"
# -----------------------------------------------------------------------------

import os
import re
import json

from column_config import get_config

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, DATA_FOLDER)
YEAR_RE = re.compile(r"(19|20)\d{2}")

html = open(os.path.join(HERE, SOURCE_HTML), encoding="utf-8").read()

# config block (escape < so it can never break out of the script tag)
cfg = json.dumps(get_config(), separators=(",", ":")).replace("<", "\\u003c")
blocks = ['<script type="application/json" id="embedded-config">%s</script>' % cfg]

# one raw-CSV block per year file, newest first
files = sorted((f for f in os.listdir(DATA) if f.lower().endswith(".csv")), reverse=True)
for f in files:
    m = YEAR_RE.search(f)
    label = m.group(0) if m else os.path.splitext(f)[0]
    text = open(os.path.join(DATA, f), encoding="utf-8").read()
    assert "</script" not in text.lower(), "%s contains </script — cannot embed as-is" % f
    blocks.append('<script type="application/csv" data-label="%s">\n%s</script>' % (label, text))

embed = "\n".join(blocks) + "\n"
marker = '<script>\n"use strict";'
assert html.count(marker) == 1, "could not find the unique script marker in %s" % SOURCE_HTML
html = html.replace(marker, embed + marker, 1)

out = os.path.join(HERE, OUTPUT_HTML)
with open(out, "w", encoding="utf-8") as fh:
    fh.write(html)

print("wrote %s  (%.2f MB)" % (OUTPUT_HTML, len(html) / 1e6))
print("baked vintages:", files)
