# run_dashboard.py
#
# Spin up the registrant dashboard. Double-click run_dashboard.bat (Windows) or
# run this file in Spyder / from a terminal. It:
#   1. scans the data folder for year CSVs (any *.csv, e.g. reg_count_final_2025.csv)
#   2. writes data/manifest.json (the file list + column_config.py)
#   3. starts a tiny localhost web server (needed: a browser opened straight from
#      the filesystem is not allowed to read sibling files)
#   4. opens your browser with every year already loaded
#
# Nothing is installed and nothing leaves your machine. Stop it with Ctrl-C.

# ---- EDIT THIS --------------------------------------------------------------
DATA_FOLDER = "data"   # folder next to this script holding the year CSV files
PORT = 8000            # localhost port; auto-bumps to the next free one if busy
OPEN_BROWSER = True    # open the browser automatically
# -----------------------------------------------------------------------------

import os
import re
import json
import socket
import datetime
import functools
import webbrowser
import http.server
import socketserver

try:
    from column_config import get_config
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(
        "Could not import column_config.py (it must sit next to this file)."
    ) from exc

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, DATA_FOLDER)
YEAR_RE = re.compile(r"(19|20)\d{2}")


def discover_vintages():
    """Every *.csv in the data folder, newest year first."""
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    vintages = []
    for name in sorted(os.listdir(DATA_DIR)):
        if not name.lower().endswith(".csv"):
            continue
        m = YEAR_RE.search(name)
        year = int(m.group(0)) if m else None
        label = str(year) if year else os.path.splitext(name)[0]
        vintages.append({
            "file": DATA_FOLDER + "/" + name,  # URL path, relative to the server root
            "label": label,
            "year": year,
        })
    vintages.sort(key=lambda v: (v["year"] is None, -(v["year"] or 0), v["file"]))
    return vintages


def write_manifest(vintages):
    manifest = {
        "generated": datetime.datetime.now().isoformat(timespec="seconds"),
        "vintages": vintages,
        "config": get_config(),
    }
    with open(os.path.join(DATA_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def free_port(start):
    for port in range(start, start + 50):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) != 0:  # nothing listening
                return port
    return start


def main():
    vintages = discover_vintages()
    write_manifest(vintages)

    print("registrant dashboard")
    print("-" * 48)
    if vintages:
        for v in vintages:
            print(f"  loaded  {v['label']:<10}  {v['file']}")
    else:
        print("  (no CSVs found in ./%s — drop your year files there)" % DATA_FOLDER)
    print("-" * 48)

    port = free_port(PORT)
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=HERE)
    # threaded so the browser can pull several CSVs at once without blocking
    socketserver.TCPServer.allow_reuse_address = True
    httpd = socketserver.ThreadingTCPServer(("127.0.0.1", port), handler)
    url = f"http://localhost:{port}/dashboard.html"
    print(f"  serving at {url}")
    print("  (leave this window open; press Ctrl-C here to stop)")

    if OPEN_BROWSER:
        webbrowser.open(url)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n  stopped.")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
