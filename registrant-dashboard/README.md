# registrant-dashboard

An offline dashboard over the registrant-count CSVs. Drop your year files in
`data/`, double-click `run_dashboard.bat`, and the browser opens with every year
loaded — filter, chart, cross-tab, compare across years, and copy or export the
result. Nothing is installed; nothing leaves the machine.

The dashboard reads whatever columns the CSV has — it is not tied to a fixed
column set, so the same build works as the dataset grows new columns.

## Open it

1. Put the year CSVs in `data/`. Any name works as long as it contains the year,
   e.g. `reg_count_final_2025.csv`, `reg_count_final_2024.csv`, …
2. **Double-click `run_dashboard.bat`** (Windows). On macOS/Linux or from Spyder,
   run `run_dashboard.py` instead.
3. The browser opens at `http://localhost:8000/dashboard.html` with all years
   loaded. Leave the small console window open; close it (or press Ctrl-C) to stop.

A localhost server runs while the dashboard is open. It has to: a page opened
straight off the filesystem is forbidden by browsers from reading the sibling
CSVs, so the launcher serves the folder locally. It uses the Python already on
the machine — there is nothing to install and the server is reachable only from
this computer.

> If you open `dashboard.html` directly (without the launcher), it still works —
> it just asks you to drag the CSVs in, since it can't auto-load them.

## Update (new year, or new columns)

- **New year:** drop the new `*.csv` in `data/` and relaunch. It appears
  automatically and is available in Compare.
- **New columns:** they show up automatically, auto-classified (category / number
  / date / id / url). To give a column a nicer label, a fixed value order, a
  definition, or to force how it's treated, add an entry in **`column_config.py`**
  and relaunch. That file is the only place to edit; everything not listed there
  is inferred.

## What's in it

- **Overview** — live count + percent for every category, and the standard
  breakdown charts. Reacts to the filters instantly.
- **Explore** — the full table: sort, filter, show/hide columns, with a running
  count of the current selection. **Download filtered CSV** writes exactly the
  rows and columns in view.
- **Build** — pick a row (and optional column) dimension to get a cross-tab and a
  chart; for numeric columns choose sum / average / min / max / median. **Copy**
  puts the table on the clipboard so it pastes cleanly into both Word and Excel.
- **Compare** — one breakdown across every loaded year, with the change.
- **Reference** — what each column means and how it's computed, plus a live
  profile (distinct values, blanks, top values).

The **filter bar** is global: turning off a value (e.g. ABS) removes those rows
from the table, every chart, and every count at once.

## Files

| File | What it is | Edit it? |
|---|---|---|
| `dashboard.html` | the whole app (self-contained; no internet, no dependencies) | no |
| `run_dashboard.py` | the launcher (data folder + port at the top) | only the EDIT block |
| `run_dashboard.bat` | Windows double-click entry | no |
| `column_config.py` | per-column treatment, labels, and definitions | **yes — this is the knob** |
| `data/` | drop your year CSVs here (not committed) | — |

## Build / verify (maintainers)

`dashboard.html` is hand-written and self-contained — there is no build step to
produce it; it ships as-is. The front end is verified with the `frontend-verify`
skill, which drives the real UI in a headless browser (every control clicked and
asserted against ground truth recomputed from the CSV) and reviews screenshots of
every view for visual polish, looping until both pass. That tooling installs
Playwright locally and is never shipped with the dashboard.
