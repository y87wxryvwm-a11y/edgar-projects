@echo off
REM Double-click this to start the registrant dashboard on Windows.
REM It runs run_dashboard.py with whichever Python is installed and opens
REM your browser. Leave the window open; close it (or Ctrl-C) to stop.
cd /d "%~dp0"
where py >nul 2>nul && (py run_dashboard.py) || (python run_dashboard.py)
echo.
echo The dashboard server has stopped.
pause
