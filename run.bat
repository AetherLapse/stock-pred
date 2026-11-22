@echo off
REM run.bat - set up once, then run a CAS session. Args pass straight through.
REM
REM   run.bat                 live session (dashboard opens in the browser)
REM   run.bat --test 120      2-minute live check during market hours
REM   run.bat --check         offline self-test only, no credentials needed
REM   run.bat --find "NIFTY 50"
REM
REM Double-clicking it with no arguments runs the live session and pauses at the
REM end so the window does not vanish.
setlocal enabledelayedexpansion
cd /d "%~dp0"

set "PY="
where py >nul 2>&1 && set "PY=py -3"
if not defined PY (
  where python >nul 2>&1 && set "PY=python"
)
if not defined PY (
  echo Python was not found. Install Python 3.10+ from python.org and tick
  echo "Add python.exe to PATH", then run this file again.
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo creating the virtual environment ^(once^)
  %PY% -m venv .venv || (echo could not create .venv & pause & exit /b 1)
)

REM The stamp stores a hash of requirements.txt, so adding a dependency re-installs
REM automatically instead of leaving an old venv in place.
set "REQHASH="
for /f "skip=1 delims=" %%H in ('certutil -hashfile requirements.txt MD5') do (
  if not defined REQHASH set "REQHASH=%%H"
)
set "REQHASH=%REQHASH: =%"
set "OLDHASH="
if exist ".venv\.deps-ok" set /p OLDHASH=<".venv\.deps-ok"

if not "%REQHASH%"=="%OLDHASH%" (
  echo installing dependencies
  .venv\Scripts\python.exe -m pip install --quiet --upgrade pip
  .venv\Scripts\python.exe -m pip install --quiet -r requirements.txt || (
    echo dependency install failed & pause & exit /b 1
  )
  > ".venv\.deps-ok" echo %REQHASH%
)

if /I "%~1"=="--check" (
  echo offline self-test ^(no credentials needed^)
  .venv\Scripts\python.exe verify.py
  if errorlevel 1 (pause & exit /b 1)
  .venv\Scripts\python.exe stress_test.py
  echo.
  echo Ctrl-C behaviour ^(starts its own stub feed^)
  .venv\Scripts\python.exe test_ctrlc.py
  pause
  exit /b 0
)

if not exist ".env" (
  copy /y ".env.example" ".env" >nul
  echo.
  echo Created .env from .env.example.
  echo Open it and fill in MSTOCK_API_KEY plus one of
  echo MSTOCK_TOTP_SECRET / MSTOCK_TOTP / MSTOCK_ACCESS_TOKEN,
  echo then run this file again.
  echo.
  pause
  exit /b 1
)

findstr /R /C:"^[ ]*MSTOCK_API_KEY=." .env >nul || (
  echo MSTOCK_API_KEY is empty in .env ^(trade.mstock.com -^> Trading APIs^)
  pause
  exit /b 1
)
findstr /R /C:"^[ ]*MSTOCK_TOTP_SECRET=." /C:"^[ ]*MSTOCK_TOTP=." /C:"^[ ]*MSTOCK_ACCESS_TOKEN=." .env >nul || (
  echo No session credential in .env: set MSTOCK_TOTP_SECRET ^(best^),
  echo MSTOCK_TOTP, or MSTOCK_ACCESS_TOKEN.
  pause
  exit /b 1
)

echo starting the session
.venv\Scripts\python.exe cas_logger.py %*
if errorlevel 1 (
  echo.
  echo the session exited with an error - the log above says why
  pause
)
endlocal
