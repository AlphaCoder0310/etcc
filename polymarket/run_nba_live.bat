@echo off
rem ============================================================================
rem  deploy_nba_live.bat  --  deploy + (re)start the Polymarket NBA Live Monitor
rem
rem  Put this bat in the same folder as nba_live_web.py, then double-click, or:
rem     deploy_nba_live.bat            deploy + restart (live Polymarket feeds)
rem     deploy_nba_live.bat mock       deploy + restart with the offline simulator
rem     deploy_nba_live.bat stop       stop the running instance only
rem
rem  Steps: find Python (conda env) -> syntax check -> backup + copy to DEST
rem         -> websocket-client check -> stop old instance on PORT -> start
rem         -> wait for /api/health -> print feed status -> open browser
rem ============================================================================
setlocal EnableExtensions EnableDelayedExpansion
title NBA Live Monitor - deploy

rem ----------------------------- CONFIG ---------------------------------------
set "APP=nba_live_web.py"
rem Deploy folder. If its drive is missing on this PC, the bat runs from its own folder.
set "DEST=M:\CB\BAU\nba_live"
set "CONDA_ENV=cb_runs"
set "PORT=59955"
rem 127.0.0.1 = this PC only, 0.0.0.0 = share with the desk on the LAN
set "HOST=127.0.0.1"
rem Corporate TLS proxy CA bundle (.pem). Leave empty if not needed.
set "CA_BUNDLE="
rem Keep this many backups of the previous version
set "KEEP_BACKUPS=10"
rem ----------------------------------------------------------------------------

set "SRC=%~dp0"
set "MODE=%~1"
set "EXTRA="
if /i "%MODE%"=="mock" set "EXTRA=--mock"
if defined CA_BUNDLE set "NBA_CA_BUNDLE=%CA_BUNDLE%"

echo.
echo  NBA Live Monitor deploy   mode=[%MODE%]   port=%PORT%   dest=%DEST%
echo  ---------------------------------------------------------------------

if /i "%MODE%"=="stop" (
    call :stop_port
    goto :done
)
if not "%MODE%"=="" if /i not "%MODE%"=="mock" (
    echo [FAIL] unknown option "%MODE%"  -- use: ^(none^) ^| mock ^| stop
    goto :fail
)

rem ---- 1. Python --------------------------------------------------------------
call :find_python || goto :fail
echo [OK] python  : %PY%

rem ---- 2. Syntax check BEFORE touching the deployed copy ---------------------
if not exist "%SRC%%APP%" (
    echo [FAIL] %APP% not found next to this bat: %SRC%
    goto :fail
)
"%PY%" -m py_compile "%SRC%%APP%"
if errorlevel 1 (
    echo [FAIL] syntax check failed - nothing deployed
    goto :fail
)
echo [OK] syntax  : %APP%

rem ---- 3. Backup + copy -------------------------------------------------------
for %%A in ("%SRC%.") do set "SRC_N=%%~fA"
rem DEST empty, or its drive not mapped on this PC (e.g. no M: at home) -> run in place
if not defined DEST set "DEST=%SRC_N%"
for %%A in ("%DEST%") do set "DEST_DRV=%%~dA"
if not exist "%DEST_DRV%\" (
    echo [WARN] drive %DEST_DRV% not available on this PC - running in place from %SRC_N%
    set "DEST=%SRC_N%"
)
for %%A in ("%DEST%") do set "DEST_N=%%~fA"
if /i "%SRC_N%"=="%DEST_N%" (
    echo [OK] deploy  : running in place ^(source folder = DEST^)
) else (
    if not exist "%DEST%" mkdir "%DEST%"
    if not exist "%DEST%" (
        echo [FAIL] cannot create %DEST%
        goto :fail
    )
    if exist "%DEST%\%APP%" (
        if not exist "%DEST%\backup" mkdir "%DEST%\backup"
        call :timestamp
        copy /y "%DEST%\%APP%" "%DEST%\backup\!TS!_%APP%" >nul
        echo [OK] backup  : backup\!TS!_%APP%
        call :prune_backups
    )
    copy /y "%SRC%%APP%" "%DEST%\%APP%" >nul
    if errorlevel 1 (
        echo [FAIL] copy to %DEST% failed ^(file locked / no access?^)
        goto :fail
    )
    echo [OK] deploy  : %DEST%\%APP%
)

rem ---- 4. Optional dependency (live scores) -----------------------------------
"%PY%" -c "import websocket" >nul 2>&1
if errorlevel 1 (
    echo [..] installing websocket-client ^(live score feed^) ...
    "%PY%" -m pip install --quiet websocket-client
    "%PY%" -c "import websocket" >nul 2>&1
    if errorlevel 1 (
        echo [WARN] websocket-client not installed - app runs, but without live score/clock
    ) else (
        echo [OK] websocket-client installed
    )
) else (
    echo [OK] deps    : websocket-client present
)

rem ---- 5. Stop old instance ---------------------------------------------------
call :stop_port
call :port_busy
if not errorlevel 1 (
    echo [FAIL] port %PORT% still in use - close that program or change PORT
    goto :fail
)

rem ---- 6. Start (own console window so errors stay visible) ------------------
start "NBA Live :%PORT%" /D "%DEST_N%" cmd /k ""%PY%" "%APP%" --port %PORT% --host %HOST% %EXTRA%"

rem ---- 7. Wait for health (up to 20s) -----------------------------------------
set /a N=0
:wait_up
set /a N+=1
"%PY%" -c "import urllib.request as u; u.build_opener(u.ProxyHandler({})).open('http://127.0.0.1:%PORT%/api/health', timeout=2)" >nul 2>&1
if not errorlevel 1 goto :is_up
if !N! GEQ 20 (
    echo [FAIL] app did not come up in 20s - check the "NBA Live :%PORT%" window
    goto :fail
)
timeout /t 1 /nobreak >nul
goto :wait_up

:is_up
echo [OK] running : http://localhost:%PORT%   ^(up after !N!s^)
echo [..] checking Polymarket feeds ...
"%PY%" -c "import time,json,urllib.request as u; o=u.build_opener(u.ProxyHandler({})); time.sleep(8); h=json.load(o.open('http://127.0.0.1:%PORT%/api/health', timeout=3)); print('     source :', h.get('source')); print('     GAMMA  :', 'OK  (%%s games listed)' %% h.get('gamma_n') if h.get('gamma_ok') else 'ERR  ' + str(h.get('gamma_err'))); print('     CLOB   :', 'OK' if h.get('clob_ok') else ('idle (no live game yet)' if not h.get('clob_err') else 'ERR  ' + str(h.get('clob_err')))); print('     SCORES :', h.get('ws_state'), h.get('ws_err') or '')"

start "" "http://localhost:%PORT%"
goto :done


rem ============================================================================
rem  subroutines
rem ============================================================================
:find_python
set "PY="
set "ENVDIR="
rem a) conda env under the usual install roots
set "CROOT="
if defined CONDA_EXE for %%C in ("%CONDA_EXE%") do set "CROOT=%%~dpC.."
for %%R in ("%CROOT%" "%USERPROFILE%\anaconda3" "%USERPROFILE%\miniconda3" "%LOCALAPPDATA%\anaconda3" "%LOCALAPPDATA%\miniconda3" "C:\ProgramData\Anaconda3" "C:\ProgramData\miniconda3" "C:\Anaconda3") do (
    if not defined ENVDIR if not "%%~R"=="" if exist "%%~R\envs\%CONDA_ENV%\python.exe" set "ENVDIR=%%~fR\envs\%CONDA_ENV%"
)
rem b) user env folder (conda create -p / .conda\envs)
if not defined ENVDIR if exist "%USERPROFILE%\.conda\envs\%CONDA_ENV%\python.exe" set "ENVDIR=%USERPROFILE%\.conda\envs\%CONDA_ENV%"
if defined ENVDIR (
    rem conda python.exe needs Library\bin on PATH for ssl DLLs when not activated
    set "PATH=!ENVDIR!;!ENVDIR!\Library\bin;!ENVDIR!\Scripts;!PATH!"
    set "PY=!ENVDIR!\python.exe"
)
rem c) fallback: python on PATH (skip the Microsoft Store stub)
if not defined PY (
    for /f "delims=" %%P in ('where python 2^>nul') do (
        if not defined PY (
            echo %%P| findstr /i "WindowsApps" >nul
            if errorlevel 1 set "PY=%%P"
        )
    )
    if defined PY echo [WARN] conda env "%CONDA_ENV%" not found - using !PY!
)
if not defined PY (
    echo [FAIL] no Python found ^(conda env "%CONDA_ENV%" or python on PATH^)
    exit /b 1
)
"%PY%" -c "import sys, ssl, json, http.server; sys.exit(0 if sys.version_info >= (3, 9) else 1)" >nul 2>&1
if errorlevel 1 (
    echo [FAIL] %PY% is not Python 3.9+ or cannot import ssl
    exit /b 1
)
exit /b 0

:stop_port
set "FOUND="
for /f "tokens=5" %%P in ('netstat -ano -p tcp ^| findstr /r /c:":%PORT% .*LISTENING"') do (
    if not "%%P"=="0" (
        set "FOUND=1"
        set "PNAME="
        for /f "tokens=1" %%N in ('tasklist /fi "PID eq %%P" /nh /fo table 2^>nul') do set "PNAME=%%N"
        rem only kill our own kind of process; never an unrelated app that grabbed the port
        if /i "!PNAME!"=="python.exe" (
            taskkill /PID %%P /F >nul 2>&1
            if errorlevel 1 (
                echo [WARN] could not stop PID %%P ^(!PNAME!^) on port %PORT%
            ) else (
                echo [OK] stopped : PID %%P ^(!PNAME!^) on port %PORT%
            )
        ) else if /i "!PNAME!"=="INFO:" (
            rem already gone - duplicate netstat line for the same PID
        ) else (
            echo [WARN] port %PORT% is held by PID %%P ^(!PNAME!^) - not a python process, left alone
        )
    )
)
if not defined FOUND echo [OK] stop    : nothing running on port %PORT%
if defined FOUND timeout /t 2 /nobreak >nul
exit /b 0

:port_busy
rem errorlevel 0 = busy, 1 = free
netstat -ano -p tcp | findstr /r /c:":%PORT% .*LISTENING" >nul
exit /b %errorlevel%

:timestamp
for /f %%T in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%T"
if not defined TS set "TS=%RANDOM%"
exit /b 0

:prune_backups
set /a CNT=0
for /f "delims=" %%F in ('dir /b /o-d "%DEST%\backup\*_%APP%" 2^>nul') do (
    set /a CNT+=1
    if !CNT! GTR %KEEP_BACKUPS% del /q "%DEST%\backup\%%F" >nul 2>&1
)
exit /b 0

:fail
echo.
echo  DEPLOY FAILED
pause
endlocal
exit /b 1

:done
echo.
echo  done.
timeout /t 8 >nul
endlocal
exit /b 0