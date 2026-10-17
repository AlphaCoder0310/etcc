@echo off
rem ============================================================================
rem  sync_to_github.bat  --  copy the polymarket project files to GitHub
rem
rem     repo   : https://github.com/AlphaCoder0310/etcc      (branch main)
rem     folder : polymarket/
rem
rem  Double-click it. Needs git on PATH and a GitHub login (Git Credential
rem  Manager opens a browser prompt the first time, then remembers it).
rem
rem  Copies : nba_live_web.py   run_nba_live.bat   nba_live_config.json
rem  Skips  : __pycache__, nba_minute.db / -shm / -wal (live runtime data), backup\
rem ============================================================================
setlocal EnableExtensions
title polymarket -> GitHub

set "REPO=https://github.com/AlphaCoder0310/etcc.git"
set "CLONE=%USERPROFILE%\etcc"
set "SUB=polymarket"
set "SRC=%~dp0"

echo.
echo  polymarket -^> GitHub    clone=%CLONE%
echo  ---------------------------------------------------------------------

where git >nul 2>&1
if errorlevel 1 (
    echo [FAIL] git not found on PATH - install Git for Windows first
    goto :fail
)

rem ---- 1. local clone (first run) or pull (later runs) ------------------------
if not exist "%CLONE%\.git" (
    echo [..] cloning %REPO% ...
    git clone -q "%REPO%" "%CLONE%"
    if errorlevel 1 goto :fail
    echo [OK] cloned  : %CLONE%
) else (
    echo [..] pulling latest ...
    git -C "%CLONE%" pull -q --rebase
    if errorlevel 1 goto :fail
    echo [OK] pulled  : %CLONE%
)

rem ---- 2. copy project files --------------------------------------------------
if not exist "%CLONE%\%SUB%" mkdir "%CLONE%\%SUB%"
for %%F in (nba_live_web.py run_nba_live.bat nba_live_config.json) do (
    if exist "%SRC%%%F" (
        copy /y "%SRC%%%F" "%CLONE%\%SUB%\%%F" >nul
        echo [OK] copied  : %%F
    ) else (
        echo [WARN] %%F not found next to this bat - skipped
    )
)

rem .gitignore keeps the live sqlite db and cache out of the repo
(
    echo # runtime data written by nba_live_web.py - never commit
    echo __pycache__/
    echo *.pyc
    echo *.db
    echo *.db-shm
    echo *.db-wal
    echo backup/
) > "%CLONE%\%SUB%\.gitignore"

rem ---- 3. commit + push -------------------------------------------------------
git -C "%CLONE%" add "%SUB%"
git -C "%CLONE%" diff --cached --quiet
if not errorlevel 1 (
    echo [OK] nothing changed - GitHub already up to date
    goto :done
)

set "TS="
for /f %%T in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm"') do set "TS=%%T"
git -C "%CLONE%" commit -q -m "polymarket: sync %TS%"
if errorlevel 1 goto :fail
echo [OK] commit  : polymarket: sync %TS%

echo [..] pushing ...
git -C "%CLONE%" push
if errorlevel 1 goto :fail
echo.
echo [OK] pushed  : https://github.com/AlphaCoder0310/etcc/tree/main/%SUB%
goto :done

:fail
echo.
echo  SYNC FAILED  ^(see message above^)
pause
endlocal
exit /b 1

:done
echo.
echo  done.
timeout /t 8 >nul
endlocal
exit /b 0
