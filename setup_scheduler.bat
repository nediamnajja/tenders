@echo off
REM ============================================================
REM  setup_scheduler.bat
REM  Run this ONCE as Administrator to register the daily task
REM  Right-click → "Run as administrator"
REM ============================================================

SET TASK_NAME=KPMG_Tender_Pipeline
SET PYTHON=C:\projects\tenders\.venv\Scripts\python.exe
SET SCRIPT=C:\projects\tenders\orchestrator.py
SET LOG=C:\projects\tenders\logs\scheduler.log
SET TIME=07:00

echo.
echo ============================================================
echo   KPMG Tender Pipeline — Task Scheduler Setup
echo ============================================================
echo.
echo   Task name : %TASK_NAME%
echo   Python    : %PYTHON%
echo   Script    : %SCRIPT%
echo   Runs at   : %TIME% every day
echo.

REM Delete existing task if it exists (clean reinstall)
schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1

REM Create the scheduled task
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "\"%PYTHON%\" \"%SCRIPT%\"" ^
  /sc DAILY ^
  /st %TIME% ^
  /ru SYSTEM ^
  /rl HIGHEST ^
  /f

IF %ERRORLEVEL% EQU 0 (
    echo.
    echo   Task created successfully
    echo.
    echo   To verify: open Task Scheduler and look for %TASK_NAME%
    echo   To test now: schtasks /run /tn "%TASK_NAME%"
    echo   To remove:   schtasks /delete /tn "%TASK_NAME%" /f
    echo.
) ELSE (
    echo.
    echo   ERROR: Task creation failed.
    echo   Make sure you ran this as Administrator.
    echo.
)

pause