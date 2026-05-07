@"
@echo off
SET TASK_NAME=KPMG_Tender_Pipeline
SET PYTHON=C:\projects\tenders\.venv\Scripts\python.exe
SET SCRIPT=C:\projects\tenders\orchestrator.py
SET TIME=07:00

schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1

schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "\"%PYTHON%\" \"%SCRIPT%\"" ^
  /sc DAILY ^
  /st %TIME% ^
  /ru "%USERDOMAIN%\%USERNAME%" ^
  /rl HIGHEST ^
  /f

IF %ERRORLEVEL% EQU 0 (
    echo Task created successfully - runs daily at %TIME%
) ELSE (
    echo ERROR: Task creation failed.
)

pause
"@ | Out-File -FilePath "setup_scheduler.bat" -Encoding ascii