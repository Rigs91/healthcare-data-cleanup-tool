@echo off
setlocal
powershell.exe -ExecutionPolicy Bypass -File "%~dp0scripts\start_hcdata_1_click.ps1"
set EXITCODE=%ERRORLEVEL%
if not "%EXITCODE%"=="0" pause
exit /b %EXITCODE%
