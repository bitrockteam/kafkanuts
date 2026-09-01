@echo off
setlocal
set "SCRIPT_DIR=%~dp0."
docker compose --project-directory "%SCRIPT_DIR%" -f "%SCRIPT_DIR%\compose.yaml" run --rm builder mvn %*
set "EXIT_CODE=%ERRORLEVEL%"
exit /b %EXIT_CODE%
