@echo off
setlocal
 docker compose run --rm builder %*
exit /b %ERRORLEVEL%
