@echo off
echo Please select the program to run:
echo 1. hana_query_analyzer
echo 2. main
echo.

choice /c 12 /m "Enter your choice"

if errorlevel 2 goto main
if errorlevel 1 goto hana_query_analyzer

:hana_query_analyzer
echo Running hana_query_analyzer...
venv\Scripts\python.exe hana_query_analyzer.py
goto end

:main
echo Running main...
venv\Scripts\python.exe main.py
goto end

:end
echo Program has exited.
pause