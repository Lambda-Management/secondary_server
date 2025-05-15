@echo off
echo Starting Data Service API Server...

REM Set environment variables
set SERVER_ID=secondary
set SERVER_PORT=8080

REM Activate virtual environment if you're using one
REM call venv\Scripts\activate.bat

REM Start the server
python -m data_server.data_service

pause 