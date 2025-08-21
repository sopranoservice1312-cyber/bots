@echo off
REM Auto-create venv, install dependencies, run FastAPI panel, open browser
SET VENV_DIR=%~dp0.venv
IF NOT EXIST "%VENV_DIR%" (
    echo Creating virtual environment...
    python -m venv "%VENV_DIR%"
)
echo Activating virtual environment...
call "%VENV_DIR%\Scripts\activate"

echo Installing dependencies...
pip install --upgrade pip
pip install -r "%~dp0requirements.txt"

echo Starting Telegram Consent Messenger panel...
start "" "http://127.0.0.1:8000"
python -m app.main
pause
