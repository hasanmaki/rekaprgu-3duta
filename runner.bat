@echo off
title Rekap RGU Application
color 0A

echo ========================================
echo     Rekap RGU Application Runner
echo ========================================
echo.

REM Check if uv is installed
echo Checking uv installation...
uv --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] uv is not installed or not in PATH
    echo Please install uv first: https://docs.astral.sh/uv/getting-started/installation/
    pause
    exit /b 1
)

echo [OK] uv found
echo.

REM Sync dependencies with uv
echo Syncing dependencies with uv...
uv sync
if errorlevel 1 (
    echo [ERROR] Failed to sync dependencies
    pause
    exit /b 1
)

echo [OK] Dependencies synced successfully
echo.

REM Check if virtual environment exists
if not exist ".venv\Scripts\activate.bat" (
    echo [ERROR] Virtual environment not found
    echo Please run 'uv sync' again to create the virtual environment
    pause
    exit /b 1
)

REM Activate virtual environment and run streamlit
echo Activating virtual environment and starting Streamlit...
echo.
echo ========================================
echo     Starting Streamlit Server...
echo ========================================
echo.

call .venv\Scripts\activate && streamlit run streamlit_app.py

echo.
echo Streamlit server stopped.
pause
