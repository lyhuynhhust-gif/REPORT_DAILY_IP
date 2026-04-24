@echo off
title IPSS Daily Report System
color 0A
echo ============================================
echo    IPSS Daily Production Report System
echo ============================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python chua duoc cai dat!
    echo Vui long cai Python 3.9+ tu https://python.org
    pause & exit /b 1
)

echo [1/3] Kiem tra dependencies...
pip install streamlit plotly openpyxl pandas numpy schedule pywin32 -q 2>nul

echo [2/3] Tao thu muc output...
if not exist "reports" mkdir reports

echo [3/3] Khoi dong dashboard...
echo.
echo  ==> Truy cap tai: http://localhost:8501
echo  ==> Nhan Ctrl+C de dung
echo.
streamlit run app.py --server.port 8501 --server.headless false --browser.gatherUsageStats false --server.fileWatcherType none

pause
