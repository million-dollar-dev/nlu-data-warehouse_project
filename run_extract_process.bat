@echo off
REM Kiểm tra phiên bản Python
python --version >nul 2>&1
IF ERRORLEVEL 1 (
    echo Python chưa được cài đặt. Vui lòng cài đặt Python và thử lại.
    exit /b
)

REM Kiểm tra và cài đặt thư viện nếu cần
echo Đang kiểm tra các thư viện Python cần thiết...

REM Kiểm tra psycopg2
python -c "import psycopg2" >nul 2>&1
IF ERRORLEVEL 1 (
    echo Thư viện psycopg2 chưa được cài đặt. Đang cài đặt psycopg2...
    pip install psycopg2
)

REM Kiểm tra tabulate
python -c "import tabulate" >nul 2>&1
IF ERRORLEVEL 1 (
    echo Thư viện tabulate chưa được cài đặt. Đang cài đặt tabulate...
    pip install tabulate
)

set LIBRARIES=requests pandas bs4 beautifulsoup4 datetime

for %%L in (%LIBRARIES%) do (
    echo Đang kiểm tra thư viện %%L...
    
    rem Kiểm tra nếu thư viện đã cài, nếu chưa cài thì tiến hành cài đặt
    python -c "import %%L" 2>nul
    if %errorlevel% neq 0 (
        echo Thư viện %%L chưa được cài đặt. Đang cài đặt...
        pip install %%L
    ) else (
        echo Thư viện %%L đã được cài đặt.
    )
)

REM Chạy script Python với đường dẫn file config.xml và hiển thị output


pause
