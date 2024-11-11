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

REM Nhận đường dẫn file config.xml từ đối số đầu vào
IF "%~1"=="" (
    echo Chưa cung cấp đường dẫn đến file config.xml. Vui lòng thử lại.
    echo Sử dụng: run_program.bat "duong_dan_toi_file_config.xml"
    exit /b
)

SET CONFIG_PATH=%~1
echo Đường dẫn file config.xml: %CONFIG_PATH%

REM Chạy script Python với đường dẫn file config.xml và hiển thị output
python extract_file.py %CONFIG_PATH%

pause
