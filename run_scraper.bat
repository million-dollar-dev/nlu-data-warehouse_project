@echo off
setlocal

REM Kiểm tra nếu Python đã được cài đặt
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed. Installing Python...
    REM Tải và cài đặt Python từ trang chủ Python
    REM Lưu ý: Thao tác này yêu cầu quyền quản trị
    powershell -Command "Start-Process 'https://www.python.org/ftp/python/3.10.7/python-3.10.7-amd64.exe' -Wait"
    
    REM Hướng dẫn người dùng hoàn thành cài đặt Python
    echo Please install Python and add it to PATH.
    pause
    exit /b
)

REM Cài đặt các thư viện cần thiết
echo Installing required Python libraries...
pip install requests beautifulsoup4 pandas

REM Đường dẫn lưu file CSV, lấy từ tham số đầu vào
set "csv_dir=%~1"

REM Kiểm tra và tạo thư mục nếu chưa tồn tại
if not exist "%csv_dir%" (
    echo Creating directory: %csv_dir%
    mkdir "%csv_dir%"
)

REM Chạy script Python và hiển thị output ra màn hình
echo Running Python scraper script...
python scraper.py

REM Tìm file CSV vừa tạo
for %%f in (daily_data_*.csv) do (
    move "%%f" "%csv_dir%\%%f"
    echo Data saved to %csv_dir%\%%f
)

pause
endlocal
