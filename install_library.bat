@echo off
setlocal enabledelayedexpansion

:: Kiểm tra xem Python đã cài đặt chưa
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python chưa được cài đặt hoặc chưa được thêm vào PATH.
    pause
    exit /b 1
)

:: Danh sách các thư viện cần kiểm tra
set LIBRARIES=numpy pandas requests beautifulsoup4 lxml psycopg2

:: Kiểm tra và cài đặt từng thư viện
for %%L in (%LIBRARIES%) do (
    echo Đang kiểm tra thư viện %%L...
    python -m pip show %%L >nul 2>&1
    if %errorlevel% neq 0 (
        echo Thư viện %%L chưa được cài đặt. Đang tiến hành cài đặt...
        python -m pip install %%L
        if %errorlevel% neq 0 (
            echo Lỗi khi cài đặt thư viện %%L. Vui lòng kiểm tra lại kết nối Internet hoặc quyền truy cập.
            pause
            exit /b 1
        )
    ) else (
        echo Thư viện %%L đã được cài đặt.
    )
)

:: Hoàn tất
echo Tất cả các thư viện đã được kiểm tra và cài đặt.
pause
