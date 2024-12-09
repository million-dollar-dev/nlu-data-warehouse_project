@echo off

:: Kiểm tra số lượng tham số
if "%1"=="" (
    echo "Thiếu tham số id_config."
    echo "Cú pháp: run_scripts.bat id_config path_to_config.xml [YYYY-MM-DD]"
    pause
    exit /b 1
)

if "%2"=="" (
    echo "Thiếu tham số path_to_config.xml."
    echo "Cú pháp: run_scripts.bat id_config path_to_config.xml [YYYY-MM-DD]"
    pause
    exit /b 1
)

:: Gán tham số đầu vào
set id_config=%1
set config_path=%2
set date_param=%3

:: Nếu tham số ngày không được truyền, lấy ngày hôm nay
if "%date_param%"=="" (
    for /f %%i in ('powershell -command "Get-Date -Format yyyy-MM-dd"') do set date_param=%%i
)

python extract_file.py %id_config% %config_path% %date_param%

python load_to_staging.py %id_config% %config_path% %date_param%

python load_to_dw.py %id_config% %config_path% %date_param%

echo Hoàn tất!
pause
