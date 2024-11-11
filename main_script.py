import os
import sys
import csv
import psycopg2
import xml.etree.ElementTree as ET
from datetime import datetime
from tabulate import tabulate
import subprocess

# Hàm đọc cấu hình từ file config.xml
def get_db_config(config_path):
    try:
        tree = ET.parse(config_path)
        root = tree.getroot()
        databases = root.find('databases')
        if databases is None:
            print("Không tìm thấy thẻ <databases> trong file cấu hình.")
            return None

        for db in databases.findall('database'):
            if db.get('name') == 'controls':
                host = db.find('hostname')
                port = db.find('port')
                database = db.find('database')
                user = db.find('username')
                password = db.find('password')
                
                if None in (host, port, database, user, password):
                    print("Thiếu thông tin cấu hình cho database 'controls' trong file config.xml.")
                    return None

                return {
                    'host': host.text,
                    'port': port.text,
                    'database': database.text,
                    'user': user.text,
                    'password': password.text
                }
    except Exception as e:
        print("Lỗi khi đọc file cấu hình:", e)
    return None

# Hàm kết nối cơ sở dữ liệu
def connect_to_db(config_path):
    config = get_db_config(config_path)
    if config is None:
        return None
    try:
        conn = psycopg2.connect(**config)
        return conn
    except Exception as e:
        print("Lỗi kết nối với database:", e)
        return None

# Hàm kiểm tra và tạo bảng nếu chưa tồn tại
def check_and_create_tables(conn):
    queries = [
        """
        CREATE TABLE IF NOT EXISTS file_config (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            source VARCHAR(255),
            source_file_location VARCHAR(500),
            destination_table_staging VARCHAR(255),
            destination_table_dw VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS file_logs (
            id SERIAL,
            id_config INTEGER REFERENCES file_config(id) ON DELETE CASCADE,
            file_name VARCHAR(500),
            time DATE,
            status VARCHAR(5),
            count INTEGER,
            file_size_kb INTEGER,
            dt_update TIMESTAMP,
            PRIMARY KEY (id, file_name)
        );
        """
    ]
    try:
        with conn.cursor() as cur:
            for query in queries:
                cur.execute(query)
            conn.commit()
            print("Kiểm tra và tạo bảng hoàn tất.")
    except Exception as e:
        print("Lỗi khi kiểm tra hoặc tạo bảng:", e)
        conn.rollback()

# Hàm hiển thị dữ liệu từ bảng file_config và chọn ID
def select_file_config(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, source, source_file_location, destination_table_staging FROM file_config;")
            records = cur.fetchall()
            headers = ["ID", "Name", "Source", "Source Location", "Destination Table Staging"]
            print("Danh sách file_config:")
            print(tabulate(records, headers=headers, tablefmt="grid"))

            selected_id = input("Nhập ID mà bạn muốn chọn: ")
            cur.execute("SELECT id, source_file_location, destination_table_staging FROM file_config WHERE id = %s;", (selected_id,))
            result = cur.fetchone()
            return result
    except Exception as e:
        print("Lỗi khi lấy dữ liệu từ bảng file_config:", e)
        return None

# Kiểm tra Task Scheduler và cài đặt nếu cần
def setup_task_scheduler(task_name, id_config, source_file_location, config_path):
    task_exists = subprocess.call(f'schtasks /query /tn "{task_name}"', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0
    if not task_exists:
        command = f'schtasks /create /tn "{task_name}" /tr "run_scraper.bat {source_file_location}" /sc daily /st 12:00'
        subprocess.call(command, shell=True)
        print(f"Đã tạo task scheduler: {task_name}")
    else:
        print(f"Task scheduler '{task_name}' đã tồn tại.")

# Hàm cào dữ liệu thủ công
def run_manual_scraper(source_file_location):
    subprocess.call(f'run_scraper.bat {source_file_location}', shell=True)

# Hàm ghi log vào bảng file_logs
def insert_file_log(conn, id_config, file_name, time, status, count, file_size_kb, dt_update):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO file_logs (id_config, file_name, time, status, count, file_size_kb, dt_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (id_config, file_name, time, status, count, file_size_kb, dt_update)
            )
            conn.commit()
            print("Đã lưu log vào bảng file_logs.")
    except Exception as e:
        print("Lỗi khi lưu log vào bảng file_logs:", e)
        conn.rollback()

# Chương trình chính
def main():
    if len(sys.argv) < 2:
        print("Vui lòng cung cấp đường dẫn tới file config.xml.")
        sys.exit(1)

    config_path = sys.argv[1]
    conn = connect_to_db(config_path)
    if conn is None:
        return

    try:
        check_and_create_tables(conn)
        selected_record = select_file_config(conn)
        if selected_record is None:
            return

        id_config, source_file_location, destination_table_staging = selected_record
        task_name = f"Extract_Task_{id_config}_{destination_table_staging}"

        print("Chọn cách thức cào dữ liệu:")
        print("1. Cào tự động (đặt task trong Task Scheduler)")
        print("2. Cào thủ công")
        choice = input("Lựa chọn của bạn (1 hoặc 2): ")

        if choice == '1':
            setup_task_scheduler(task_name, id_config, source_file_location, config_path)
        elif choice == '2':
            run_manual_scraper(source_file_location)
            file_name = os.path.basename(source_file_location)
            time = datetime.now().date()
            dt_update = datetime.now()
            
            with open(source_file_location, 'r') as f:
                row_count = sum(1 for _ in f) - 1
            file_size_kb = os.path.getsize(source_file_location) // 1024

            insert_file_log(conn, id_config, file_name, time, "ER", row_count, file_size_kb, dt_update)
        else:
            print("Lựa chọn không hợp lệ.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
