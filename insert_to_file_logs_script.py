import os
import sys
import csv
import psycopg2
from datetime import datetime
from xml.etree.ElementTree import parse

# Hàm lấy thông tin cấu hình database
def get_db_config(config_path):
    try:
        tree = parse(config_path)
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

# Hàm lấy thông tin file
def get_file_info(file_path):
    try:
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            row_count = sum(1 for row in reader) - 1
        file_size_kb = os.path.getsize(file_path) // 1024
        return row_count, file_size_kb
    except Exception as e:
        print("Lỗi khi đọc file:", e)
        return None, None

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
    if len(sys.argv) < 4:
        print("Vui lòng cung cấp: <id_config> <source_file_location> <config_path>")
        sys.exit(1)

    id_config = int(sys.argv[1])
    source_file_location = sys.argv[2]
    config_path = sys.argv[3]

    conn = connect_to_db(config_path)
    if conn is None:
        return

    try:
        row_count, file_size_kb = get_file_info(source_file_location)
        if row_count is None or file_size_kb is None:
            return

        dt_update = datetime.now()
        extract_date = dt_update.date()

        insert_file_log(
            conn,
            id_config=id_config,
            file_name=os.path.basename(source_file_location),
            time=extract_date,
            status="ER",
            count=row_count,
            file_size_kb=file_size_kb,
            dt_update=dt_update
        )
    finally:
        conn.close()

if __name__ == "__main__":
    main()
