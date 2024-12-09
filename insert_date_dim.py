import sys
import os
from datetime import datetime
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2 import extras
import csv


def load_database_config(db_name, config_path):
    """
    Hàm đọc file config.xml và lấy thông tin kết nối cho database có tên cụ thể.

    :param db_name: Tên cơ sở dữ liệu cần kết nối.
    :param config_path: Đường dẫn file config.xml.
    :return: Dictionary chứa thông tin kết nối.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"File config tại '{config_path}' không tồn tại.")

    # Parse file XML
    tree = ET.parse(config_path)
    root = tree.getroot()

    # Tìm thông tin database theo tên
    for db in root.findall(".//database"):
        if db.get("name") == db_name:
            return {
                "hostname": db.find("hostname").text,
                "port": db.find("port").text,
                "database": db.find("database").text,
                "username": db.find("username").text,
                "password": db.find("password").text,
            }

    raise ValueError(f"Không tìm thấy database với tên '{db_name}' trong file config.")


def connect_to_database(db_config):
    """
    Hàm kết nối tới cơ sở dữ liệu PostgreSQL dựa trên thông tin cấu hình.

    :param db_config: Dictionary chứa thông tin kết nối.
    :return: Kết nối PostgreSQL (psycopg2 connection object).
    """

    conn = psycopg2.connect(
        host=db_config["hostname"],
        port=db_config["port"],
        database=db_config["database"],
        user=db_config["username"],
        password=db_config["password"],
    )
    print("Kết nối cơ sở dữ liệu thành công.")
    return conn

def insert_date_dim(conn, csv_path):
    """
    Đọc dữ liệu từ file CSV và chèn vào bảng date_dim.

    Args:
        conn: Kết nối psycopg2 tới PostgreSQL.
        csv_path: Đường dẫn tới file CSV.
    """
    try:
        # Mở file CSV
        with open(csv_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Đọc dòng tiêu đề

            # Kiểm tra cột của file CSV
            expected_columns = {
                'full_date', 'day_of_month', 'month', 'day_name', 'month_name', 'year',
                'start_of_week', 'day_of_week', 'day_of_year', 'iso_week', 'iso_week_year',
                'start_of_iso_week', 'iso_week_alt', 'iso_week_year_alt', 'start_of_iso_alt',
                'quarter', 'quarter_num', 'holiday_flag', 'is_weekend'
            }
            
            if not set(headers).issubset(expected_columns):
                raise ValueError("File CSV không đúng định dạng hoặc thiếu cột cần thiết.")
            
            # Tạo câu lệnh INSERT
            insert_query = sql.SQL("""
                INSERT INTO date_dim (
                    full_date, day_of_month, month, day_name, month_name, year,
                    start_of_week, day_of_week, day_of_year, iso_week, iso_week_year,
                    start_of_iso_week, iso_week_alt, iso_week_year_alt, start_of_iso_alt,
                    quarter, quarter_num, holiday_flag, is_weekend
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """)

            # Chèn dữ liệu vào bảng
            with conn.cursor() as cursor:
                for row in reader:
                    cursor.execute(insert_query, row)
            
            # Lưu thay đổi
            conn.commit()
            print("Dữ liệu đã được chèn thành công vào bảng date_dim.")

    except Exception as e:
        conn.rollback()
        print(f"Đã xảy ra lỗi: {e}")
        

def main():
    if len(sys.argv) < 3:
        print("Vui lòng nhập ít nhất 3 tham số: path_config, và csv_path.")
        sys.exit(1)

    # Nhận tham số đầu vào
    path_config = sys.argv[1]
    csv_path = sys.argv[2]

    # 2.1. Load file config.xml
    db_config = load_database_config("dw", path_config)
    try:
        # 2.2.Kết nối cơ sở dữ liệu dw
        conn = connect_to_database(db_config)
    except Exception as e:
        sys.exit(1)
        return

    insert_date_dim(conn, csv_path)

if __name__ == "__main__":
    main()

