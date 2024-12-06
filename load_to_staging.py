import requests
import pandas as pd
import re
from datetime import datetime
import sys
import os
import subprocess
from datetime import datetime
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2 import extras
import csv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

EMAIL = 'hoangtunqs134@gmail.com'

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
        if db.get('name') == db_name:
            return {
                'hostname': db.find('hostname').text,
                'port': db.find('port').text,
                'database': db.find('database').text,
                'username': db.find('username').text,
                'password': db.find('password').text
            }
    
    raise ValueError(f"Không tìm thấy database với tên '{db_name}' trong file config.")

def connect_to_database(db_config):
    """
    Hàm kết nối tới cơ sở dữ liệu PostgreSQL dựa trên thông tin cấu hình.
    
    :param db_config: Dictionary chứa thông tin kết nối.
    :return: Kết nối PostgreSQL (psycopg2 connection object).
    """
    try:
        conn = psycopg2.connect(
            host=db_config['hostname'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password']
        )
        print(f"Kết nối cơ sở dữ liệu {db_config['database']} thành công.")
        return conn
    except Exception as e:
        print(f"Lỗi khi kết nối cơ sở dữ liệu: {e}")
        sys.exit(1)

def fetch_file_info(conn, id_config, date):
    """
    Hàm join hai bảng file_config và file_logs để truy vấn các bản ghi sẵn sàng load vào staging (status = 'ER').
    
    :param conn: Kết nối PostgreSQL.
    :param id_config: ID Config cần lọc.
    :param date: Ngày cần lọc.
    :return: Dictionary của bản ghi lỗi hoặc dictionary rỗng nếu không tìm thấy.
    """
    query = """
    SELECT
        fl.id, 
        fl.id_config,
        fl.file_name,
        fc.source,
        fl.time,
        fl.status,
        fl.count,
        fl.file_size_kb,
        fl.dt_update,
        fc.source_file_location,
        fc.destination_table_staging
    FROM file_logs fl
        INNER JOIN file_config fc ON fl.id_config = fc.id
    WHERE fl.id_config = %s AND fl.time::date = %s AND fl.status = 'ES'
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (id_config, date))
            results = cur.fetchall()

            if results:
                return dict(results[0])

            # Nếu không có kết quả, trả về dictionary rỗng
            return {}

    except Exception as e:
        print(f"Lỗi khi thực hiện truy vấn: {e}")
        return {}


def insert_csv_to_table(conn, file_path, table_name, id_config, dt_extract, dt_load):
    """
    Hàm chèn dữ liệu từ file CSV vào bảng PostgreSQL.

    :param conn: Kết nối PostgreSQL.
    :param file_path: Đường dẫn đầy đủ đến file CSV.
    :param table_name: Tên bảng để chèn dữ liệu.
    :param id_config: Giá trị chèn vào cột id_config.
    :param dt_extract: Giá trị chèn vào cột dt_extract.
    :param dt_load: Giá trị chèn vào cột dt_load.
    """
    try:
        print(f'Đang import dữ liệu từ file...')
        with conn.cursor() as cur:
            # Đọc dữ liệu từ file CSV
            with open(file_path, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Lấy danh sách các cột từ file CSV
                csv_columns = reader.fieldnames
                
                # Chuẩn bị câu lệnh INSERT
                columns = ", ".join(csv_columns + ["natural_key", "id_config", "dt_extract", "dt_load"])
                placeholders = ", ".join(["%s"] * (len(csv_columns) + 4))  # 3 cột bổ sung
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                # Chèn từng dòng từ file CSV vào bảng
                for row in reader:
                    # Tạo giá trị cho cột `natural_key` (nối `product_name` và `sku`)
                    natural_key = f"{row['product_name']}-{row['sku']}"
                    
                    # Lấy giá trị từ file CSV và thêm các cột bổ sung
                    values = [row[col] for col in csv_columns] + [natural_key, id_config, dt_extract, dt_load]
                    cur.execute(query, values)
            
            # Commit thay đổi
            conn.commit()
            print(f"Dữ liệu từ file '{file_path}' đã được chèn vào bảng '{table_name}' thành công.")
    except Exception as e:
        print(f"Lỗi khi chèn dữ liệu từ file CSV vào bảng: {e}")
        conn.rollback()

def transform_data(conn, table_name):
    """
    Hàm thay thế tất cả các giá trị NULL trong bảng thành:
    - 'N/A' cho các cột kiểu chuỗi.
    - -1 cho các cột kiểu số.
    
    :param conn: Kết nối PostgreSQL.
    :param table_name: Tên bảng cần thay thế giá trị NULL.
    """
    # Câu truy vấn thay thế NULL
    query = f"""
    UPDATE {table_name}
    SET 
        sku = COALESCE(sku, 'N/A'),
        product_name = COALESCE(product_name, 'N/A'),
        price = COALESCE(price, -1),
        brand = COALESCE(brand, 'N/A'),
        material = COALESCE(material, 'N/A'),
        shape = COALESCE(shape, 'N/A'),
        dimension = COALESCE(dimension, 'N/A'),
        origin = COALESCE(origin, 'N/A'),
        quantity_available = COALESCE(quantity_available, -1),
        product_url = COALESCE(product_url, 'N/A');
    """
    
    try:
        print(f"Đang Tranform bảng '{table_name}'...")
        with conn.cursor() as cur:
            cur.execute(query)  # Thực thi câu truy vấn
            conn.commit()  # Ghi nhận thay đổi
            print(f"Đã Tranform bảng '{table_name}'.")
    except Exception as e:
        print(f"Lỗi khi thay thế giá trị NULL: {e}")
        conn.rollback()  # Quay lại trạng thái trước đó nếu có lỗi


def update_status(conn, record_id, id_config, time_value, status):
    """
    Cập nhật trường `status` của một bản ghi trong bảng `file_logs`.

    :param conn: Kết nối cơ sở dữ liệu PostgreSQL đã được tạo.
    :param record_id: Giá trị của `id` dùng để tìm bản ghi.
    :param id_config: Giá trị của `id_config` dùng để tìm bản ghi.
    :param time_value: Giá trị thời gian để cập nhật.
    """
    try:
        # Tạo con trỏ
        cursor = conn.cursor()

        # Câu lệnh SQL để cập nhật
        update_query = """
        UPDATE file_logs
        SET status = %s, time = %s
        WHERE id = %s AND id_config = %s;
        """

        # Thực thi câu lệnh SQL
        cursor.execute(update_query, (status, time_value, record_id, id_config))

        # Xác nhận thay đổi
        conn.commit()

        # Kiểm tra số lượng bản ghi được cập nhật
        if cursor.rowcount > 0:
            print(f"Successfully updated {cursor.rowcount} record(s).")
        else:
            print("No records were updated. Please check your input conditions.")

    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    finally:
        # Đóng con trỏ
        if cursor:
            cursor.close()

def check_file_log(conn, id_config, date):
    """
    Hàm kiểm tra trong bảng `file_logs` có bản ghi nào có `id_config` là id_config nhập vào,
    `time` là ngày nhập vào và `status` là 'Loading' hoặc 'LS'.

    :param conn: Kết nối PostgreSQL.
    :param id_config: Giá trị `id_config` cần kiểm tra.
    :param date: Ngày cần kiểm tra (dạng chuỗi 'YYYY-MM-DD').
    :return: True nếu tồn tại bản ghi thỏa mãn, False nếu không.
    """
    query = """
    SELECT 1
    FROM file_logs
    WHERE id_config = %s
      AND time = %s
      AND (status = 'Loading' OR status = 'LS' OR status != 'ES')
    LIMIT 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (id_config, date))
            result = cur.fetchone()
            if result:
                print(f"Có bản ghi thỏa mãn điều kiện trong file_logs.")
                return True
            else:
                print(f"Không có bản ghi thỏa mãn điều kiện trong file_logs.")
                return False
    except Exception as e:
        print(f"Lỗi khi kiểm tra bản ghi trong file_logs: {e}")
        return False

def check_csv_file_exists(file_path):
    """
    Hàm kiểm tra xem tệp .csv có tồn tại tại đường dẫn nhập vào hay không.

    :param file_path: Đường dẫn tới tệp .csv.
    :return: True nếu tệp tồn tại, False nếu không.
    """
    if os.path.isfile(file_path) and file_path.lower().endswith('.csv'):
        return True
    else:
        return False

def send_email(to_email, subject, body):
    """
    Gửi email qua Gmail SMTP.

    :param to_email: Email người nhận (str)
    :param subject: Tiêu đề email (str)
    :param body: Nội dung email (str)
    :return: None
    """
    # Thông tin tài khoản Gmail
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    EMAIL = "chamdaynoidaucuaerik@gmail.com"
    PASSWORD = "wuag gbxt lhoa lele"

    try:
        # Tạo email
        msg = MIMEMultipart()
        msg["From"] = EMAIL
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL, PASSWORD)
            server.sendmail(EMAIL, to_email, msg.as_string())
            print(f"Email đã được gửi tới {to_email}")

    except Exception as e:
        print(f"Đã xảy ra lỗi khi gửi email: {e}")

def main():
    if len(sys.argv) < 3:
        print("Vui lòng nhập ít nhất 3 tham số: id_config, path_config, và db_name.")
        print("Cú pháp: python script.py <id_config> <path_config> [date]")
        sys.exit(1)
    
    # Nhận tham số đầu vào
    id_config = sys.argv[1]
    path_config = sys.argv[2]
    
    # Xử lý tham số ngày, mặc định là ngày hôm nay nếu không có đầu vào
    if len(sys.argv) > 3:
        date_str = sys.argv[3]
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            print("Lỗi: Ngày không đúng định dạng YYYY-MM-DD.")
            sys.exit(1)
    else:
        date = datetime.today().strftime("%Y-%m-%d")
    
    # In thông tin
    print(f"ID Config: {id_config}")
    print(f"Path Config: {path_config}")
    print(f"Date: {date}")

    # Load thông tin kết nối từ file config
    db_config = load_database_config("dw", path_config)
    try:
        # Kết nối cơ sở dữ liệu dw
        conn = connect_to_database(db_config)
    except Exception as e:
        send_email(EMAIL, 'LỖI KẾT NỐI CƠ SỞ DỮ LIỆU DW', 'Lỗi phát hiện: {e}')
        sys.exit(1)
        return
    
    #Kiểm tra file log có tiến trình đang chạy hay đã chạy hay không có file nào có status ES
    exists = check_file_log(conn, id_config, date)
    if exists:
        print('Có tiến trình đã/đang thực hiện hoặc không có file nào sẵn sàng đưa vào staging')
        send_email(EMAIL, 'LỖI TRONG QUÁ TRÌNH LOAD_TO_STAGING: NGÀY {date} | ID CONFIG: {id_config}', 'Lỗi phát hiện: Đã có tiến trình đang/đã chạy hoặc không có file sẵn sàng đưa vào staging')
    else:
        # Lấy thông tin file log
        file_info = fetch_file_info(conn, id_config, date)
        print(file_info)
        # Kiểm tra file .csv theo thông tin của file config
        if not check_csv_file_exists(file_info['source_file_location'] + "\\" + file_info['file_name']):
            send_email(EMAIL, 'LỖI KẾT TRONG QUÁ TRÌNH LOAD_TO_STAGING: NGÀY {date} | ID CONFIG: {id_config}', 'Lỗi phát hiện: {e}')
        else:
            #Cập nhật file log sang status "Loading to staging"
            update_status(conn, file_info['id'], id_config, file_info['time'], 'Loading')
            #Insert dữ liệu vào bảng staging tương ứng
            insert_csv_to_table(conn, file_info['source_file_location'] + "\\" + file_info['file_name'], file_info['destination_table_staging'], id_config, file_info['time'], date)
            #Transform dữ liệu
            transform_data(conn, file_info['destination_table_staging'])
            #Cập nhật file log sang "LR"
            update_status(conn, file_info['id'], id_config, file_info['time'], 'LS')
    conn.close()
if __name__ == "__main__":
    main()
    
