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
    WHERE fl.id_config = %s AND fl.time::date = %s AND fl.status = 'ER'
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

def export_table_to_csv(conn, folder_path, date, table_name, source):
    """
    Xuất dữ liệu từ một bảng trong PostgreSQL ra file CSV.

    :param conn: Kết nối PostgreSQL.
    :param folder_path: Đường dẫn folder để lưu file.
    :param date: Ngày xuất dữ liệu (định dạng yyyy-mm-dd).
    :param table_name: Tên bảng cần export.
    :return: Đường dẫn đầy đủ của file CSV đã xuất ra.
    """
    # Tạo tên file
    date = date.strftime("%Y-%m-%d")
    domain_name = source.split("//")[1].split("/")[0]
    file_name = f"lr_{table_name}_{date}_{domain_name}.csv"
    file_path = os.path.join(folder_path, file_name)
    
    # Câu lệnh truy vấn
    query = f"SELECT * FROM {table_name}"
    
    try:
        with conn.cursor() as cur:
            # Thực hiện truy vấn
            cur.execute(query)
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]  # Lấy tên cột
            
            # Ghi dữ liệu ra file CSV
            with open(file_path, mode='w', encoding='utf-8', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(column_names)  # Ghi dòng tên cột
                writer.writerows(rows)        # Ghi dữ liệu
                
            print(f"Dữ liệu đã được export ra file: {file_path}")
            return file_name  # Trả về đường dẫn file
    except Exception as e:
        print(f"Lỗi khi export dữ liệu từ bảng '{table_name}': {e}")
        return None

def update_status_to_lr(conn, id, file_name):
    """
    Hàm cập nhật trường status thành 'LR' và dt_update thành thời điểm hiện tại
    cho record có id là id nhập vào.

    :param conn: Kết nối PostgreSQL.
    :param id: ID của record cần cập nhật.
    """
    # Thời gian hiện tại
    dt_update = datetime.now()

    # Câu lệnh SQL để cập nhật record
    query = """
    UPDATE file_logs
    SET file_name = %s, status = 'LR', dt_update = %s
    WHERE id = %s
    """
    
    try:
        with conn.cursor() as cur:
            # Thực thi câu lệnh SQL
            cur.execute(query, (file_name, dt_update, id))
            conn.commit()  # Lưu thay đổi vào cơ sở dữ liệu
            print(f"Record với id {id} đã được cập nhật thành công.")
    except Exception as e:
        print(f"Lỗi khi cập nhật record: {e}")
        conn.rollback()  # Rollback nếu có lỗi


def update_status(conn, record_id, id_config, time_value):
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
        SET status = 'ES', time = %s
        WHERE id = %s AND id_config = %s;
        """

        # Thực thi câu lệnh SQL
        cursor.execute(update_query, (time_value, record_id, id_config))

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


def insert_file_log_LR(conn, id_config, time, file_name, count, file_size_kb):
    """
    Thêm một bản ghi mới vào bảng `file_logs`.

    :param conn: Kết nối cơ sở dữ liệu PostgreSQL đã được tạo.
    :param id_config: Giá trị của cột `id_config` trong bảng `file_logs`.
    :param time: Thời gian để thêm vào cột `time`.
    :param file_name: Tên file để thêm vào cột `file_name`.
    :param cout: Giá trị số để thêm vào cột `cout`.
    :param file_size_location: Giá trị để thêm vào cột `file_size_location`.
    """
    try:
        # Tạo con trỏ
        cursor = conn.cursor()

        # Lấy thời gian hiện tại cho `dt_update`
        current_time = datetime.now()

        # Câu lệnh SQL để chèn dữ liệu
        insert_query = """
        INSERT INTO file_logs (
            id_config, 
            file_name, 
            time, 
            status, 
            count, 
            file_size_kb, 
            dt_update
        )
        VALUES (%s, %s, %s, 'LR', %s, %s, %s);
        """

        # Thực thi câu lệnh SQL
        cursor.execute(insert_query, (id_config, file_name, time, count, file_size_kb, current_time))

        # Xác nhận thay đổi
        conn.commit()

        print("Insert successful.")

    except psycopg2.Error as e:
        print(f"An error occurred during insertion: {e}")
    finally:
        # Đóng con trỏ
        if cursor:
            cursor.close()


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
    try:
        db_controls_config = load_database_config("controls", path_config)
        db_staging_config = load_database_config("staging", path_config)
    except Exception as e:
        print(f"Lỗi: {e}")
        sys.exit(1)

    # Kết nối cơ sở dữ liệu controls
    conn = connect_to_database(db_controls_config)
    # Logic xử lý sau khi kết nối (nếu cần)
    # Thực hiện truy vấn Join và lấy kết quả
    file_info = fetch_file_info(conn, id_config, date)
    if not file_info:
        print('Không có file trong trạng thái ER')
        return
    conn.close()
    #Kết nối csdl staging
    conn = connect_to_database(db_staging_config)
    #Insert dữ liệu vào staging
    insert_csv_to_table(conn, file_info['source_file_location'] + "\\" + file_info['file_name'], file_info['destination_table_staging'], id_config, file_info['time'], date)
    #Transform dữ liệu
    transform_data(conn, file_info['destination_table_staging'])
    #export dữ liệu ra file
    file_name = export_table_to_csv(conn, file_info['source_file_location'], date, file_info['destination_table_staging'], file_info['source'])
    conn.close()
    # Kết nối cơ sở dữ liệu controls
    conn = connect_to_database(db_controls_config)
    #Cập nhật file log sang "LR"
    # update_status_to_lr(conn, file_info['id'], file_name)
    update_status(conn, file_info['id'], file_info['id_config'], file_info['time'])
    insert_file_log_LR(conn, file_info['id_config'], file_info['time'], file_name, file_info['count'], file_info['file_size_kb'])
    conn.close()
if __name__ == "__main__":
    main()
    
