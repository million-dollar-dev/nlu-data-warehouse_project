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
        print("Kết nối cơ sở dữ liệu thành công.")
        return conn
    except Exception as e:
        print(f"Lỗi khi kết nối cơ sở dữ liệu: {e}")
        sys.exit(1)

def fetch_joined_data(conn, id_config, time=None):
    """
    Hàm thực hiện truy vấn Join hai bảng `file_config` và `file_logs`.
    
    :param conn: Kết nối PostgreSQL.
    :param time: Thời gian filter, mặc định là ngày hôm nay nếu không có.
    :return: Danh sách các bản ghi kết quả.
    """
    # Mặc định là ngày hôm nay nếu không có tham số time
    if time is None:
        time = datetime.today().strftime('%Y-%m-%d')

    query = """
    SELECT 
        fl.id_config,
        fc.source_file_location,
        fc.destination_table_staging
    FROM file_config fc
        INNER JOIN file_logs fl ON fc.id = fl.id_config
    WHERE fl.status = 'LR' AND fl.time::date = %s AND fl.id_config = %s
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (time, id_config))
            results = cur.fetchall()
            return results
    except Exception as e:
        print(f"Lỗi khi thực hiện truy vấn: {e}")
        return []

def export_table_to_csv(conn, id_config, output_dir, table_name, date):
    """
    Hàm export toàn bộ dữ liệu từ một bảng ra file CSV.
    
    :param conn: Kết nối PostgreSQL.
    :param id_config: ID Config được sử dụng để tạo tên file.
    :param output_dir: Thư mục lưu trữ file CSV.
    :param table_name: Tên bảng cần export dữ liệu.
    :param date: Ngày được sử dụng để tạo tên file.
    """
    # Đảm bảo thư mục output tồn tại
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Tạo tên file
    file_name = f"load_to_dw_{id_config}_{table_name}_{date}.csv"
    file_path = os.path.join(output_dir, file_name)
    
    # Truy vấn dữ liệu từ bảng
    query = f"SELECT * FROM {table_name}"
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
            if not rows:
                print(f"Bảng '{table_name}' không có dữ liệu.")
                return
            
            # Lấy danh sách tên cột
            column_names = [desc[0] for desc in cur.description]
            
            # Ghi dữ liệu ra file CSV
            with open(file_path, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                
                # Ghi tiêu đề cột
                writer.writerow(column_names)
                
                # Ghi từng dòng dữ liệu
                for row in rows:
                    writer.writerow(row)
            
            print(f"Dữ liệu từ bảng '{table_name}' đã được export ra file: {file_path}")
            return file_path
    except Exception as e:
        print(f"Lỗi khi export dữ liệu từ bảng '{table_name}': {e}")
        return None

def truncate_table(conn, table_name):
    """
    Hàm để truncate (xóa toàn bộ dữ liệu) từ một bảng trong cơ sở dữ liệu.
    
    :param conn: Kết nối PostgreSQL.
    :param table_name: Tên bảng cần truncate.
    :return: None
    """
    try:
        with conn.cursor() as cur:
            query = f"TRUNCATE TABLE {table_name} RESTART IDENTITY"
            cur.execute(query)
            conn.commit()
            print(f"Bảng '{table_name}' đã được truncate thành công.")
    except Exception as e:
        print(f"Lỗi khi truncate bảng '{table_name}': {e}")
        conn.rollback()


def main():
    # Kiểm tra số lượng tham số đầu vào
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
        date = datetime.today()
    
    # In thông tin
    print(f"ID Config: {id_config}")
    print(f"Path Config: {path_config}")
    print(f"Date: {date.strftime('%Y-%m-%d')}")
    
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
    results = fetch_joined_data(conn, id_config, time=date)
    if results:
        print("Kết quả truy vấn:")
        for row in results:
            print(dict(row))
    else:
        print("Không có kết quả nào phù hợp.")

    # Đóng kết nối db controls sau khi xử lý xong
    conn.close()
    print("Đã đóng kết nối cơ sở dữ liệu controls.")

    # Kết nối cơ sở dữ liệu staging
    file_path_list = []
    conn = connect_to_database(db_staging_config)
    if results:
        for row in results:
            file_path_list.append(export_table_to_csv(conn, row['id_config'], row['source_file_location'], row['destination_table_staging'], time=date))
            truncate_table(row['destination_table_staging'])
    conn.close()



if __name__ == "__main__":
    main()
