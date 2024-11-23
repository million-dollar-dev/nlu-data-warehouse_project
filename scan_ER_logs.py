import psycopg2
import xml.etree.ElementTree as ET
from datetime import datetime
import sys
from tabulate import tabulate

# Hàm đọc cấu hình từ file config.xml
def get_db_config(config_path, name):
    try:
        tree = ET.parse(config_path)
        root = tree.getroot()
        databases = root.find('databases')
        if databases is None:
            print("Không tìm thấy thẻ <databases> trong file cấu hình.")
            return None

        for db in databases.findall('database'):
            if db.get('name') == name:
                host = db.find('hostname')
                port = db.find('port')
                database = db.find('database')
                user = db.find('username')
                password = db.find('password')
                
                if None in (host, port, database, user, password):
                    print(f"Thiếu thông tin cấu hình cho database {name} trong file config.xml.")
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
def connect_to_db(config_path, name):
    config = get_db_config(config_path, name)
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

# Hàm chèn dữ liệu từ file config.xml vào bảng file_config
def insert_file_config_from_xml(conn, config_path):
    try:
        tree = ET.parse(config_path)
        root = tree.getroot()
        file_configs = root.find('file_configs')
        if file_configs is None:
            print("Không tìm thấy thẻ <file_configs> trong file cấu hình.")
            return

        with conn.cursor() as cur:
            for file_config in file_configs.findall('file_config'):
                name = file_config.find('name').text
                source = file_config.find('sources/source').text
                source_file_location = file_config.find('source_file_location').text
                destination_table_staging = file_config.find('destination_table_staging').text
                destination_table_dw = file_config.find('destination_table_dw').text

                # Kiểm tra trùng lặp
                cur.execute(
                    """
                    SELECT COUNT(*) FROM file_config
                    WHERE name = %s AND source = %s AND source_file_location = %s 
                    AND destination_table_staging = %s AND destination_table_dw = %s;
                    """,
                    (name, source, source_file_location, destination_table_staging, destination_table_dw)
                )
                if cur.fetchone()[0] == 0:
                    cur.execute(
                        """
                        INSERT INTO file_config (name, source, source_file_location, destination_table_staging, destination_table_dw)
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        (name, source, source_file_location, destination_table_staging, destination_table_dw)
                    )
                    print(f"Đã chèn cấu hình mới: {name}")
                else:
                    print(f"Bỏ qua bản ghi trùng lặp: {name}")

            conn.commit()
            print("Hoàn tất chèn dữ liệu từ file config.xml vào bảng file_config.")
    except Exception as e:
        print("Lỗi khi chèn dữ liệu từ file config.xml vào bảng file_config:", e)
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

# Hàm query các record có status time và status = 'ER'
def get_records(conn, id_config, date_str):
    query = """
    SELECT fc.config_id, fc.source_file_location, fc.destination_table_staging, fc.source_file
    FROM controls c
    JOIN file_config fc ON c.config_id = fc.config_id
    WHERE c.status_time = %s AND c.status = 'ER' AND c.id_config = %s;
    """
    cursor = conn.cursor()
    cursor.execute(query, (date_str, id_config))
    return cursor.fetchall()

# Hàm kiểm tra và tạo bảng tạm
def check_create_temp_table(conn, staging_table):
    cursor = conn.cursor()
    cursor.execute(f"""
    SELECT to_regclass('public.{staging_table}_temp');
    """)
    result = cursor.fetchone()
    if result[0] is None:
        print(f"Creating temporary table {staging_table}_temp...")
        cursor.execute(f"""
        CREATE TABLE public.{staging_table}_temp AS
        SELECT * FROM public.{staging_table} WHERE 1=0;
        """)
        conn.commit()
    else:
        print(f"Temporary table {staging_table}_temp already exists.")

# Hàm chính
def main(id_config, config_path, date_str=None):
    # Nếu không có ngày đầu vào, sử dụng ngày hôm nay
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')
        
    conn_controls = connect_to_db(config_path, 'controls')
    if conn_controls is None:
        return
    
    # Query các record từ database
    records = get_records(conn_controls, id_config, date_str)
    if not records:
        print(f"No records found for id_config {id_config} on {date_str}.")
        conn_controls.close()
        return
    
    # Kết nối với database staging
    conn_staging = connect_to_db(config_path, 'staging_8i3l')
    if conn_staging is None:
        conn_staging.close()
        return

    # Xử lý từng record
    for record in records:
        config_id, source_file_location, destination_table_staging, source_file = record
        print(f"Processing config_id: {config_id}, source file: {source_file}, destination table: {destination_table_staging}")
        
        # Kiểm tra và tạo bảng tạm
        check_create_temp_table(conn_staging, destination_table_staging)

    # Đóng kết nối
    conn_controls.close()
    conn_staging.close()

# Chạy chương trình
if __name__ == "__main__":
    # Kiểm tra số lượng tham số đầu vào
    if len(sys.argv) < 3:
        print("Usage: python process_records.py <id_config> [<date>]")
        sys.exit(1)
    
    # Lấy id_config và date từ sys.argv
    config_path = sys.argv[1]
    id_config = sys.argv[2]
    date_str = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Chạy hàm main với các tham số
    main(id_config, config_path, date_str)
