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
        fc.destination_table_staging,
        fc.destination_table_dw
    FROM file_logs fl
        INNER JOIN file_config fc ON fl.id_config = fc.id
    WHERE fl.id_config = %s AND fl.time::date = %s AND fl.status = 'LR'
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

def export_table_to_csv(conn, folder_path, date, table_name, source, status):
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
    file_name = f"{status}_{table_name}_{date}_{domain_name}.csv"
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

def insert_csv_to_table_temp(conn, csv_file_path):
    """
    Chèn dữ liệu từ file CSV vào bảng `temp_dw`.

    :param conn: Kết nối cơ sở dữ liệu PostgreSQL.
    :param csv_file_path: Đường dẫn đầy đủ tới file .csv.
    """
    try:
        # Tạo con trỏ
        cursor = conn.cursor()

        # Đọc file CSV
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile)

            # Lấy tiêu đề (header) của file CSV
            headers = next(csvreader)

            # Tạo câu lệnh SQL dựa trên tiêu đề
            placeholders = ", ".join(["%s"] * len(headers))  # Tạo các placeholder như %s, %s, ...
            insert_query = f"INSERT INTO temp_dw ({', '.join(headers)}) VALUES ({placeholders})"

            # Chèn từng dòng dữ liệu vào bảng
            for row in csvreader:
                cursor.execute(insert_query, row)

        # Xác nhận thay đổi
        conn.commit()
        print("Data inserted successfully from CSV into temp_dw.")

    except psycopg2.Error as e:
        print(f"An error occurred while inserting data: {e}")
        conn.rollback()  # Hoàn tác nếu có lỗi xảy ra
    except FileNotFoundError:
        print(f"File {csv_file_path} not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Đóng con trỏ
        if cursor:
            cursor.close()

def insert_news_into_dw(conn, dt_load_to_dw):
    """
    Chèn dữ liệu mới từ bảng `temp_dw` vào bảng `dw` với điều kiện và giá trị `dt_load_to_dw` được truyền vào.

    :param conn: Kết nối cơ sở dữ liệu PostgreSQL.
    :param dt_load_to_dw: Ngày (chuỗi) để sử dụng làm giá trị cho cột `dt_load_to_dw`.
    """
    try:
        # Tạo con trỏ
        cursor = conn.cursor()

        # Câu lệnh SQL
        insert_query = f"""
        INSERT INTO dw (
            natural_key,
            sku,
            product_name,
            price,
            brand,
            material,
            shape,
            dimension,
            origin,
            quantity_available,
            product_url,
            id_config,
            dt_extract,
            dt_load,
            dt_load_to_dw,
            dt_last_update
        )
        SELECT 
            natural_key,
            sku,
            product_name,
            price,
            brand,
            material,
            shape,
            dimension,
            origin,
            quantity_available,
            product_url,
            id_config,
            dt_extract,
            dt_load,
            %s AS dt_load_to_dw,
            '9999-12-31' AS dt_last_update
        FROM temp_dw t
        WHERE t.natural_key NOT IN (SELECT d.natural_key FROM dw d);
        """

        # Thực thi câu lệnh SQL
        cursor.execute(insert_query, (dt_load_to_dw,))

        # Xác nhận thay đổi
        conn.commit()
        print(f"Insert into dw successful with dt_load_to_dw = {dt_load_to_dw}.")

    except psycopg2.Error as e:
        print(f"An error occurred during insertion: {e}")
        conn.rollback()  # Hoàn tác nếu có lỗi xảy ra
    finally:
        # Đóng con trỏ
        if cursor:
            cursor.close()

def insert_changed_into_dw(conn, dt_load_to_dw):
    """
    Chèn dữ liệu mới từ bảng `temp_dw` vào bảng `dw` với điều kiện và giá trị `dt_load_to_dw` được truyền vào.

    :param conn: Kết nối cơ sở dữ liệu PostgreSQL.
    :param dt_load_to_dw: Ngày (chuỗi) để sử dụng làm giá trị cho cột `dt_load_to_dw`.
    """
    try:
        # Tạo con trỏ
        cursor = conn.cursor()

        # Câu lệnh SQL
        insert_query = f"""
        INSERT INTO dw (
    natural_key,
    sku,
    product_name,
    price,
    brand,
    material,
    shape,
    dimension,
    origin,
    quantity_available,
    product_url,
    id_config,
    dt_extract,
    dt_load,
    dt_load_to_dw,
    dt_last_update
)
SELECT 
    t.natural_key,
    t.sku,
    t.product_name,
    t.price,
    t.brand,
    t.material,
    t.shape,
    t.dimension,
    t.origin,
    t.quantity_available,
    t.product_url,
    t.id_config,
    t.dt_extract,
    t.dt_load,
    %s AS dt_load_to_dw,
    '9999-12-31' AS dt_last_update
FROM temp_dw t
JOIN dw d ON t.natural_key = d.natural_key
WHERE 
    d.sku <> t.sku OR
    d.product_name <> t.product_name OR
    d.price <> t.price OR
    d.brand <> t.brand OR
    d.material <> t.material OR
    d.shape <> t.shape OR
    d.dimension <> t.dimension OR
    d.origin <> t.origin OR
    d.quantity_available <> t.quantity_available OR
    d.product_url <> t.product_url;

        """

        # Thực thi câu lệnh SQL
        cursor.execute(insert_query, (dt_load_to_dw,))

        # Xác nhận thay đổi
        conn.commit()
        print(f"Insert into dw successful with dt_load_to_dw = {dt_load_to_dw}.")

    except psycopg2.Error as e:
        print(f"An error occurred during insertion: {e}")
        conn.rollback()  # Hoàn tác nếu có lỗi xảy ra
    finally:
        # Đóng con trỏ
        if cursor:
            cursor.close()

def update_news_dt_last_update(conn, date):
    """
    Cập nhật cột dt_last_update trong bảng dw với giá trị ngày được cung cấp.
    
    :param conn: Kết nối cơ sở dữ liệu (psycopg2 connection object).
    :param date_param: Giá trị ngày cần cập nhật (str hoặc datetime, format 'YYYY-MM-DD').
    """
    try:
        # Tạo cursor để thực hiện truy vấn
        cursor = conn.cursor()
        
        # Câu lệnh SQL UPDATE
        update_query = """
        UPDATE dw
        SET dt_last_update = %s
        FROM temp_dw t
        WHERE dw.natural_key = t.natural_key
          AND dw.dt_last_update = '9999-12-31'
          AND (
            dw.sku <> t.sku OR
            dw.product_name <> t.product_name OR
            dw.price <> t.price OR
            dw.brand <> t.brand OR
            dw.material <> t.material OR
            dw.shape <> t.shape OR
            dw.dimension <> t.dimension OR
            dw.origin <> t.origin OR
            dw.quantity_available <> t.quantity_available OR
            dw.product_url <> t.product_url
          );
        """
        
        # Thực thi câu truy vấn với tham số ngày
        cursor.execute(update_query, (date,))
        
        # Commit thay đổi
        conn.commit()
        
        # In kết quả thành công
        print(f"Update completed. dt_last_update set to {date}.")
    
    except Exception as e:
        # In thông báo lỗi nếu có
        print(f"Error occurred: {e}")
        # Rollback thay đổi nếu lỗi xảy ra
        conn.rollback()
    
    finally:
        # Đóng cursor
        cursor.close()

def update_dt_dim(conn):
    """
    Cập nhật giá trị cột dt_dim trong bảng dw với giá trị id từ bảng date_dim
    khi dt_extract trong bảng dw khớp với full_date trong bảng date_dim.
    
    :param conn: Kết nối cơ sở dữ liệu (psycopg2 connection object).
    """
    try:
        # Tạo cursor để thực thi câu truy vấn
        cursor = conn.cursor()
        
        # Câu lệnh SQL UPDATE
        update_query = """
        UPDATE dw
        SET dt_dim = date_dim.id
        FROM date_dim
        WHERE dw.dt_extract = date_dim.full_date;
        """
        
        # Thực thi câu lệnh SQL
        cursor.execute(update_query)
        
        # Commit thay đổi vào cơ sở dữ liệu
        conn.commit()
        
        # In thông báo thành công
        print("Cập nhật dt_dim thành công.")
    
    except Exception as e:
        # Xử lý lỗi nếu có và rollback giao dịch
        print(f"Lỗi khi cập nhật: {e}")
        conn.rollback()
    
    finally:
        # Đảm bảo đóng cursor sau khi thực thi
        cursor.close()

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
        SET status = 'LS', time = %s
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


def insert_file_log_LDMR(conn, id_config, time, file_name, count, file_size_kb):
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
        VALUES (%s, %s, %s, 'LDMR', %s, %s, %s);
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
    
    # 3.1. Load thông tin kết nối từ file config
    try:
        db_controls_config = load_database_config("controls", path_config)
        db_staging_config = load_database_config("staging", path_config)
        db_dw_config = load_database_config("dw", path_config)
    except Exception as e:
        print(f"Lỗi: {e}")
        sys.exit(1)
    
    # 3.2. Kết nối cơ sở dữ liệu controls
    conn = connect_to_database(db_controls_config)
    
    # 3.3. Truy vấn lấy thông file với điều kiện là id_config, ngày nhập vào và status là LR
    file_info = fetch_file_info(conn, id_config, date)
    conn.close()

    # 3.4. Kết nối cơ sở dữ liệu staging
    conn = connect_to_database(db_staging_config)
    # 3.5. Export ra file csv dựa trên thông tin file vừa lấy
    file_name = export_table_to_csv(conn, file_info['source_file_location'], file_info['time'], file_info['destination_table_staging'], file_info['source'], 'l')
    # Truncate bảng staging tương ứng
    truncate_table(conn, file_info['destination_table_staging'])
    conn.close()

    # 3.6. Kết nối cơ sở dữ liệu dw
    conn = connect_to_database(db_dw_config)
    # 3.7. Insert file .csv vừa lấy vào bảng temp_dw
    insert_csv_to_table_temp(conn, file_info['source_file_location'] + "\\" + file_name)
    # 3.8. Insert dữ liệu mới (chưa có bên dw) từ bảng temp_dw vào dw
    insert_news_into_dw(conn, date)
    # 3.9. Cập nhật dt_last_update các record có giá trị thay đổi
    update_news_dt_last_update(conn, date)
    # 3.10. Insert các dữ liệu thay đổi từ bảng temp_dw vào dw với dt_last_udpate là '9999-12-31'
    insert_changed_into_dw(conn, date)
    # 3.11. Update cột dt_dim theo ngày của date_dim
    update_dt_dim(conn)
    # 3.12. Truncate bảng temp_dw
    #truncate_table(conn, 'temp_dw')
    # 3.13. export table dw ra file .csv
    file_name_dw = export_table_to_csv(conn, file_info['source_file_location'], file_info['time'], file_info['destination_table_dw'], file_info['source'], 'ldmr')
    conn.close()
    # 3.14. Kết nối cơ sở dữ liệu controls
    conn = connect_to_database(db_controls_config)
    # 3.15. Cập nhật file log sang status LS
    update_status(conn, file_info['id'], file_info['id_config'], file_info['time'])
    # 3.16. Ghi log mới với status LDMR
    insert_file_log_LDMR(conn, file_info['id_config'], file_info['time'], file_name_dw, file_info['count'], file_info['file_size_kb'])
    conn.close()


if __name__ == "__main__":
    main()
