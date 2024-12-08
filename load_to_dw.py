import sys
import os
from datetime import datetime
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2 import extras
import csv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

EMAIL = "hoangtunqs134@gmail.com"


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
    WHERE fl.id_config = %s AND fl.time::date = %s AND fl.status = 'LS'
    """

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, (id_config, date))
        results = cur.fetchall()

        if results:
            return dict(results[0])
        # Nếu không có kết quả, trả về dictionary rỗng
        return {}


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


def insert_into_temp_dw(conn, id_config, date, table_staging):
    """
    Hàm này chèn dữ liệu từ bảng staging vào bảng temp_dw với điều kiện `id_config` và `dt_load`.

    :param conn: Kết nối PostgreSQL.
    :param id_config: Giá trị `id_config` cần kiểm tra.
    :param date: Ngày `dt_load` cần kiểm tra (dạng chuỗi 'YYYY-MM-DD').
    :param table_staging: Tên bảng staging để lấy dữ liệu nguồn.
    :return: True nếu chèn thành công, False nếu có lỗi.
    """
    query = f"""
    INSERT INTO temp_dw (natural_key, sku, product_name, 
            price, brand, material, shape, dimension, origin, 
            quantity_available, product_url, id_config, dt_extract, dt_load) 
    SELECT natural_key, sku, product_name, 
            price, brand, material, shape, dimension, origin, 
            quantity_available, product_url, id_config, dt_extract, dt_load                
    FROM {table_staging}
    WHERE id_config = %s
      AND dt_load = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (id_config, date))
            conn.commit()
            print("Dữ liệu đã được chèn vào temp_dw thành công.")
            return True
    except Exception as e:
        print(f"Lỗi khi chèn dữ liệu vào temp_dw: {e}")
        conn.rollback()
        return False


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


def update_status(conn, record_id, status, id_config, time_value):
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
    `time` là ngày nhập vào và `status` là 'RUNNING' hoặc 'LS'.

    :param conn: Kết nối PostgreSQL.
    :param id_config: Giá trị `id_config` cần kiểm tra.
    :param date: Ngày cần kiểm tra (dạng chuỗi 'YYYY-MM-DD').
    :return: True nếu tồn tại bản ghi thỏa mãn, False nếu không.
    """
    query = """
    SELECT * FROM file_logs where id_config = %s and time = %s and status = 'LS'
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (id_config, date))
            result = cur.fetchone()
            if result:
                print(f"Có bản ghi thỏa mãn điều kiện trong file_logs.")
                return False
            else:
                print(f"Không có bản ghi thỏa mãn điều kiện trong file_logs.")
                return True
    except Exception as e:
        print(f"Lỗi khi kiểm tra bản ghi trong file_logs: {e}")
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
            date = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            print("Lỗi: Ngày không đúng định dạng YYYY-MM-DD.")
            sys.exit(1)
    else:
        date = datetime.today().strftime("%Y-%m-%d")

    # In thông tin
    print(f"ID Config: {id_config}")
    print(f"Path Config: {path_config}")
    print(f"Date: {date}")

    # 3.1. Load file config.xml
    db_config = load_database_config("dw", path_config)
    try:
        # 3.2. Kết nối cơ sở dữ liệu dw
        conn = connect_to_database(db_config)
    except Exception as e:
        # 3.2.1.Gửi mail thông báo kết nối csdl dw thất bại
        send_email(EMAIL, "LỖI KẾT NỐI CƠ SỞ DỮ LIỆU DW", "Lỗi phát hiện: {e}")
        sys.exit(1)
        return

    # 3.3.Kiểm tra có tiến trình đang/đã chạy hoặc không có dữ liệu sẵn sàng đưa vào dw hay không
    exists = check_file_log(conn, id_config, date)
    if exists:
        # 3.3.1.Gửi mail thông báo có tiến trình đang/đã chạy hoặc không có dữ liệu sẵn sàng đưa vào dw(status 'LS')
        send_email(
            EMAIL,
            "LỖI TRONG QUÁ TRÌNH LOAD_TO_DW: NGÀY {date} | ID CONFIG: {id_config}",
            "Lỗi phát hiện: Đã có tiến trình đang/đã chạy hoặc không có file sẵn sàng đưa vào data warehouse",
        )
    else:
        # 3.4.Lấy thông tin file log
        file_info = fetch_file_info(conn, id_config, date)
        # 3.5.Cập nhật file log sang trạng thái 'RUNNING'
        update_status(conn, file_info["id"], "RUNNING", id_config, date)
        # 3.6.Insert dữ liệu ngày tương ứng từ bảng staging sang temp_dw
        insert_into_temp_dw(
            conn, id_config, date, file_info["destination_table_staging"]
        )
        # 3.7.Insert dữ liệu mới (chưa có bên dw) từ bảng temp_dw vào dw với dt_last_update là '9999-12-31'
        insert_news_into_dw(conn, date)
        # 3.8.Cập nhật dt_last_update các record có giá trị thay đổi
        update_news_dt_last_update(conn, date)
        # 3.9.Insert các dữ liệu thay đổi từ bảng temp_dw vào dw với dt_last_udpate là '9999-12-31'
        insert_changed_into_dw(conn, date)
        # 3.10.Update cột dt_dim theo ngày của date_dim
        update_dt_dim(conn)
        # 3.11.Truncate bảng temp_dw
        truncate_table(conn, "temp_dw")
        # 3.12.Cập nhật file log sang trạng thái 'LWS'
        update_status(conn, file_info["id"], "LWS", id_config, date)
    # 3.13.Đóng kết nối
    conn.close()


if __name__ == "__main__":
    main()
