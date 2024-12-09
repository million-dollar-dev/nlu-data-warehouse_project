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
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from io import StringIO

EMAIL = os.getenv("MY_EMAIL_DW_VAR")


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
    try:
        conn = psycopg2.connect(
            host=db_config["hostname"],
            port=db_config["port"],
            database=db_config["database"],
            user=db_config["username"],
            password=db_config["password"],
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
        fc.destination_table_staging,
        fc.bucket_name,
        fc.folder_b2_name,
        fc.bucket_id
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


def insert_csv_to_table(
    conn, url, bucket_name, file_name, table_name, id_config, dt_extract, dt_load
):
    """
    Đọc file CSV từ URL và chèn dữ liệu vào bảng PostgreSQL.

    Args:
        conn: Kết nối đến PostgreSQL (psycopg2 connection).
        url: Đường dẫn URL của file CSV.
        table_name: Tên bảng trong cơ sở dữ liệu.
        id_config: ID cấu hình cần thêm vào mỗi dòng.
        dt_extract: Thời điểm extract dữ liệu.
        dt_load: Thời điểm load dữ liệu.

    Returns:
        None
    """
    try:
        response = requests.get(
            url["download_url_base"],
            headers={"Authorization": url["authorization_token"]}
        )
        response.raise_for_status()  # Kiểm tra lỗi HTTP
        print(url)
        # Chuyển nội dung CSV thành StringIO
        csv_content = response.content.decode("utf-8")  # Giả định encoding là UTF-8
        csv_file = StringIO(csv_content)

        # Đọc file CSV bằng csv.reader
        reader = csv.reader(csv_file)
        headers = next(reader)

        # Kiểm tra cột product_name và sku
        if "product_name" not in headers or "sku" not in headers:
            raise ValueError("CSV thiếu cột 'product_name' hoặc 'sku'.")

        # Thêm các cột bổ sung
        extended_headers = headers + ["natural_key", "id_config", "dt_extract", "dt_load"]

        # Tạo chuỗi truy vấn SQL
        placeholders = ", ".join(["%s"] * len(extended_headers))
        query = f"""
            INSERT INTO {table_name} ({", ".join(extended_headers)})
            VALUES ({placeholders})
        """

        # Chèn dữ liệu vào bảng
        cursor = conn.cursor()
        for row in reader:
            # Tìm vị trí của product_name và sku
            product_name_index = headers.index("product_name")
            sku_index = headers.index("sku")
            
            # Tạo giá trị natural_key
            natural_key = f"{row[product_name_index]}-{row[sku_index]}"
            
            # Mở rộng hàng với giá trị mới
            extended_row = row + [natural_key, id_config, dt_extract, dt_load]
            cursor.execute(query, extended_row)

        # Lưu thay đổi
        conn.commit()
        print(f"Dữ liệu đã được chèn thành công vào bảng {table_name}.")

    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải file CSV từ URL: {e}")
    except psycopg2.DatabaseError as e:
        print(f"Lỗi cơ sở dữ liệu: {e}")
        conn.rollback()  # Hoàn tác nếu có lỗi xảy ra
    except Exception as e:
        print(f"Lỗi khác: {e}")


def transform_data(conn, table_name):
    """
    Hàm thực hiện hai chức năng trên bảng:
    1. Thay thế tất cả các giá trị NULL trong bảng thành:
       - 'N/A' cho các cột kiểu chuỗi.
       - -1 cho các cột kiểu số.
    2. Loại bỏ các dòng trùng lặp dựa trên natural_key, chỉ giữ lại một dòng.

    :param conn: Kết nối PostgreSQL.
    :param table_name: Tên bảng cần xử lý.
    :param natural_key: Cột dùng để xác định trùng lặp.
    """
    # Câu truy vấn thay thế NULL
    query_replace_null = f"""
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
    
    # Câu truy vấn loại bỏ trùng lặp
    query_remove_duplicates = f"""
    DELETE FROM {table_name}
    WHERE ctid NOT IN (
        SELECT MIN(ctid)
        FROM {table_name}
        GROUP BY natural_key
    );
    """

    try:
        with conn.cursor() as cur:
            # Bước 1: Thay thế giá trị NULL
            print(f"Đang thay thế giá trị NULL trong bảng '{table_name}'...")
            cur.execute(query_replace_null)
            print(f"Hoàn tất thay thế giá trị NULL trong bảng '{table_name}'.")

            # Bước 2: Loại bỏ dòng trùng lặp
            print(f"Đang loại bỏ các dòng trùng lặp trong bảng '{table_name}'...")
            cur.execute(query_remove_duplicates)
            print(f"Hoàn tất loại bỏ dòng trùng lặp trong bảng '{table_name}'.")

            # Ghi nhận thay đổi
            conn.commit()
    except Exception as e:
        print(f"Lỗi khi xử lý bảng '{table_name}': {e}")
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
    `time` là ngày nhập vào và `status` là 'RUNNING' hoặc 'LS'.

    :param conn: Kết nối PostgreSQL.
    :param id_config: Giá trị `id_config` cần kiểm tra.
    :param date: Ngày cần kiểm tra (dạng chuỗi 'YYYY-MM-DD').
    :return: True nếu tồn tại bản ghi thỏa mãn, False nếu không.
    """
    query = """
    SELECT * FROM file_logs where id_config = %s and time = %s and status = 'ES'
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


def check_csv_existed_in_b2(config_file, bucket_name, folder_name, csv_file_name):
    """
    Check if a CSV file exists in a specific folder within a Backblaze B2 bucket.

    Args:
        config_file (str): Path to the XML configuration file.
        bucket_name (str): Name of the B2 bucket.
        folder_name (str): Folder path within the bucket to check.
        csv_file_name (str): Name of the CSV file to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    # Parse the config file to extract Backblaze credentials
    tree = ET.parse(config_file)
    root = tree.getroot()

    # Extract credentials from the XML
    key_id = root.find("./backblaze/key_id").text
    application_key = root.find("./backblaze/application_key").text

    # Initialize B2 API
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)

    # Authorize with B2
    b2_api.authorize_account("production", key_id, application_key)

    # Get the bucket
    bucket = b2_api.get_bucket_by_name(bucket_name)

    # Ensure folder name ends with a slash
    if not folder_name.endswith("/"):
        folder_name += "/"

    # Construct the full file path
    file_path = folder_name + csv_file_name

    # Check if the file exists in the bucket
    try:
        bucket.get_file_info_by_name(file_path)
        return True
    except Exception:
        return False


def get_download_url(account_id, application_key, bucket_id, bucket_name, file_name_prefix, valid_duration_in_seconds=3600):
    try:
        # Bước 1: Authorize tài khoản
        auth_response = requests.get(
            "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
            auth=(account_id, application_key)
        )
        auth_response.raise_for_status()
        auth_data = auth_response.json()

        # Lấy API URL và token
        download_url_base = auth_data['downloadUrl']
        auth_token = auth_data['authorizationToken']

        # Bước 2: Lấy Download Authorization Token
        download_auth_response = requests.post(
            f"{auth_data['apiUrl']}/b2api/v2/b2_get_download_authorization",
            headers={"Authorization": auth_token},
            json={
                "bucketId": bucket_id,
                "fileNamePrefix": file_name_prefix,
                "validDurationInSeconds": valid_duration_in_seconds
            }
        )
        download_auth_response.raise_for_status()
        download_auth_data = download_auth_response.json()

        # Trả về URL cơ sở và token để dùng trong Header
        return {
            "download_url_base": f"{download_url_base}/file/{bucket_name}/{file_name_prefix}",
            "authorization_token": download_auth_data['authorizationToken']
        }
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi lấy URL tải xuống: {e}")
        return None


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


def insert_to_table_from_b2(
    conn,
    config_file,
    bucket_id,
    bucket_name,
    folder_name,
    file_name,
    table_name,
    id_config,
    dt_extract,
    dt_load,
):
    """
    Download a CSV file from a specific folder in a Backblaze B2 bucket to a local directory.

    Args:
        config_file (str): Path to the XML configuration file.
        bucket_name (str): Name of the B2 bucket.
        folder_name (str): Folder path within the bucket where the file is located.
        csv_file_name (str): Name of the CSV file to download.
        download_directory (str): Local directory to save the downloaded file.

    Returns:
        str: Path to the downloaded file if successful.
    """
    # Parse the config file to extract Backblaze credentials
    tree = ET.parse(config_file)
    root = tree.getroot()

    key_id = root.find("./backblaze/key_id").text
    application_key = root.find("./backblaze/application_key").text

    # Initialize B2 API
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)

    # Authorize with B2
    b2_api.authorize_account("production", key_id, application_key)

    url = get_download_url(
        key_id, application_key, bucket_id,bucket_name, folder_name + "/" + file_name
    )
    insert_csv_to_table(
        conn, url, bucket_name, file_name, table_name, id_config, dt_extract, dt_load
    )


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

    # 2.1. Load file config.xml
    db_config = load_database_config("dw", path_config)
    try:
        # 2.2.Kết nối cơ sở dữ liệu dw
        conn = connect_to_database(db_config)
    except Exception as e:
        # 2.2.1.Gửi mail thông báo kết nối csdl dw thất bại
        send_email(EMAIL, f"LỖI KẾT NỐI CƠ SỞ DỮ LIỆU DW NGÀY {date}", f"Lỗi phát hiện: {e}")
        sys.exit(1)
        return

    # 2.3.Kiểm tra file log có tiến trình đang hoặc đã chạy hoặc không có file nào có trạng thái ES chưa
    exists = check_file_log(conn, id_config, date)
    if exists:
        # 2.3.1.Gửi mail thông báo có tiến trình đã/đang chạy hoặc không có file nào có status ES(sẵn sàng load)
        send_email(
            EMAIL,
            f"LỖI TRONG QUÁ TRÌNH LOAD_TO_STAGING: NGÀY {date} | ID CONFIG: {id_config}",
            f"Lỗi phát hiện: Đã có tiến trình đang/đã chạy hoặc không có file sẵn sàng đưa vào staging",
        )
    else:
        # 2.4.Lấy thông tin file config
        file_info = fetch_file_info(conn, id_config, date)
        print(f"data: {file_info}")
        # 2.5.Kiểm tra có tồn tại file .csv trên B2 theo thông tin của file config hay không
        if not check_csv_existed_in_b2(
            path_config,
            file_info["bucket_name"],
            file_info["folder_b2_name"],
            file_info["file_name"],
        ):
            # 2.5.1.Gửi mail thông báo không tồn tại file theo file log
            send_email(
                EMAIL,
                f"LỖI KẾT TRONG QUÁ TRÌNH LOAD_TO_STAGING: NGÀY {date} | ID CONFIG: {id_config}",
                f"Lỗi phát hiện: Không tìm thấy file {file_info['file_name']} trên Bucket {file_info['bucket_name']}",
            )
        else:
            # 2.6.Update file log sang status 'RUNNING'
            update_status(
                conn, file_info["id"], id_config, file_info["time"], "RUNNING"
            )
            # 2.7. Insert từ file .csv  trên B2 vào bảng staging tương ứng theo file log
            insert_to_table_from_b2(
                conn,
                path_config,
                file_info["bucket_id"],
                file_info["bucket_name"],
                file_info["folder_b2_name"],
                file_info["file_name"],
                file_info["destination_table_staging"],
                id_config,
                file_info["time"],
                date,
            )
            # 2.8. Tiến hành transform các cột còn thiếu thành N/A và bỏ các dòng trùng
            transform_data(conn, file_info["destination_table_staging"])
            # 2.9. Update file log sang status là LS
            update_status(conn, file_info["id"], id_config, file_info["time"], "LS")
    # 2.10. Đóng kết nối csdl
    conn.close()


if __name__ == "__main__":
    main()
