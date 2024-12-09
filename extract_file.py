import requests
from bs4 import BeautifulSoup
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

EMAIL = os.getenv("MY_EMAIL_DW_VAR")


# Hàm lấy danh sách link sản phẩm từ trang danh mục
def get_product_links(page_url):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Tìm tất cả liên kết sản phẩm trong trang danh mục
    product_links = []
    for link in soup.find_all("a", class_="ps-product__title"):
        product_url = link.get("href")
        if product_url:
            product_links.append(f"{product_url}")

    return product_links


# Hàm lấy thông tin chi tiết sản phẩm từ trang chi tiết
def get_product_details(product_url):
    response = requests.get(product_url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Trích xuất thông tin chi tiết sản phẩm
    product_name = soup.find("h1").text.strip() if soup.find("h1") else None

    # Xử lý giá để loại bỏ phần "/ 1 chiếc"
    price_text = (
        soup.find("h4", class_="ps-product__price").text.strip().split("/")[0]
        if soup.find("h4", class_="ps-product__price")
        else None
    )
    price = re.sub(r"[₫,]", "", price_text) if price_text else None
    brand = (
        soup.find("a", href=lambda href: href and "brands" in href).text.strip()
        if soup.find("a", href=lambda href: href and "brands" in href)
        else None
    )

    # Các thuộc tính bổ sung từ phần mô tả chi tiết
    description_items = soup.find("div", class_="ps-product__desc")
    sku = material = shape = dimension = origin = None
    if description_items:
        desc_text = format_description_text(description_items.text.strip())
        # Tìm các thông tin cụ thể theo từ khóa
        if "Mã sản phẩm" in desc_text:
            sku = desc_text.split("Mã sản phẩm:")[1].split("•")[0].strip()
        if "Chất liệu" in desc_text:
            material = desc_text.split("Chất liệu:")[1].split("•")[0].strip()
        if "Hình dạng" in desc_text:
            shape = desc_text.split("Hình dạng:")[1].split("•")[0].strip()
        if "Thông số" in desc_text:
            dimension = desc_text.split("Thông số:")[1].split("•")[0].strip()

        # Xử lý xuất xứ chỉ lấy tên quốc gia
        if "Xuất xứ" in desc_text:
            origin = desc_text.split("Xuất xứ:")[1].split("•")[0].split()[0].strip()
    # Lấy số lượng sản phẩm có sẵn, chỉ giữ lại số
    quantity = soup.find("div", class_="number-items-available")
    quantity_available = (
        "".join(filter(str.isdigit, quantity.text)) if quantity else None
    )
    if not quantity_available:
        quantity_available = "0"
    return {
        "sku": sku,
        "product_name": product_name,
        "price": price,
        "brand": brand,
        "material": material,
        "shape": shape,
        "dimension": dimension,
        "origin": origin,
        "quantity_available": quantity_available,
        "product_url": product_url,
    }


# Hàm chính để duyệt qua các trang danh mục và cào dữ liệu tất cả sản phẩm
def scrape_all_products_to_csv(source_file_location, name):
    all_products = []
    base_url = "https://kinhmatviettin.vn/product-categories/gong-kinh?pages="
    total_pages = 3  # Số trang cần duyệt
    # Lấy ngày hiện tại để tạo tên file
    current_date = datetime.now().strftime("%Y-%m-%d")
    # Lấy phần domain của base_url cho tên file
    domain_name = base_url.split("//")[1].split("/")[0]
    # Tạo tên file theo format yêu cầu
    csv_filename = f"data_{name}_{current_date}_{domain_name}.csv"

    # Đảm bảo rằng thư mục lưu file tồn tại, nếu không tạo mới
    if not os.path.exists(source_file_location):
        os.makedirs(source_file_location)

    # Tạo đường dẫn đầy đủ cho file CSV
    csv_filepath = os.path.join(source_file_location, csv_filename)

    # Lặp qua từng trang danh mục
    for page in range(2, total_pages + 1):
        page_url = f"{base_url}{page}"
        print(f"Lấy thông tin sản phẩm từ trang: {page_url}")

        product_links = get_product_links(page_url)
        # Lặp qua từng link sản phẩm để lấy thông tin chi tiết
        # for product_url in product_links[:10]:
        for product_url in product_links:
            print(f"Lấy thông tin sản phẩm từ: {product_url}")
            product_details = get_product_details(product_url)
            all_products.append(product_details)

    # Lưu dữ liệu vào file CSV
    df = pd.DataFrame(all_products)
    df.to_csv(csv_filepath, index=False, encoding="utf-8")
    print(f"Dữ liệu được lưu vào {csv_filepath}")
    print(f"Tổng số dữ liệu: {len(all_products)}")

    # Trả về đường dẫn đầy đủ của file đã lưu
    return csv_filename


def format_description_text(text):
    # Biểu thức chính quy kiểm tra và thêm "•" trước "Thông tinNK và PP:" nếu chưa có
    pattern = r"(•\s*)?Thông tin"
    formatted_text = re.sub(pattern, r"• Thông tin", text)
    return formatted_text


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
        print("Kết nối cơ sở dữ liệu thành công.")
        return conn
    except Exception as e:
        print(f"Lỗi khi kết nối cơ sở dữ liệu: {e}")
        sys.exit(1)


def fetch_file_config_by_id(conn, id_config):
    """
    Hàm lấy một bản ghi từ bảng `file_config` dựa vào id_config.

    :param conn: Kết nối PostgreSQL.
    :param id_config: ID Config cần truy vấn.
    :return: Bản ghi dưới dạng dictionary hoặc None nếu không tìm thấy.
    """
    query = """
    SELECT *
    FROM file_config
    WHERE id = %s
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (id_config,))
            record = cur.fetchone()
            if record:
                return dict(record)  # Chuyển kết quả thành dictionary
            else:
                print(f"Không tìm thấy bản ghi nào với id_config = {id_config}.")
                return None
    except Exception as e:
        print(f"Lỗi khi thực hiện truy vấn: {e}")
        return None


def get_csv_file_info(folder_path, file_name):
    """
    Hàm lấy thông tin của một file .csv.

    :param folder_path: Đường dẫn thư mục chứa file.
    :param file_name: Tên file .csv.
    :return: Dictionary chứa số dòng, dung lượng file (KB), và thời gian tạo file.
    """
    # Kết hợp đường dẫn thư mục và tên file
    file_path = os.path.join(folder_path, file_name)

    # Kiểm tra file có tồn tại không
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File '{file_path}' không tồn tại.")

    try:
        # Lấy dung lượng file (KB)
        file_size_bytes = os.path.getsize(file_path)
        file_size_kb = file_size_bytes / 1024  # Chuyển đổi sang KB

        # Lấy thời gian tạo file
        creation_time = os.path.getctime(file_path)
        creation_time_str = datetime.fromtimestamp(creation_time).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # Đếm số dòng trong file .csv
        with open(file_path, mode="r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            line_count = sum(1 for _ in reader)  # Đếm số dòng

        return {
            "line_count": line_count - 1,
            "file_size_kb": round(file_size_kb, 2),  # Làm tròn 2 chữ số thập phân
            "creation_time": creation_time_str,
        }
    except Exception as e:
        print(f"Lỗi khi xử lý file '{file_path}': {e}")
        return None


def insert_file_log(
    conn, id_config, status, file_name, time, count, file_size_kb, dt_update
):
    """
    Hàm chèn một bản ghi vào bảng `file_logs`.

    :param conn: Kết nối PostgreSQL.
    :param id_config: ID Config.
    :param file_name: Tên file.
    :param time: Thời gian.
    :param status: Trạng thái (e.g., 'ER').
    :param count: Số dòng.
    :param file_size_kb: Kích thước file (KB).
    :param dt_update: Thời gian cập nhật.
    :return: None
    """
    query = """
    INSERT INTO file_logs (id_config, file_name, time, status, count, file_size_kb, dt_update)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (id_config, file_name, time, status, count, file_size_kb, dt_update),
            )
            inserted_id = cur.fetchone()[0]
            conn.commit()
            return inserted_id
    except Exception as e:
        print(f"Lỗi khi chèn bản ghi vào file_logs: {e}")
        conn.rollback()
        return None


def update_file_log(conn, id, status, file_name, count, file_size_kb, dt_update):
    """
    Hàm cập nhật một bản ghi trong bảng `file_logs` dựa trên ID.

    :param conn: Kết nối PostgreSQL.
    :param id: ID của bản ghi cần cập nhật.
    :param status: Trạng thái mới.
    :param file_name: Tên file mới.
    :param count: Số dòng mới.
    :param file_size_kb: Kích thước file mới (KB).
    :param dt_update: Thời gian cập nhật mới.
    :return: True nếu cập nhật thành công, False nếu có lỗi xảy ra.
    """
    query = """
    UPDATE file_logs
    SET status = %s, file_name = %s, count = %s, file_size_kb = %s, dt_update = %s
    WHERE id = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (status, file_name, count, file_size_kb, dt_update, id))
            conn.commit()
            print(f"Cập nhật bản ghi ID {id} thành công.")
            return True
    except Exception as e:
        print(f"Lỗi khi cập nhật bản ghi ID {id}: {e}")
        conn.rollback()
        return False


def check_file_log(conn, id_config, date):
    """
    Hàm kiểm tra trong bảng `file_logs` có bản ghi nào có `id_config` là id_config nhập vào,
    `time` là ngày nhập vào và `status` là 'Scapping' hoặc 'ES'.

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
      AND status IN ('RUNNING', 'ES')
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


def upload_csv_to_b2(config_file, bucket_name, folder_name, csv_file_path):
    """
    Upload a CSV file to a folder in a bucket on Backblaze B2.

    Args:
        config_file (str): Path to the XML configuration file.
        bucket_name (str): Name of the B2 bucket.
        folder_name (str): Folder path within the bucket where the file will be uploaded.
        csv_file_path (str): Path to the CSV file to upload.

    Returns:
        str: The URL of the uploaded file.
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

    # Construct the full file name (folder + base file name)
    file_name = folder_name + os.path.basename(csv_file_path)

    # Upload the file
    bucket.upload_local_file(
        local_file=csv_file_path,
        file_name=file_name,
        file_infos={"id_config": "1"},
    )


def main():
    # Kiểm tra số lượng tham số đầu vào
    if len(sys.argv) < 2:
        print("Vui lòng nhập 2 tham số: id_config, path_config")
        print("Cú pháp: python script.py <id_config> <path_config>")
        sys.exit(1)

    # Nhận tham số đầu vào
    id_config = sys.argv[1]
    path_config = sys.argv[2]
    date = datetime.today().strftime("%Y-%m-%d")

    # In thông tin
    print(f"ID Config: {id_config}")
    print(f"Path Config: {path_config}")
    print(f"Date: {date}")

    # 1.1. Load thông tin kết nối từ file config
    db_config = load_database_config("dw", path_config)
    try:
        # 1.2. Kết nối cơ sở dữ liệu controls
        conn = connect_to_database(db_config)
    except Exception as e:
        # 1.2.1. Gửi mail thông lỗi kết nối csdl dw
        send_email(EMAIL, f"LỖI KẾT NỐI CƠ SỞ DỮ LIỆU DW {date}", f"Lỗi phát hiện: {e}")
        sys.exit(1)
        return

    # 1.3. Kiểm tra file logs có tiến trình đã chạy hoặc đang chạy hay không
    exists = check_file_log(conn, id_config, date)
    if exists:
        # 1.3.1. Gửi mail thông báo có tiến trình đã/đang chạy
        send_email(
            EMAIL,
            f"LỖI TRONG QUÁ TRÌNH EXTRACT_FILE: NGÀY {date} | ID CONFIG: {id_config}",
            f"Lỗi phát hiện: Đã có tiến trình đang/đã chạy",
        )
    else:
        # 1.4.Lấy thông tin file config
        file_config = fetch_file_config_by_id(conn, id_config)
        # 1.5.Insert 1 dòng vào file log có status RUNNING
        inserted_id = insert_file_log(
            conn=conn,
            id_config=id_config,
            status="RUNNING",
            file_name=None,
            time=date,
            count=None,
            file_size_kb=None,
            dt_update=None,
        )
        try:
            # 1.6.Tiến hành cào dữ liệu
            file_name = scrape_all_products_to_csv(
                file_config["source_file_location"],
                file_config["destination_table_staging"],
            )
            # 1.7.Lấy thông tin file vào cào về
            info_file_csv = get_csv_file_info(
                file_config["source_file_location"], file_name
            )
            # 1.8.Upload file vừa cào về lên Backblaze B2
            upload_csv_to_b2(
                path_config,
                file_config["bucket_name"],
                file_config["folder_b2_name"],
                file_config["source_file_location"] + "\\" + file_name,
            )
            # 1.9.Cập nhật file log sang trạng thái ES
            update_file_log(
                conn,
                inserted_id,
                "ES",
                file_name,
                info_file_csv["line_count"],
                info_file_csv["file_size_kb"],
                info_file_csv["creation_time"],
            )
        except Exception as e:
            # 1.6.1.Cập nhật file log sang trạng thái EF
            update_file_log(
                conn=conn,
                id=inserted_id,
                status="EF",
                file_name=None,
                count=None,
                file_size_kb=None,
                dt_update=None,
            )
            # 1.6.2.Gửi mail thông báo cào thất bại
            send_email(
                EMAIL,
                f"LỖI KẾT TRONG QUÁ TRÌNH CÀO DỮ LIỆU: NGÀY {date} | ID CONFIG: {id_config}",
                f"Lỗi phát hiện: {e}",
            )
    # 1.10. Đóng kết nối csdl
    conn.close()


if __name__ == "__main__":
    main()
