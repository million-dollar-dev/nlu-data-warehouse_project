import requests
import requests
import psycopg2
import csv
from io import StringIO
def get_authorization_token(account_id, application_key, bucket_id, file_name_prefix, valid_duration_in_seconds=3600):
    try:
        # Bước 1: Authorize tài khoản
        auth_response = requests.get(
            "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
            auth=(account_id, application_key)
        )
        auth_response.raise_for_status()
        auth_data = auth_response.json()

        # Lấy API URL và token
        api_url = auth_data['apiUrl']
        auth_token = auth_data['authorizationToken']

        # Bước 2: Lấy Download Authorization Token
        download_auth_response = requests.post(
            f"{api_url}/b2api/v3/b2_get_download_authorization",
            headers={"Authorization": auth_token},
            json={
                "bucketId": bucket_id,
                "fileNamePrefix": file_name_prefix,
                "validDurationInSeconds": valid_duration_in_seconds
            }
        )
        download_auth_response.raise_for_status()
        download_auth_data = download_auth_response.json()

        return download_auth_data['authorizationToken']
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi lấy authorization token: {e}")
        return None
    
def insert_csv_to_table(conn, auth_token, bucket_name, file_name, table_name, id_config, dt_extract, dt_load):
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
        url = f"https://f005.backblazeb2.com/file/{bucket_name}/{file_name}?Authorization={auth_token}"
        print(url)
        # Tải file CSV từ URL
        response = requests.get(url)
        response.raise_for_status()  # Kiểm tra lỗi HTTP
        
        # Chuyển nội dung CSV thành một đối tượng StringIO
        csv_content = response.content.decode('utf-8')  # Giả định encoding là UTF-8
        csv_file = StringIO(csv_content)
        print(csv_content)

        # Đọc file CSV bằng csv.reader
        reader = csv.reader(csv_file)
        headers = next(reader)  # Đọc tiêu đề cột từ dòng đầu tiên

        # Thêm các cột bổ sung
        extended_headers = headers + ["id_config", "dt_extract", "dt_load"]

        # Tạo chuỗi truy vấn SQL
        placeholders = ", ".join(["%s"] * len(extended_headers))
        query = f"""
            INSERT INTO {table_name} ({", ".join(extended_headers)})
            VALUES ({placeholders})
        """

        # Chèn dữ liệu vào bảng
        # cursor = conn.cursor()
        # for row in reader:
        #     extended_row = row + [id_config, dt_extract, dt_load]
        #     cursor.execute(query, extended_row)

        # Lưu thay đổi
        #conn.commit()
        print(f"Dữ liệu đã được chèn thành công vào bảng {table_name}.")
        
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải file CSV từ URL: {e}")
    except psycopg2.DatabaseError as e:
        print(f"Lỗi cơ sở dữ liệu: {e}")
        conn.rollback()  # Hoàn tác nếu có lỗi xảy ra
    except Exception as e:
        print(f"Lỗi khác: {e}")
        
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
    
def read_csv_file_via_url(download_url, authorization_token):
    try:
        # Gửi yêu cầu HTTP với token trong Header
        response = requests.get(
            download_url,
            headers={"Authorization": authorization_token}
        )
        response.raise_for_status()  # Kiểm tra lỗi HTTP

        # Chuyển nội dung CSV thành StringIO
        csv_content = response.content.decode("utf-8")  # Giả định encoding là UTF-8
        csv_file = StringIO(csv_content)
        
        # In nội dung để kiểm tra
        print(csv_content)

        # Đọc file CSV bằng csv.reader
        reader = csv.reader(csv_file)
        headers = next(reader)  # Đọc tiêu đề cột
        print(f"Headers: {headers}")

        # Đọc từng dòng dữ liệu
        for row in reader:
            print(row)
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi đọc file CSV: {e}")

# conn = psycopg2.connect(
#     dbname="dw_xyg1",
#     user="root",
#     password="XQqw4cZnWjM5JaAge4fMLpV8PEzIYhzH",
#     host="dpg-ct2jaspu0jms738rmkgg-a.singapore-postgres.render.com",
#     port="5432"
# )
#token = get_authorization_token('539a40c28e85', '00526180925d1c4786025f54da57b50c77496f009c', 'a5a379fab4009ce2983e0815', 'daily/data_matkinh_daily_2024-12-07_kinhmatviettin.vn.csv')
#get_download_url('539a40c28e85', '00526180925d1c4786025f54da57b50c77496f009c', 'a5a379fab4009ce2983e0815', 'daily/data_matkinh_daily_2024-12-07_kinhmatviettin.vn.csv')
#print(token)
table_name = "cors"
id_config = 123
dt_extract = "2024-12-12"
dt_load = "2024-12-12"

result = get_download_url(
    account_id="539a40c28e85",
    application_key="00526180925d1c4786025f54da57b50c77496f009c",
    bucket_id="a5a379fab4009ce2983e0815",
    bucket_name='dw-nlu-storage',
    file_name_prefix="daily/data_matkinh_daily_2024-12-08_kinhmatviettin.vn.csv",
    valid_duration_in_seconds=3600
)

if result:
    read_csv_file_via_url(result["download_url_base"], result["authorization_token"])
else:
    print("Không thể tạo URL hoặc đọc file.")

# Gọi hàm
#insert_csv_to_table(conn, token, 'dw-nlu-storage', 'daily/data_matkinh_daily_2024-12-06_kinhmatviettin.vn.csv', table_name, id_config, dt_extract, dt_load)

# Đóng kết nối sau khi hoàn tất
#conn.close()