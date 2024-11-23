import psycopg2
import csv

# Kết nối đến PostgreSQL
conn = psycopg2.connect(
    host="dpg-csoqdqt6l47c7393u110-a.singapore-postgres.render.com",
    port=5432,
    database="staging_8i3l",
    user="root",
    password="gHnOGiRGn7krJJZeZcFr1YrttM6Rphji"
)
cursor = conn.cursor()

# Đọc file CSV và chèn dữ liệu
with open("E:\\Documents\\NLU\\DataWarehouse\\project\\daily_data\\daily_data_2024-11-12_kinhmatviettin.vn.csv", mode="r", encoding="utf-8") as file:
    reader = csv.reader(file)
    next(reader)  # Bỏ qua dòng tiêu đề nếu file CSV có header

    for row in reader:
        cursor.execute(
            """
            INSERT INTO temp (
                product_name, price, brand, sku, material, shape, dimension,
                origin, quantity_available, product_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            row  # Dữ liệu từng dòng từ file CSV
        )

# Lưu thay đổi và đóng kết nối
conn.commit()
cursor.close()
conn.close()

print("Dữ liệu đã được chèn thành công vào bảng temp!")
