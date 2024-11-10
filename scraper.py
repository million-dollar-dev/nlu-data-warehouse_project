import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
# Hàm lấy danh sách link sản phẩm từ trang danh mục
def get_product_links(page_url):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Tìm tất cả liên kết sản phẩm trong trang danh mục
    product_links = []
    for link in soup.find_all('a', class_='ps-product__title'):
        product_url = link.get('href')
        if product_url:
            product_links.append(f"{product_url}")
    
    return product_links

# Hàm lấy thông tin chi tiết sản phẩm từ trang chi tiết
def get_product_details(product_url):
    response = requests.get(product_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Trích xuất thông tin chi tiết sản phẩm
    product_name = soup.find('h1').text.strip() if soup.find('h1') else None
    
    # Xử lý giá để loại bỏ phần "/ 1 chiếc"
    price_text = soup.find('h4', class_='ps-product__price').text.strip().split("/")[0] if soup.find('h4', class_='ps-product__price') else None
    price = re.sub(r'[₫,]', '', price_text) if price_text else None
    brand = soup.find('a', href=lambda href: href and 'brands' in href).text.strip() if soup.find('a', href=lambda href: href and 'brands' in href) else None
    
    # Các thuộc tính bổ sung từ phần mô tả chi tiết
    description_items = soup.find('div', class_='ps-product__desc')
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
        print(origin)
    # Lấy số lượng sản phẩm có sẵn, chỉ giữ lại số
    quantity = soup.find('div', class_='number-items-available')
    quantity_available = ''.join(filter(str.isdigit, quantity.text)) if quantity else "Không xác định"

    return {
        'Product Name': product_name,
        'Price': price,
        'Brand': brand,
        'SKU': sku,
        'Material': material,
        'Shape': shape,
        'Dimension': dimension,
        'Origin': origin,
        'Quantity Available': quantity_available,
        'Product URL': product_url
    }

# Hàm chính để duyệt qua các trang danh mục và cào dữ liệu tất cả sản phẩm
def scrape_all_products_to_csv():
    all_products = []
    base_url = "https://kinhmatviettin.vn/product-categories/gong-kinh?page="
    total_pages = 5  # Số trang cần duyệt
    # Lấy ngày hiện tại để tạo tên file
    current_date = datetime.now().strftime("%Y-%m-%d")
    # Lấy phần domain của base_url cho tên file
    domain_name = base_url.split("//")[1].split("/")[0]
    # Tạo tên file theo format yêu cầu
    csv_filename = f"daily_data_{current_date}_{domain_name}.csv"
    # Lặp qua từng trang danh mục
    for page in range(1, total_pages + 1):
        page_url = f"{base_url}{page}"
        print(f"Fetching products from: {page_url}")
        
        product_links = get_product_links(page_url)
        
        
        # Lặp qua từng link sản phẩm để lấy thông tin chi tiết
        for product_url in product_links:
            print(f"Fetching details for: {product_url}")
            product_details = get_product_details(product_url)
            all_products.append(product_details)
            
    
    #Lưu dữ liệu vào file CSV
    df = pd.DataFrame(all_products)
    df.to_csv(csv_filename, index=False, encoding='utf-8')
    print(f"Data saved to {csv_filename}")
    print(f"Total products scraped: {len(all_products)}")
           

def format_description_text(text):
    # Biểu thức chính quy kiểm tra và thêm "•" trước "Thông tinNK và PP:" nếu chưa có
    pattern = r"(•\s*)?Thông tin"
    formatted_text = re.sub(pattern, r"• Thông tin", text)
    return formatted_text
# Chạy hàm chính để bắt đầu quá trình cào dữ liệu
scrape_all_products_to_csv()
