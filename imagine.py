import requests
from bs4 import BeautifulSoup
import csv

# URL của trang Goodreads mà bạn muốn cào
url = 'https://www.goodreads.com/'

# Headers tùy chỉnh để giả lập yêu cầu từ trình duyệt
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}

# Gửi yêu cầu GET đến trang web với headers
response = requests.get(url, headers=headers)

# Kiểm tra nếu yêu cầu thành công (mã trạng thái 200)
if response.status_code == 200:
    # Phân tích nội dung HTML của trang
    soup = BeautifulSoup(response.text, 'html.parser')

    # In ra tiêu đề của trang để xác nhận trang được tải đúng
    print(f"Tiêu đề của trang: {soup.title.string}\n")
    
    # Tìm tất cả các thẻ <img> và lấy URL của các hình ảnh
    images = soup.find_all('img')
    
    if images:
        # Mở (hoặc tạo mới) một file CSV để lưu trữ các URL hình ảnh
        with open('image_urls.csv', 'w', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            # Ghi tiêu đề của cột
            csvwriter.writerow(['Index', 'Image URL'])
            
            # Ghi các URL hình ảnh vào file CSV
            for idx, img in enumerate(images, 1):
                src = img.get('src')
                if src:
                    csvwriter.writerow([idx, src])
                else:
                    csvwriter.writerow([idx, 'Không có URL cho thẻ <img>'])

        print("Danh sách URL hình ảnh đã được lưu vào 'image_urls.csv'.")
    else:
        print("Không tìm thấy thẻ <img> nào.")
else:
    print(f"Lỗi khi truy cập trang. Mã trạng thái: {response.status_code}")