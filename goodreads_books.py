import csv
import requests
from bs4 import BeautifulSoup

# URL của trang Goodreads mà bạn muốn cào
url = 'https://www.goodreads.com/list/show/19.Best_for_Book_Clubs'  # Thay URL này bằng trang Goodreads cụ thể bạn muốn cào

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

    # Tìm tên sách, tác giả và đánh giá
    book_titles = soup.find_all('a', {'class': 'bookTitle'})
    authors = soup.find_all('a', {'class': 'authorName'})
    avg_ratings = soup.find_all('span', {'class': 'minirating'})

    # Lưu trữ dữ liệu đã cào
    scraped_data = []

    # Lưu dữ liệu vào danh sách
    for idx in range(min(len(book_titles), len(authors), len(avg_ratings))):
        title = book_titles[idx].get_text(strip=True)
        author = authors[idx].get_text(strip=True)
        rating = avg_ratings[idx].get_text(strip=True)

        scraped_data.append({
            'Tên sách': title,
            'Tác giả': author,
            'Đánh giá': rating
        })

    # Lưu dữ liệu vào file CSV
    with open('goodreads_books.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['Tên sách', 'Tác giả', 'Đánh giá'])
        writer.writeheader()
        writer.writerows(scraped_data)

    print("Dữ liệu đã được lưu vào file 'goodreads_books.csv'.")

else:
    print(f"Lỗi khi truy cập trang. Mã trạng thái: {response.status_code}")