import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import glob
import os

# Hàm load trang
def loadTrang():
    while True:
        if driver.execute_script("return document.readyState") == "complete":
            break

# Khởi tạo trình duyệt
service = Service('chromedriver.exe')
driver = webdriver.Chrome(service=service)

# Khởi tạo danh sách để lưu trữ dữ liệu
data = []

# URL của trang Goodreads
driver.get('https://www.goodreads.com/')
loadTrang()

# Lấy các đường dẫn danh mục
linksCategory = driver.find_elements(By.CSS_SELECTOR,'.gr-hyperlink')
linksCategory = [link.get_attribute('href') for link in linksCategory][3:32]

# Lặp qua từng danh mục để lấy thông tin sách
for link in linksCategory:
    driver.get(link)
    loadTrang()
    checkMore = driver.find_elements(By.CSS_SELECTOR,'div.coverBigBox.clearFloats.bigBox')
    for check in checkMore:
        try:
            if "/shelf/show/" in check.find_element(By.CSS_SELECTOR,'h2.brownBackground a').get_attribute('href'):
                more = check.find_element(By.CSS_SELECTOR,'h2.brownBackground a').get_attribute('href')
                break
        except:
            pass
    driver.get(more)
    loadTrang()
    books = driver.find_element(By.CSS_SELECTOR,'div.leftContainer').find_elements(By.CSS_SELECTOR,'div.elementList')
    booksLink = [book.find_element(By.CSS_SELECTOR,'a.bookTitle').get_attribute('href') for book in books]
    
    # Lặp qua từng sách để lấy thông tin
    for book in booksLink:
        try:
            driver.get(book)
            while True:
                try:
                    loadTrang()
                    title = driver.find_element(By.CSS_SELECTOR,'h1.Text.Text__title1').text
                    author = driver.find_element(By.CSS_SELECTOR,'span.ContributorLink__name').text
                    rating = driver.find_element(By.CSS_SELECTOR,'div.RatingStatistics__rating').text
                    ratingCount = driver.find_elements(By.CSS_SELECTOR,'div.RatingStatistics__meta span')[0].text
                    reviewCount = driver.find_elements(By.CSS_SELECTOR,'div.RatingStatistics__meta span')[1].text
                    page = driver.find_elements(By.CSS_SELECTOR,'div.FeaturedDetails p')[0].text
                    if 'pages' in page:
                        page = page
                    else:
                        page = 'Not sure, '+page
                    published = driver.find_elements(By.CSS_SELECTOR,'div.FeaturedDetails p')[1].text
                    try:
                        price = driver.find_element(By.CSS_SELECTOR,'button.Button.Button--buy.Button--medium').text.split(' ')[-1]
                        if '$' in price:
                            price = price
                        else:
                            price = 'Shop in Amazon'
                    except:
                        price = 'Not found'
                    genres = [genre.find_element(By.CSS_SELECTOR,'a span').text for genre in driver.find_elements(By.CSS_SELECTOR,'span.BookPageMetadataSection__genreButton')]
                    shortDescription = driver.find_element(By.CSS_SELECTOR,'span.Formatted').text
                    
                    # Lấy chi tiết thêm (ISBN và ngôn ngữ)
                    try:
                        moreDetails = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.Button.Button--inline.Button--medium[aria-label="Book details and editions"]'))
                        )
                        moreDetails.click()
                        details = driver.find_element(By.CSS_SELECTOR,'div.EditionDetails').find_elements(By.CSS_SELECTOR,'div.DescListItem')
                    except:
                        details = []

                    isbn = 'Not found'
                    language = 'Not found'
                    for detail in details:
                        if 'ISBN' in detail.find_element(By.CSS_SELECTOR,'dt').text:
                            isbn = detail.find_element(By.CSS_SELECTOR,'div.TruncatedContent__text.TruncatedContent__text--small').text.split(' ')[0]
                        if 'Language' in detail.find_element(By.CSS_SELECTOR,'dt').text:
                            language = detail.find_element(By.CSS_SELECTOR,'div.TruncatedContent__text.TruncatedContent__text--small').text
                    
                    # Lưu thông tin sách vào danh sách
                    data.append([title, author, rating, ratingCount, reviewCount, page, published, price, genres, isbn, language, shortDescription])
                    break
                except:
                    driver.refresh()
            
            # Ghi dữ liệu vào file Excel tạm thời (mỗi danh mục)
            df = pd.DataFrame(data, columns=['Title', 'Author', 'Rating', 'Rating Count', 'Review Count', 'Page', 'Published', 'Price', 'Genres', 'ISBN','Language', 'Short Description'])
            df.to_excel(f'{link.split("/")[-1]}.xlsx', index=False)
        except:
            driver.refresh()

# Kết thúc quá trình lấy dữ liệu
driver.quit()

# Kết hợp nhiều file Excel thành một file duy nhất
excel_files = glob.glob('*.xlsx')
df_list = [pd.read_excel(file) for file in excel_files]
combined_df = pd.concat(df_list)

# Loại bỏ các dòng trùng lặp (nếu có)
combined_df = combined_df.drop_duplicates()

# Ghi DataFrame kết hợp thành file Excel cuối cùng
combined_df.to_excel('Goodreads_combined.xlsx', index=False)

# Xóa các file tạm
for file in excel_files:
    os.remove(file)
