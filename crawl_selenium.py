import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
import glob
import os


print("Đã kết hợp các file Excel thành công và lưu lại thành 'Goodreads_combined.xlsx'.")

def loadTrang():
    while True:
        if driver.execute_script("return document.readyState") == "complete":
            break

service = Service('chromedriver.exe')
driver = webdriver.Chrome(service=service)

data = []
driver.get('https://www.goodreads.com/')
loadTrang()

linksCategory = driver.find_elements(By.CSS_SELECTOR,'.gr-hyperlink')
linksCategory = [link.get_attribute('href') for link in linksCategory][3:32]

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
    for book in booksLink:
        try:
            driver.get(book)
            while True:
                try:
                    loadTrang()
                    title = driver.find_element(By.CSS_SELECTOR,'h1.Text.Text__title1').text
                    print('Đọc xong title')
                    author = driver.find_element(By.CSS_SELECTOR,'span.ContributorLink__name').text
                    print('Đọc xong author')
                    rating = driver.find_element(By.CSS_SELECTOR,'div.RatingStatistics__rating').text
                    print('Đọc xong rating')
                    ratingCount = driver.find_elements(By.CSS_SELECTOR,'div.RatingStatistics__meta span')[0].text
                    print('Đọc xong ratingCount')
                    reviewCount = driver.find_elements(By.CSS_SELECTOR,'div.RatingStatistics__meta span')[1].text
                    print('Đọc xong reviewCount')
                    page = driver.find_elements(By.CSS_SELECTOR,'div.FeaturedDetails p')[0].text
                    if 'pages' in page:
                        page = page
                    else:
                        page = 'Not sure, '+page
                    print('Đọc xong page')
                    published = driver.find_elements(By.CSS_SELECTOR,'div.FeaturedDetails p')[1].text
                    print('Đọc xong published')
                    try:
                        price = driver.find_element(By.CSS_SELECTOR,'button.Button.Button--buy.Button--medium').text.split(' ')[-1]
                        if '$' in price:
                            price = price
                        else:
                            price = 'Shop in Amazon'
                    except:
                        price = 'Not found'
                    print('Đọc xong price')
                    genres = [genre.find_element(By.CSS_SELECTOR,'a span').text for genre in driver.find_elements(By.CSS_SELECTOR,'span.BookPageMetadataSection__genreButton')]
                    print('Đọc xong genres')
                    shortDescription = driver.find_element(By.CSS_SELECTOR,'span.Formatted').text
                    print('Đọc xong shortDescription')
                    while True:
                        try:
                            print('Đọc moreDetails')
                            moreDetails = WebDriverWait(driver, 5).until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.Button.Button--inline.Button--medium[aria-label="Book details and editions"]'))
                            )
                            moreDetails = driver.find_element(By.CSS_SELECTOR,'button.Button.Button--inline.Button--medium[aria-label="Book details and editions"]')
                            moreDetails.click()
                            print('Đọc xong moreDetails')
                            details = driver.find_element(By.CSS_SELECTOR,'div.EditionDetails').find_elements(By.CSS_SELECTOR,'div.DescListItem')
                            print('Đọc xong details')
                            break
                        except:
                            print('Lỗi moreDetails')
                    isbn = 'Not found'
                    language = 'Not found'
                    for detail in details:
                        if 'ISBN' in detail.find_element(By.CSS_SELECTOR,'dt').text:
                            isbn = detail.find_element(By.CSS_SELECTOR,'div.TruncatedContent__text.TruncatedContent__text--small').text.split(' ')[0]
                        if 'Language' in detail.find_element(By.CSS_SELECTOR,'dt').text:
                            language = detail.find_element(By.CSS_SELECTOR,'div.TruncatedContent__text.TruncatedContent__text--small').text
                    print('Đọc xong isbn, language')
                    print(title, author, rating, ratingCount, reviewCount, page, published, price, genres, isbn, language)
                    data.append([title, author, rating, ratingCount, reviewCount, page, published, price, genres, isbn,language, shortDescription])
                    break
                except:
                    driver.refresh()
            df = pd.DataFrame(data, columns=['Title', 'Author', 'Rating', 'Rating Count', 'Review Count', 'Page', 'Published', 'Price', 'Genres', 'ISBN','Language', 'Short Description'])
            df.to_excel(f'Goodreads.xlsx', index=False)
        except:
            driver.refresh()
            print('Lỗi get')    

df = pd.DataFrame(data, columns=['Title', 'Author', 'Rating', 'Rating Count', 'Review Count', 'Page', 'Published', 'Price', 'Genres', 'ISBN','Language', 'Short Description'])
df.to_excel(f'Goodreads.xlsx', index=False)

# Tìm tất cả các file Excel trong thư mục hiện tại (định dạng .xlsx)
excel_files = glob.glob('*.xlsx')

# Đọc và lưu trữ từng file Excel vào danh sách
df_list = [pd.read_excel(file) for file in excel_files]

# Kết hợp tất cả các DataFrame thành một DataFrame duy nhất
combined_df = pd.concat(df_list)

# Loại bỏ các dòng trùng lặp (nếu có)
combined_df = combined_df.drop_duplicates()

# Ghi DataFrame kết hợp thành file Excel cuối cùng
combined_df.to_excel('Goodreads_combined.xlsx', index=False)

# Xóa các file Excel tạm (nếu muốn)
for file in excel_files:
    os.remove(file)