import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import numpy as np

service = Service('chromedriver.exe')
driver = webdriver.Chrome(service=service)

data = []
driver.get('https://www.goodreads.com/')
time.sleep(5)

linksCategory = driver.find_elements(By.CSS_SELECTOR,'.gr-hyperlink')
linksCategory = [link.get_attribute('href') for link in linksCategory][3:32]

for link in linksCategory:
    driver.get(link)
    time.sleep(5)
    books = driver.find_element(By.CSS_SELECTOR,'.coverBigBox.clearFloats.bigBox').find_elements(By.CSS_SELECTOR,'.coverWrapper')
    books = [book.find_element(By.CSS_SELECTOR,'a').get_attribute('href') for book in books]
    for book in books:
        driver.get(book)
        time.sleep(5)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[6]/div/div/button")))
        moreDetails = driver.find_element("xpath","/html/body/div[1]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[6]/div/div/button")
        moreDetails.click()
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
        price = driver.find_element(By.CSS_SELECTOR,'button.Button.Button--buy.Button--medium.Button--block span').text.split(' ')[-1]
        if '$' in price:
            price = price
        else:
            price = 'Shop in Amazon'
        genres = [genre.find_element(By.CSS_SELECTOR,'a span').text for genre in driver.find_elements(By.CSS_SELECTOR,'span.BookPageMetadataSection__genreButton')]
        shortDescription = driver.find_element(By.CSS_SELECTOR,'span.Formatted').text
        details = driver.find_element(By.CSS_SELECTOR,'div.EditionDetails').find_elements(By.CSS_SELECTOR,'div.DescListItem')
        isbn = details[2].find_element(By.CSS_SELECTOR,'div.TruncatedContent__text.TruncatedContent__text--small').text.split(' ')[0]
        language = details[4].find_element(By.CSS_SELECTOR,'div.TruncatedContent__text.TruncatedContent__text--small').text
        print(title, author, rating, ratingCount, reviewCount, page, published, price, genres, isbn, language)
        data.append([title, author, rating, ratingCount, reviewCount, page, published, price, genres, isbn,language, shortDescription])

df = pd.DataFrame(data, columns=['Title', 'Author', 'Rating', 'Rating Count', 'Review Count', 'Page', 'Published', 'Price', 'Genres', 'ISBN','Language', 'Short Description'])
df.to_excel(f'Goodreads.xlsx', index=False)
