import requests
from bs4 import BeautifulSoup
import pandas as pd

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
x = requests.get('https://www.goodreads.com/', headers = headers)

if x.status_code == 200:
    soup = BeautifulSoup(x.content,'html.parser')

data = []
linksCategory = soup.find_all('a', class_='gr-hyperlink')
for link in linksCategory[3:32]:
    connectLink = requests.get('https://www.goodreads.com'+link['href'], headers = headers)
    print('Watching:', link['href'])
    if connectLink.status_code == 200:
        soup = BeautifulSoup(connectLink.content,'html.parser')
        books = soup.find('div',class_='coverBigBox clearFloats bigBox').find_all('div', class_='coverWrapper')
        for book in books:
            connectBook = requests.get('https://www.goodreads.com'+book.a['href'], headers = headers)
            print('Watching book:', book.a['href'])
            if connectBook.status_code == 200:
                soup = BeautifulSoup(connectBook.content,'html.parser')
                title = soup.find('h1', class_='Text Text__title1').text
                author = soup.find('span', class_='ContributorLink__name').text
                rating = soup.find('div', class_='RatingStatistics__rating').text
                listsRatingCount = soup.find('div', class_='RatingStatistics__meta')
                ratingCount = listsRatingCount.find_all('span')[0].text
                reviewCount = listsRatingCount.find_all('span')[1].text
                page = soup.find('div', class_='FeaturedDetails').find_all('p')[0].text
                if 'pages' in page:
                    page = page
                else:
                    page = 'Not sure, '+page
                published = soup.find('div', class_='FeaturedDetails').find_all('p')[1].text
                price = soup.find('button', class_='Button Button--buy Button--medium Button--block').find('span').text.split(' ')[-1]
                if '$' in price:
                    price = price
                else:
                    price = 'Shop in Amazon'
                listGenres = soup.find_all('span', class_='BookPageMetadataSection__genreButton')
                genres = [genre.find('a').find('span').text for genre in listGenres]
                imgLink = soup.find('img',class_='ResponsiveImage')['src']
                shortDescription = soup.find('span', class_='Formatted').text
                print(title, author, rating, ratingCount, reviewCount, page, published, price, genres)
                data.append([title, author, rating, ratingCount, reviewCount, page, published, price, genres, imgLink, shortDescription])
            else:
                print('Error connectBook:', connectBook.status_code)
    else:
        print('Error connectLink:', connectLink.status_code)
df = pd.DataFrame(data, columns=['Title', 'Author', 'Rating', 'Rating Count', 'Review Count', 'Page', 'Published', 'Price', 'Genres', 'Image link', 'Short Description'])
df.to_excel(f'Goodreads.xlsx', index=False)
        
    