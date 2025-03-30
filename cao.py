import requests
from bs4 import BeautifulSoup
import pandas as pd

# Define headers for the requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}

# Make a request to the main page
response = requests.get('https://www.goodreads.com/', headers=headers)

# Check if the request was successful
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all category links
    linksCategory = soup.find_all('a', class_='gr-hyperlink')
    
    # Iterate over a subset of category links
    for link in linksCategory[3:32]:
        connectLink = requests.get('https://www.goodreads.com' + link['href'], headers=headers)
        
        # Check if the request was successful
        if connectLink.status_code == 200:
            soup = BeautifulSoup(connectLink.content, 'html.parser')
            
            # Find all books on the page
            books = soup.find("div", class_='coverBigBox clearFloats bigBox').find_all('div', class_='coverWrapper')
            
            for book in books:
                connectBook = requests.get('https://www.goodreads.com' + book.a['href'], headers=headers)
                
                # Check if the request was successful
                if connectBook.status_code == 200:
                    book_soup = BeautifulSoup(connectBook.content, 'html.parser')
                    
                    # Extract book details
                    title = book_soup.find('h1', id='bookTitle').text.strip() if book_soup.find('h1', id='bookTitle') else 'N/A'
                    author = book_soup.find('a', class_='authorName').text.strip() if book_soup.find('a', class_='authorName') else 'N/A'
                    rating = book_soup.find('span', itemprop='ratingValue').text.strip() if book_soup.find('span', itemprop='ratingValue') else 'N/A'
                    
                    print(f'Watching book: {book.a["href"]}')
                    print(f'Title: {title}')
                    print(f'Author: {author}')
                    print(f'Rating: {rating}')