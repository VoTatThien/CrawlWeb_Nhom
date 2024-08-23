import scrapy
from goodreads.items import GoodreadsItem

class GoodreadsAppCrawler(scrapy.Spider):
    name = "GoodreadsAppCrawler"
    start_urls = ['https://www.goodreads.com/list/show/1.Best_Books_Ever']

    def parse(self, response):
        for book in response.css('tr'):
            item = GoodreadsItem()
            item['title'] = book.css('a.bookTitle span::text').get(default='N/A')
            item['author'] = book.css('a.authorName span::text').get(default='N/A')
            item['rating'] = book.css('span.minirating::text').get(default='N/A').strip()
            item['book_url'] = response.urljoin(book.css('a.bookTitle::attr(href)').get(default=''))
            item['description'] = book.css('span.readable span::text').get(default='N/A')
            item['review_count'] = book.css('a.gr-hyperlink::text').re_first(r'\d+', default='N/A')
            item['rating_count'] = book.css('span.votes.value-title::text').re_first(r'\d+', default='N/A')
            item['publication_year'] = book.css('span.greyText.smallText.uitext::text').re_first(r'\d{4}', default='N/A')
            item['genres'] = book.css('a.actionLinkLite.bookPageGenreLink::text').getall()
            item['isbn'] = book.css('div.infoBoxRowItem span::text').re_first(r'\d+', default='N/A')

            yield item

        next_page = response.css('a.next_page::attr(href)').get()
        if next_page:
            yield response.follow(next_page, self.parse)
