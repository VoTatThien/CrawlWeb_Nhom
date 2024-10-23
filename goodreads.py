import scrapy

class GoodreadsSpider(scrapy.Spider):
    name = "goodreads"
    allowed_domains = ["goodreads.com"]
    start_urls = [
        'https://www.goodreads.com/list/show/1.Best_Books_Ever'
    ]
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': 1000
    }

    def parse(self, response):
        for book in response.css('tr'):
            yield {
                'title': book.css('a.bookTitle span::text').get(),
                'author': book.css('a.authorName span::text').get(),
                'rating': book.css('span.minirating::text').get(),
                'details': book.css('a.bookTitle::attr(href)').get(),
                'num_reviews': book.css('span.greyText.smallText::text').re_first(r'(\d{1,3}(,\d{3})*) reviews'),
                'num_ratings': book.css('span.greyText.smallText::text').re_first(r'(\d{1,3}(,\d{3})*) ratings'),
                'publish_date': book.css('span.greyText.smallText::text').re_first(r'published (\d{4})'),
                'genres': book.css('a.actionLinkLite.bookPageGenreLink::text').getall(),
                'description': book.css('span.readable span::text').get(),
                'image_url': book.css('img.bookCover::attr(src)').get(),
            }

        next_page = response.css('a.next_page::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)
