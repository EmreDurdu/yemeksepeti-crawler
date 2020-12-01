from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor

from CommentMatcher import CommentMatcher
from Spiders import CommentSpider, CitiesSpider, DistrictSpider, RestaurantSpider, MenuSpider

if __name__ == '__main__':
    print(
        "Note that, since the scrapping takes too much time, this is implemented only for Ankara."
        " If you want other cities too, you need to edit Spiders.py file")
    process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
    process.crawl(CitiesSpider)
    process.crawl(DistrictSpider)
    process.crawl(RestaurantSpider)
    process.crawl(MenuSpider)
    process.crawl(CommentSpider)
    process.start()
    reactor.run()
    cm = CommentMatcher()
    cm.add_city_name()
    cm.guess_menus()
