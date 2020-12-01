from scrapy.crawler import CrawlerProcess, CrawlerRunner
from twisted.internet import reactor, defer

from CommentMatcher import CommentMatcher
from Spiders import CommentSpider, CitiesSpider, DistrictSpider, RestaurantSpider, MenuSpider


def start_sequentially(process: CrawlerProcess, crawlers: list):
    print('start crawler {}'.format(crawlers[0].__name__))
    deferred = process.crawl(crawlers[0])
    if len(crawlers) > 1:
        deferred.addCallback(lambda _: start_sequentially(process, crawlers[1:]))


if __name__ == '__main__':
    print(
        "Note that, since the scrapping takes too much time, this is implemented only for Ankara."
        " If you want other cities too, you need to edit Spiders.py file")
    crawlers = [CitiesSpider, DistrictSpider, RestaurantSpider, MenuSpider, CommentSpider]
    process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
    start_sequentially(process, crawlers)
    process.start()

    cm = CommentMatcher()
    cm.add_city_name()
    cm.guess_menus()
