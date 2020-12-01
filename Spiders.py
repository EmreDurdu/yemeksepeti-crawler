import json
from datetime import date
from math import ceil

from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from pymongo import MongoClient
import scrapy
import time
import datetime
import dateutil.relativedelta


class CitiesSpider(scrapy.Spider):
    name = 'yemeksepeti.com'
    allowed_domains = ['yemeksepeti.com']

    def __init__(self, **kwargs):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['YemekSepeti']
        self.cities_collection = self.db['Cities']
        if self.cities_collection is None:
            self.db.create_collection(name='City')
            self.cities_collection = self.db['Cities']

    def start_requests(self):
        print("Waiting for response...")
        yield scrapy.Request('https://www.yemeksepeti.com/sehir-secim', callback=self.parse)
        print("Response taken")

    def parse(self, response, **kwargs):
        body = response.body
        response = HtmlResponse(url='https://www.yemeksepeti.com/sehir-secim', body=body)
        links = response.xpath('/html/body/div[2]/div[3]/div/div/div/a').getall()
        for link in links:
            _link = Selector(text=link).xpath('//a/@href').get()
            _plate_no = Selector(text=link).xpath('//a/div/span/text()').get()
            _name = Selector(text=link).xpath('//a/span/text()').get()
            city = {
                "plate_no": _plate_no,
                "name": _name,
                "link": _link
            }
            inserted_id = self.cities_collection.insert_one(city).inserted_id
            print(f'City {inserted_id} has been inserted to the database')


class DistrictSpider(scrapy.Spider):

    def __init__(self, **kwargs):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['YemekSepeti']
        self.cities = self.db['City']
        if self.cities is None:
            self.db.create_collection(name='City')
            self.cities = self.db['Cities']

        self.districts = self.db['District']
        if self.districts is None:
            self.db.create_collection(name='District')
            self.districts = self.db['District']

        self.host = 'https://yemeksepeti.com'

    def start_requests(self):
        for city in self.cities.find():
            yield scrapy.Request(f'https://www.yemeksepeti.com{city["link"]}',
                                 callback=self.parse,
                                 meta={
                                     'city_name': city['name'],
                                     'city_id': city['_id'],
                                     'current_link': city['link']
                                 })

    def parse(self, response, **kwargs):
        body = response.body
        __city_name = response.meta.get('city_name')
        __city_id = response.meta.get('city_id')
        __current_link = response.meta.get('current_link')
        options = Selector(text=body).xpath('//*[@id="ys-areaSelector"]/optgroup[1]/option').getall()
        for option in options:
            _link = Selector(text=option).xpath('//option/@data-url').get()
            _area_name = Selector(text=option).xpath('//option/@data-area-name').get()
            _value = Selector(text=option).xpath('//option/@value').get()
            district = {
                'link': _link,
                'area_name': _area_name,
                'value': _value,
                'city_id': __city_id,
                'city_name': __city_name
            }
            inserted_id = self.districts.insert_one(district).inserted_id
            print(f'District is inserted with id {inserted_id}')


class RestaurantSpider(scrapy.Spider):
    def __init__(self, **kwargs):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['YemekSepeti']
        self.cities = self.db['City']
        if self.cities is None:
            self.db.create_collection(name='City')
            self.districts = self.db['City']

        self.districts = self.db['District']
        if self.districts is None:
            self.db.create_collection(name='District')
            self.districts = self.db['District']

        self.restaurants = self.db['Restaurant']
        if self.restaurants is None:
            self.db.create_collection(name='Restaurant')
            self.restaurants = self.db['Restaurant']

        self.host = 'https://yemeksepeti.com'
        self.page_counter = 1
        self.max_page = 0  # will be updated after first page request
        self.restaurant_counter = 1

    def start_requests(self):
        url = 'https://service.yemeksepeti.com/YS.WebServices/CatalogService.svc/SearchRestaurants'
        request_body = {
            "ysRequest": {
                "PageNumber": 1,
                "PageRowCount": "50",
                "Token": "abfafe4363c243a6a28e369c3449f00b",
                "CatalogName": "TR_ANKARA",
                "Culture": "tr-TR",
                "LanguageId": "tr-TR"
            },
            "searchRequest": {
                "SortField": 1,
                "SortDirection": 0,
                "OpenOnly": False
            }
        }
        yield scrapy.Request(url, method='POST',
                             body=json.dumps(request_body),
                             headers={'Content-Type': 'application/json'},
                             callback=self.parse)

    def parse(self, response, **kwargs):
        json_res = json.loads(response.text)
        self.max_page = json_res['d']['TotalPageCount']
        for restaurant in json_res['d']['ResultSet']['searchResponseList']:
            inserted_id = self.restaurants.insert_one(restaurant).inserted_id
            print(f'{self.restaurant_counter}th is added with id {inserted_id}')
            self.restaurant_counter = self.restaurant_counter + 1
        if self.page_counter != self.max_page:
            self.page_counter = self.page_counter + 1
            url = 'https://service.yemeksepeti.com/YS.WebServices/CatalogService.svc/SearchRestaurants'
            request_body = {
                "ysRequest": {
                    "PageNumber": self.page_counter,
                    "PageRowCount": "50",
                    "Token": "abfafe4363c243a6a28e369c3449f00b",
                    "CatalogName": "TR_ANKARA",
                    "Culture": "tr-TR",
                    "LanguageId": "tr-TR"
                },
                "searchRequest": {
                    "SortField": 1,
                    "SortDirection": 0,
                    "OpenOnly": False
                }
            }

            yield scrapy.Request(url, method='POST',
                                 body=json.dumps(request_body),
                                 headers={'Content-Type': 'application/json'},
                                 callback=self.parse)


class MenuSpider(scrapy.Spider):

    def __init__(self, **kwargs):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['YemekSepeti']
        self.cities = self.db['City']
        if self.cities is None:
            self.db.create_collection(name='City')
            self.cities = self.db['City']

        self.districts = self.db['District']
        if self.districts is None:
            self.db.create_collection(name='District')
            self.districts = self.db['District']

        self.restaurants = self.db['Restaurant']
        if self.restaurants is None:
            self.db.create_collection(name='Restaurant')
            self.restaurants = self.db['Restaurant']

        self.menus = self.db['Menu']
        self.host = 'https://yemeksepeti.com'
        self.menu_counter = 0

    def start_requests(self):
        for restaurant in self.restaurants.find({'CatalogName': 'TR_ANKARA'}):
            url = restaurant['SeoUrl']
            time.sleep(1)  # in order not to get banned
            yield scrapy.Request(url=self.host + url,
                                 callback=self.parse,
                                 meta={
                                     'restaurant_id': restaurant['_id']
                                 })

    def parse(self, response, **kwargs):
        body = response.body
        restaurant_id = response.meta['restaurant_id']
        _xpath = '//*[re:test(@id, "menu_\d$"]/div[2]/ul/li'
        xpath = '//*[@id="menu_0"]'
        menus = Selector(text=body).xpath('//*[re:test(@id, "menu_\d$")]/div[2]/ul/li').getall()
        for menu in menus:
            product_name = Selector(text=menu).xpath(
                '//li/div[@class="product"]/div[@class="product-info"]/a/text()').get()
            product_desc = Selector(text=menu).xpath(
                '//li/div[@class="product"]/div[@class="product-desc"]/text()').get()
            listed_price = Selector(text=menu).xpath('//li/div[contains(@class, "product-price")]/span[1]/text()').get()
            discounted_price = Selector(text=menu).xpath(
                '//li/div[contains(@class, "product-price")]/span[2]/text()').get()

            _menu = {
                'product_name': product_name,
                'product_desc': product_desc,
                'listed_price': listed_price,
                'discounted_price': discounted_price,
                'restaurant_id': restaurant_id
            }
            inserted_id = self.menus.insert_one(_menu).inserted_id
            print(f'{self.menu_counter}th menu added with id {inserted_id}')
            self.menu_counter = self.menu_counter + 1


class CommentSpider(scrapy.Spider):
    handle_httpstatus_list = [301, 302]

    def __init__(self, **kwargs):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['YemekSepeti']
        self.cities = self.db['City']
        if self.cities is None:
            self.db.create_collection(name='City')
            self.cities = self.db['City']

        self.districts = self.db['District']
        if self.districts is None:
            self.db.create_collection(name='District')
            self.districts = self.db['District']

        self.restaurants = self.db['Restaurant']
        if self.restaurants is None:
            self.db.create_collection(name='Restaurant')
            self.restaurants = self.db['Restaurant']

        self.comments = self.db['Comment']
        if self.comments is None:
            self.db.create_collection(name='Comment')
            self.comments = self.db['Comment']

        self.host = 'https://www.yemeksepeti.com'
        self.comment_counter = 0
        self.current_page = 1
        self.total_restaurant_counter = 1

    def start_requests(self):
        restaurants = self.restaurants.find({'CatalogName': 'TR_ANKARA'})
        for restaurant in restaurants:
            url = restaurant['SeoUrl']
            time.sleep(1)  # in order not to get banned
            yield scrapy.Request(url=f'{self.host}{url}',
                                 callback=self.parse,
                                 errback=self.err_callback,
                                 meta={
                                     'restaurant_id': restaurant['_id'],
                                     'url': url,
                                     'current_page': 1,
                                     'total_comment_count': None,
                                     'current_comment_count': 0,

                                 })

    def err_callback(self, response, **kwargs):
        print(response)

    def parse(self, response, **kwargs):
        body = response.body
        restaurant_id = response.meta['restaurant_id']
        url = response.meta['url']
        current_page = response.meta['current_page']
        total_comment_count = response.meta['total_comment_count']
        current_comment_count = response.meta['current_comment_count']
        if response.status == 301 or response.status == 302:
            new_url = Selector(text=response.text).xpath(('//html/body/h2/a/@href')).get()
            yield scrapy.Request(url=f'{self.host}{new_url}',
                                 callback=self.parse,
                                 errback=self.err_callback,
                                 meta={
                                     'restaurant_id': restaurant_id,
                                     'url': new_url,
                                     'current_page': current_page,
                                     'total_comment_count': total_comment_count,
                                     'current_comment_count': current_comment_count,
                                 })
        else:

            if total_comment_count is None:
                total_comment_count_text = Selector(text=body).xpath(
                    '//*[@id="restaurantDetail"]/div[2]/div[1]/ul/li[4]/a/text()').get()
                total_comment_count = int(total_comment_count_text.split(' ')[1].replace('(', '').replace(')', ''))
            comments = Selector(text=body) \
                .xpath(
                '//*[@id="restaurant_comments"]/div[contains(@class, "comments")]/div[@class="comments-body"]').getall()
            for comment in comments:
                speed_point = None
                serving_point = None
                flavour_point = None
                speed_text = Selector(text=comment).xpath(
                    '//*[contains(@class, "restaurantPoints")]/div[1]/text()').get()
                if speed_text is not None:
                    speed_point = int(speed_text.split(' ')[1])
                serving_text = Selector(text=comment).xpath(
                    '//*[contains(@class, "restaurantPoints")]/div[2]/text()').get()
                if serving_text is not None:
                    serving_point = int(serving_text.split(' ')[1])
                flavour_text = Selector(text=comment).xpath(
                    '//*[contains(@class, "restaurantPoints")]/div[3]/text()').get()
                if flavour_text is not None:
                    flavour_point = int(flavour_text.split(' ')[1])

                date_text = Selector(text=comment).xpath('//*[contains(@class, "commentDate")]/div/text()').get()
                _date = date.today()
                if date_text == 'bugün':
                    _date = date.today()
                else:
                    new_date = date_text.split(' ')
                    value = int(new_date[0])
                    if new_date[1] == 'ay':
                        d = datetime.datetime.strptime(_date.strftime("%Y-%m-%d"), "%Y-%m-%d")
                        _date = d - dateutil.relativedelta.relativedelta(months=value)
                    elif new_date[1] == 'gün':
                        d = datetime.datetime.strptime(_date.strftime("%Y-%m-%d"), "%Y-%m-%d")
                        _date = d - dateutil.relativedelta.relativedelta(days=value)
                    elif new_date[1] == 'yıl':
                        d = datetime.datetime.strptime(_date.strftime("%Y-%m-%d"), "%Y-%m-%d")
                        _date = d - dateutil.relativedelta.relativedelta(years=value)
                comment_text = Selector(text=comment).xpath('//*[contains(@class, "comment")]/p/text()').get()
                _comment = {
                    'speed_point': speed_point,
                    'serving_point': serving_point,
                    'flavour_point': flavour_point,
                    'date': _date.strftime('%d/%m/%Y'),
                    'comment': comment_text,
                    'restaurant_id': restaurant_id,

                }
                current_comment_count = current_comment_count + 1
                inserted_id = self.comments.insert_one(_comment).inserted_id
                # print(f'{self.comment_counter}th comment is added with id {inserted_id}')
                self.comment_counter = self.comment_counter + 1

            if current_comment_count == total_comment_count:
                print(
                    f'{total_comment_count} is added for the {self.total_restaurant_counter}th restaurant with url {url}')
                self.total_restaurant_counter = self.total_restaurant_counter + 1
            else:
                if '?' in url:
                    url = url[:str(url).index('?')]
                time.sleep(1)  # in order not to get banned
                yield scrapy.Request(url=f'{self.host}{url}?section=comments&page={current_page + 1}',
                                     callback=self.parse,
                                     meta={
                                         'restaurant_id': restaurant_id,
                                         'url': url,
                                         'current_page': current_page + 1,
                                         'total_comment_count': total_comment_count,
                                         'current_comment_count': current_comment_count,

                                     })
