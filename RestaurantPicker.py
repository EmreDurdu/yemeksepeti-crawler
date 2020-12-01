import difflib
from datetime import datetime

from pymongo import MongoClient


class RestaurantPicker:
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client['YemekSepeti']
        self.restaurants = self.db['Restaurant']
        self.restaurant_dataset = self.db['RestaurantDataset']
        if self.restaurant_dataset is None:
            self.db.create_collection(name='RestaurantDataset')
            self.restaurant_dataset = self.db['RestaurantDataset']
        self.districts = self.db['District']
        self.cities = self.db['City']

        tmp = list(self.districts.find({}, {'_id': 0, 'area_name': 1}))
        self.district_names = []
        for t in tmp:
            self.district_names.append(t['area_name'])

        tmp = list(self.cities.find({}, {'_id': 0, 'name': 1}))
        self.city_names = []
        for t in tmp:
            self.city_names.append(t['name'])

    def add_area_names(self):
        restaurants = self.restaurants.find()
        i = 1
        for restaurant in restaurants:
            if ',' in restaurant['DisplayName']:

                str = restaurant['DisplayName'].split(',')
                print(str)
                restaurant_name = str[0].strip()
                area_name = str[1].strip()
                print(f'restaurant name: {restaurant_name} and area_name: {area_name}')
                # print(restaurant)
                self.restaurants.update({'_id': restaurant['_id']},
                                        {"$set": {"area_name": area_name, "restaurant_name": restaurant_name}})

                print(f'{i}th restaurant updated with values {restaurant_name} and {area_name}')
            else:
                self.restaurants.update({'_id': restaurant['_id']},
                                        {"$set": {"area_name": None, "restaurant_name": None}})

            i += 1

    def fix_work_hours(self):
        i = 1
        restaurants = self.restaurants.find()
        for restaurant in restaurants:
            work_hours_text = restaurant['WorkHoursText']
            if work_hours_text is not None and work_hours_text != '':
                print(work_hours_text)
                start_hour = float(work_hours_text.split('-')[0].replace(":", "."))
                end_hour = float(work_hours_text.split('-')[1].replace(":", "."))
                self.restaurants.update(
                    {'_id': restaurant['_id']},
                    {
                        "$set": {
                            "start_hour": start_hour,
                            "end_hour": end_hour
                        }
                    }
                )
            else:
                self.restaurants.update(
                    {'_id': restaurant['_id']},
                    {
                        "$set": {
                            "start_hour": None,
                            "end_hour": None
                        }
                    }
                )
            print(f'{i}th restaurant updated')
            i += 1

    def add_created_dates(self):
        i = 1
        for restaurant in self.restaurants.find():
            date_str = restaurant['CreatedDate']
            if date_str is not None:
                date_str = date_str.replace('/', '')
                date_str = date_str.replace('Date', '')
                date_str = date_str.replace('(', '')
                date_str = date_str.replace(')', '')
                mils = int(date_str[:date_str.index('+')])
                tmp = datetime.utcfromtimestamp(mils / 1000.)
                self.restaurants.update(
                    {'_id': restaurant['_id']},
                    {"$set":
                         {"created_year": tmp.year}
                     }
                )
                print(f'{i}th restaurant updated')
                i += 1
            else:
                self.restaurants.update(
                    {'_id': restaurant['_id']},
                    {"$set":
                         {"created_year": None}
                     }
                )
                print(f'{i}th restaurant updated')
                i += 1

    def pick_dataset(self, city=None, district=None, created_year=None, start_hour=None, end_hour=None, point=None):
        self.restaurant_dataset.remove({})
        for name in self.db.list_collection_names():
            if name not in ['City', 'Comment', 'District', 'Menu', 'Restaurant', 'RestaurantDataset']:
                self.db.drop_collection(name_or_collection=name)
        query = {}
        if city is not None:
            tmp = difflib.get_close_matches(city, self.city_names, len(self.city_names), 0)
            city_name = tmp[0]
            cn = 'TR_' + city_name.upper()
            query['CatalogName'] = cn
        if district is not None:
            tmp = difflib.get_close_matches(district, self.district_names, len(self.district_names), 0)
            district_name = tmp[0]
            query['area_name'] = district_name
        if created_year is not None:
            query['created_year'] = {"$gte": created_year}
        if start_hour is not None:
            query['start_hour'] = {"$not": {"$gt": start_hour}}
        if end_hour is not None:
            query['end_hour'] = {"$not": {"$lt": end_hour}}

        if point is not None:
            point = str(point).replace(".", ",")
            query['AvgRestaurantScore'] = {"$gte": point}
        restaurants = list(self.restaurants.find(query))
        if len(restaurants) != 0:
            self.restaurant_dataset.insert_many(restaurants)
        print(f'{len(restaurants)} countity added to RestaurantDataset collection in MongoDB')
