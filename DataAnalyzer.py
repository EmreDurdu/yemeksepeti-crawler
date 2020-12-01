from pymongo import MongoClient
import matplotlib.pyplot as plt
from bson.objectid import ObjectId


class DataSegmentizer:
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client['YemekSepeti']
        self.data_set = self.db['RestaurantDataset']
        if self.data_set is None:
            self.db.create_collection(name='RestaurantDataset')
            self.data_set = self.db['RestaurantDataset']

        self.restaurants = self.db['Restaurant']
        self.comments = self.db['Comment']
        self.menus = self.db['Menu']

    def fix_restaurants(self):
        restaurants = self.restaurants.find()
        for restaurant in restaurants:
            display_name = restaurant['DisplayName']
            if display_name is not None and ',' in display_name:
                res_name = display_name.split(',')[0]
                area_name = display_name.split(',')[1]
                self.restaurants.update({'_id': restaurant['_id']}, {"$set": {
                    "restaurant_name": res_name,
                    "area_name": area_name
                }})

    def segmentize(self):
        i = 1
        self.comments.create_index([("menu_id", 1)])
        self.comments.create_index([('restaurant_id', 1)])
        restaurants = self.data_set.find()
        for restaurant in restaurants:
            _menus = self.menus.find({'restaurant_id': restaurant['_id']})
            j = 1
            for menu in _menus:
                seg = self.db[f'{restaurant["_id"]}_{menu["_id"]}']
                if seg is None:
                    seg = self.db.create_collection(name=f'{restaurant["_id"]}_{menu["_id"]}')
                else:
                    seg.remove({})  # drop all data from segment
                comments = list(self.comments.find(
                    {
                        "menu_id": menu['_id'],
                        "restaurant_id": restaurant['_id']
                    }
                ))
                j += 1
                if len(comments) != 0:
                    seg.insert_many(comments)
                else:
                    self.db.drop_collection(name_or_collection=f'{restaurant["_id"]}_{menu["_id"]}')
            print(f'{j} segment added for restaurant {i} out of {restaurants.count()}')
            i += 1

    def analyze_comment_frequencies(self):
        col_names = self.db.collection_names()
        restaurant_names = []
        restaurant_ids = []
        menu_names = []
        menu_ids = []
        for col_name in col_names:
            if '_' in col_name:
                res_id = col_name.split('_')[0]
                if res_id not in restaurant_ids:
                    restaurant_ids.append(res_id)
                    restaurant = self.data_set.find_one({'_id': ObjectId(res_id)})
                    res_name = restaurant['restaurant_name']
                    restaurant_names.append(res_name)
        while True:
            i = 1
            for name in restaurant_names:
                print(f'{i}-{name}')
                i += 1

            print('Please select a restaurant')
            res_index = int(input())
            res_id = restaurant_ids[res_index - 1]

            for col_name in col_names:
                if f'{res_id}_' in col_name:
                    menu_id = col_name.split('_')[1]
                    menu_ids.append(menu_id)
                    menu = self.menus.find_one({'_id': ObjectId(menu_id)})
                    menu_names.append(menu['product_name'])

            print('----------------')
            i = 1
            for name in menu_names:
                print(f'{i}-{name}')
                i += 1
            print('Please select a menu')
            menu_index = int(input())
            menu_id = menu_ids[menu_index - 1]
            data_set_comments = self.db[f'{res_id}_{menu_id}']
            print(f'{res_id}_{menu_id}')
            comment_date_counter = []
            for i in range(0, 12):
                comment_date_counter.append(0)
            for i in range(0, 12):
                for comment in data_set_comments.find():
                    c_date = comment['date']
                    if f'{i + 1}/2020' in c_date:
                        comment_date_counter[i] += 1
            plt.plot([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], comment_date_counter)
            plt.show()
            ans = input('Do you want to continue (y/n)')
            if ans == 'n':
                break
