from DataAnalyzer import DataSegmentizer
from RestaurantPicker import RestaurantPicker
import json
from bson.json_util import loads
from pymongo import MongoClient

if __name__ == '__main__':
    client = MongoClient()
    db = client['YemekSepeti']
    print("Preparing Database")
    collection_names = db.list_collection_names()

    ans = input('Do you want to load data again?(y/n)')
    if ans == 'y':

        if 'City' not in collection_names:
            db.create_collection(name='City')
        cities = db['City']
        cities.delete_many({})
        with open('Data/City.json', encoding="utf8") as f:
            print("Inserting cities ...")
            file_data = json.load(f)
            cities.insert_many(loads(json.dumps(file_data)))
            print("Inserting cities done!")
        if 'District' not in collection_names:
            db.create_collection(name='District')
        districts = db['District']
        districts.delete_many({})
        with open('Data/District.json', encoding="utf8") as f:
            print("Inserting districts ...")
            file_data = json.load(f)
            districts.insert_many(loads(json.dumps(file_data)))
            print("Inserting districts done!")
        if 'Restaurant' not in collection_names:
            db.create_collection(name='Restaurant')
        restaurants = db['Restaurant']
        restaurants.delete_many({})
        with open('Data/Restaurant.json', encoding="utf8") as f:
            print("Inserting restaurants ...")
            file_data = json.load(f)
            restaurants.insert_many(loads(json.dumps(file_data)))
            print("Inserting restaurants done!")
        if 'Menu' not in collection_names:
            db.create_collection(name='Menu')
        menus = db['Menu']
        menus.delete_many({})
        with open('Data/Menu.json', encoding="utf8") as f:
            print("Inserting menus ...")
            file_data = json.load(f)
            menus.insert_many(loads(json.dumps(file_data)))
            print("Inserting menus done!")
        if 'Comment' not in collection_names:
            db.create_collection(name='Comment')
        comments = db['Comment']
        comments.delete_many({})
        with open('Data/Comment.json', encoding="utf8") as f:
            print("Inserting comments ...")
            file_data = json.load(f)
            comments.insert_many(loads(json.dumps(file_data)))
            comments.create_index([('menu_id', 1)])
            comments.create_index([('restaurant_id', 1)])

            print("Inserting comments done!")

    print("Preparing database is done")

    rp = RestaurantPicker()
    da = DataSegmentizer()
    ans = input('Do you want to pick a dataset again?(y/n)')
    if ans == 'y':
        print("Now it is time for picking dataset. If you do not want to enter an input. Type 'n'")
        city = input('City:')
        if city == 'n':
            city = None
        district = input('District:')
        if district == 'n':
            district = None
        created_year = input('Created Year:')
        if created_year == 'n':
            created_year = None
        else:
            created_year = int(created_year)
        start_hour = input('Start Hour:')
        if start_hour == 'n':
            start_hour = None
        else:
            start_hour = float(start_hour)
        end_hour = input('End Hour:')
        if end_hour == 'n':
            end_hour = None
        else:
            end_hour = float(end_hour)
        point = input('Point:')
        if point == 'n':
            point = None
        else:
            point = float(point)

        print("Preparing dataset...")
        rp.pick_dataset(city=city, district=district, created_year=created_year, start_hour=start_hour, end_hour=end_hour,
                        point=point)
        print("Dataset prepared")
        print("Now segmentizing dataset...")
        da.segmentize()
        print("Segmentizing done")

    da.analyze_comment_frequencies()
