from difflib import SequenceMatcher

from pymongo import MongoClient


class CommentMatcher:
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client['YemekSepeti']
        self.comments = self.db['Comment']
        self.restaurants = self.db['Restaurant']
        self.menus = self.db['Menu']

    def add_city_name(self):
        i = 1
        for comment in self.comments.find({'city_catalog_name': None}):
            restaurant = self.restaurants.find_one({'_id': comment['restaurant_id']})
            self.comments.update(
                {"_id": comment["_id"]},
                {"$set": {"city_catalog_name": restaurant["CatalogName"]}})
            print(f'Updated {i}th comment')
            i = i + 1

    def guess_menus(self):
        i = 1
        threshold = 0.7
        all_comments = self.comments.find({'city_catalog_name': 'TR_ANKARA'})
        for comment in all_comments:
            _menus = list(self.menus.find({'restaurant_id': comment['restaurant_id']}))
            match_counts = []
            for k in range(0, len(_menus)):
                match_counts.append(0)
            j = 0
            for menu in _menus:
                c = comment['comment']
                if c is not None:
                    comment_words = c.split(' ')
                    desc = menu['product_name']
                    if desc is None:
                        desc = menu['product_desc']
                    if desc is not None:
                        name_words = desc.split(' ')
                        for word in comment_words:
                            for name_word in name_words:
                                ratio = SequenceMatcher(None, word.lower(), name_word.lower()).ratio()
                                if ratio >= threshold:
                                    match_counts[j] = 1
                                    break
                                # else:
                                #     match_counts[j] += ratio * (1 / len(name_words))

                j = j + 1

            max_value = max(match_counts)
            max_index = match_counts.index(max_value)
            if max_value >= threshold:
                self.comments.update({'_id': comment['_id']}, {
                    "$set": {"menu_id": _menus[max_index]['_id'], "menu_name": _menus[max_index]['product_name']}})
            else:
                self.comments.update({'_id': comment['_id']},
                                     {"$set": {"menu_id": None, "menu_name": None}})

            print("%{:.2f}".format((i / all_comments.count()) * 100))
            i = i + 1
