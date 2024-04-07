import gzip
import pickle as pkl
from collections import defaultdict
import numpy as np
import pandas as pd
import json

def parse(path):
    with gzip.open(path, 'rb') as f:
        for line in f:
            yield json.loads(line)
# get txt file
def write_seq(User, dataset_name):
    f = open(f'{dataset_name}/{dataset_name}.txt','w')
    for user in User.keys():
        f.write('%d '%(user))
        for interaction in User[user]:
            f.write('%d ' %(interaction[0])) # extract itemid
        f.write('\n')
            
def get_items_meta(meta_path, categories_used='all'):
    # item2price = {}
    item2category = {}
    item2brand = {}

    if categories_used == 'all':
        for l in parse(meta_path):

            asin = l['asin']
            item2category[asin] = l['category']
            # item2price[asin] = l['price'][1:] if 'price' in l else 0.0
            item2brand[asin] = l['brand'] if 'brand' in l else ''
    else:
        for l in parse(meta_path):
            asin = l['asin']
            item2category[asin] = l['category'][0] if l['category'] else ''  # 첫 번째 카테고리만 사용
            # item2price[asin] = l['price'][1:] if 'price' in l else 0.0
            item2brand[asin] = l['brand'] if 'brand' in l else ''

    items_meta = {
        # 'item2price': item2price,
        'item2category': item2category,
        'item2brand': item2brand
    }
    return items_meta
# item purchase period
def calculate_purchase_intervals(interactions):
    user_items = defaultdict(list)

    for user, interactions_list in interactions.items():
        item_last_timestamp = {} 

        for itemid, timestamp_list in interactions_list:
            timestamp = timestamp_list[2]
            if itemid in item_last_timestamp:
                time_diff = abs(timestamp - item_last_timestamp[itemid])
                user_items[user].append([itemid, timestamp_list,time_diff]) 
            else:
                user_items[user].append([itemid, timestamp_list,0])

            item_last_timestamp[itemid] = timestamp

    return user_items
### 
def generate_data(dataset_name, reviews_path, meta_path):
    category_used_list = ['all']
    min_units = ['multi_word']

    for categories_used in category_used_list:
        for min_unit in min_units:
            user2id = {'[PAD]': 0}
            item2id = {'[PAD]': 0}
            items_map = {
                # 'item2price': {},
                'item2category': {},
                'item2brand': {}
            }
            user_reviews = defaultdict(list)
            action_times = []
            items_meta = get_items_meta(meta_path, categories_used)

            for l in parse(reviews_path):
                if l['reviewerID'] not in user2id:
                    user2id[l['reviewerID']] = len(user2id)
                action_times.append(l['unixReviewTime'])
                user_reviews[l['reviewerID']].append([l['asin'], l['unixReviewTime']])
            ###
            for u in user_reviews:
                user_reviews[u].sort(key=lambda x: x[1])
                for item, time in user_reviews[u]:
                    if item not in item2id:
                        item2id[item] = len(item2id)
            item2_id_list=[] 
            for item in item2id.keys():
                item2_id_list.append(item)  # review item 41321
            
            items_meta_itemid_list=[]
            not_match_item_id=[]
            items_meta_itemid_list=set(list(items_meta['item2brand'].keys())) # meta data itemid
            
            for item in item2_id_list:
                if item not in items_meta_itemid_list:
                      not_match_item_id.append(item)# meta item
            not_match_item_id.remove('[PAD]')  # 40

            #change itemid: 41321->41281

            remove_unmatch_item_item2id={}
            for itemid in item2id:
                if itemid not in not_match_item_id: 
                    if itemid not in remove_unmatch_item_item2id.keys(): # remove duplicate & first appear
                        remove_unmatch_item_item2id[itemid] = len(remove_unmatch_item_item2id)
            
            item2id=remove_unmatch_item_item2id

            #items_map
            for u in user_reviews:
                user_reviews[u].sort(key=lambda x: x[1])
                for item, time in user_reviews[u]:
                    for s in ['item2category', 'item2brand']:  # item2price remove
                        if item in items_meta[s] and item not in not_match_item_id:
                            items_map[s][item] = items_meta[s][item]  # item_map=41280
            ### 

            brand2id = {'[PAD]': 0} 
            item2brand_id = {}
            for k in items_map['item2brand'].keys():
                if items_map['item2brand'][k] in brand2id:
                    item2brand_id[k] = brand2id[items_map['item2brand'][k]]
                else:
                    brand2id[items_map['item2brand'][k]] = len(brand2id)
                    item2brand_id[k] = brand2id[items_map['item2brand'][k]]

            category2id = {'[PAD]': 0}
            item2category_id = defaultdict(list)
            categories_n_max = 0
            if min_unit == 'single_word':
                for k in items_map['item2category'].keys():
                    for category in items_map['item2category'][k]:
                        for w in category.split(" "):
                            if w not in category2id:
                                category2id[w] = len(category2id)
                            if category2id[w] not in item2category_id[k]:
                                item2category_id[k].append(category2id[w])
                    categories_n_max = len(item2category_id[k]) if len(
                        item2category_id[k]) > categories_n_max else categories_n_max
            else:
                for k in items_map['item2category'].keys():
                    for category in items_map['item2category'][k]:
                        if category not in category2id:
                            category2id[category] = len(category2id)
                        if category2id[category] not in item2category_id[k]:
                            item2category_id[k].append(category2id[category])
                    categories_n_max = len(item2category_id[k]) if len(
                        item2category_id[k]) > categories_n_max else categories_n_max

            # item_features = {0: [0] * (1 + categories_n_max + 1)}
            item_features = {0: [0] * (categories_n_max + 1)}
            for k in items_map['item2brand'].keys():
                category_feature = item2category_id[k] + (categories_n_max - len(item2category_id[k])) * [0]
                item_feature = category_feature + [item2brand_id[k]] #[items_map['item2price'][k]]+category_feature + [item2brand_id[k]]
                assert len(item_feature) == len(item_features[0])
                item_features[item2id[k]] = item_feature

            item_features = list(item_features.values())

            min_year = pd.to_datetime(np.array(action_times).min(), unit='s').year
            max_year = pd.to_datetime(np.array(action_times).max(), unit='s').year

            # user_review remove unmatch itemid

            User = defaultdict(list)
            for u in user_reviews.keys():
                for item, action_time in user_reviews[u]:
                    act_datetime = pd.to_datetime(action_time, unit='s')
                    year = (act_datetime.year - min_year) / (max_year - min_year)
                    month = act_datetime.month / 12
                    day = act_datetime.day / 31
                    dayofweek = act_datetime.dayofweek / 7
                    dayofyear = act_datetime.dayofyear / 365
                    week = act_datetime.week / 4
                    context = [year, month, day, dayofweek, dayofyear, week]
                    if item not in not_match_item_id:
                        User[user2id[u]].append([item2id[item], context])
            #item period extract

            item_period = calculate_purchase_intervals(User)

            data = {
                'user_seq': user_reviews,
                'items_map': items_map,
                'user_seq_token': User,
                'items_feat': item_features,
                'user2id': user2id,
                'item2id': item2id,
                'category2id': category2id,
                'brand2id': brand2id,
                'max_categories_n': categories_n_max,
                'item_period': item_period
            }

            pkl.dump(data, open(f'{dataset_name}/{dataset_name}_{categories_used}_{min_unit}.dat', 'wb'))
            print(f'generate_data:{dataset_name}_{categories_used}_{min_unit}.dat has finished!')

            write_seq(User,'Grocery_and_Gourmet_Food')  # write seq txt file 

 
if __name__ == '__main__':
    for dataset_name in ['Grocery_and_Gourmet_Food']:
        generate_data(dataset_name, f'{dataset_name}/{dataset_name}_5.json.gz',
                      f'{dataset_name}/meta_{dataset_name}.json.gz')