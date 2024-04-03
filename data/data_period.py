import gzip
import json
import pandas as pd
from collections import defaultdict

def parse_json_gz_files(file_path):
    user_id_list = []
    asin_list = []
    timestamp_list = []
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            user_id_list.append(data['reviewerID'])
            asin_list.append(data['asin'])
            timestamp_list.append(data['unixReviewTime'])
    return user_id_list, asin_list, timestamp_list

def make_dictionary(user_id_list, asin_list, timestamp_list):
    df = pd.DataFrame({'user_id': user_id_list, 'item_id': asin_list, 'timestamp': timestamp_list})
    df['user_id'], _ = pd.factorize(df['user_id'])
    df['item_id'], _ = pd.factorize(df['item_id'])

    # 딕셔너리 초기화
    user_interactions = {}

    # 사용자별로 아이템 리스트 생성
    for _, row in df.iterrows():
        user_id = row['user_id']
        item_id = row['item_id']
        timestamp = row['timestamp']
        
        # 딕셔너리에 사용자가 없는 경우 빈 리스트를 초기화
        if user_id not in user_interactions:
            user_interactions[user_id] = []
        
        # 아이템과 타임스탬프를 리스트로 묶어서 딕셔너리에 추가
        user_interactions[user_id].append([item_id, timestamp])

    return user_interactions

# def calculate_purchase_periods(interactions):
#     user_periods = defaultdict(dict)

#     for user, interactions_list in interactions.items():
#         item_timestamps = defaultdict(list)
        
#         # 각 아이템별로 구매한 timestamp 기록
#         for item, timestamp in interactions_list:
#             item_timestamps[item].append(timestamp)
        
#         # 각 아이템의 구매 주기 계산
            
#         for item, timestamps in item_timestamps.items():
#             if len(timestamps) <= 1:  # 아이템을 한 번만 구매한 경우
#                 user_periods[user][item] = 0
#             else:
#                 # 구매 주기 계산
#                 intervals = [abs(timestamps[i] - timestamps[i-1]) for i in range(1, len(timestamps))]
#                 average_interval = sum(intervals) / len(intervals)
#                 user_periods[user][item] = average_interval
    
#     return user_periods
def calculate_purchase_periods(interactions):
    user_periods = defaultdict(list)

    for user, interactions_list in interactions.items():
        item_timestamps = defaultdict(list)
        
        # 각 아이템별로 구매한 timestamp 기록
        for item, timestamp in interactions_list:
            item_timestamps[item].append(timestamp)
        
        # 각 아이템의 구매 주기 계산
        for item, timestamps in item_timestamps.items():
            if len(timestamps) <= 1:  # 아이템을 한 번만 구매한 경우
                user_periods[user].append([item, 0])
            else:
                # 구매 주기 계산
                intervals = [abs(timestamps[i] - timestamps[i-1]) for i in range(1, len(timestamps))]
                average_interval = sum(intervals) / len(intervals)
                user_periods[user].append([item, average_interval])
    
    return user_periods

# 파일 경로
file_path = 'Grocery_and_Gourmet_Food/Grocery_and_Gourmet_Food_5.json.gz'

# JSON 파일 파싱
user_id_list, asin_list, timestamp_list = parse_json_gz_files(file_path)

# 사용자-상호작용 딕셔너리 생성
user_interactions = make_dictionary(user_id_list, asin_list, timestamp_list)

# 구매 주기 계산
user_periods = calculate_purchase_periods(user_interactions)

