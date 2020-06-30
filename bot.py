import time, datetime
from posixpath import join as urljoin
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from secrets import token


bot_id = '24b5cb92c298e2b3e6e81ec593'
group_id = 60402884

base_url = 'https://api.groupme.com/v3'

def s_datetime(ms):
    return datetime.datetime(1970,1,1) + datetime.timedelta(0, ms)

def get_liked_messages():
    url = urljoin(base_url, f'groups/{group_id}/likes')
    params = {'token': token, 'period':'month'}
    response = requests.get(url=url, params=params)
    assert response.status_code == 200, 'get_liked_messages request failed: ' + str(response.json()['meta'])
    return response.json()['response']['messages']


def remove_self_likes(msg):
    users = [usr for usr in msg['favorited_by'] if usr != msg['user_id']]
    msg['favorited_by'] = users


def get_messages():
    messages = []
    before_id = None
    url = urljoin(base_url, f'groups/{group_id}/messages')
    while True:
        params = {'token': token, 'limit': 100, 'before_id': before_id}
        response = requests.get(url=url, params=params)
        status_code = response.status_code
        if status_code != 200:
            codes = {420: 'rate limited', 304: 'end of history'}
            print(f'Requests ended due to: {codes.get(status_code, "")} by {status_code}')
            break
        msg_list = response.json()['response']['messages']
        messages += msg_list
        before_id = msg_list[-1]['id']
    return messages


def parse_to_df(messages, self_likes=True):
    if not self_likes:
        for msg in messages:
            remove_self_likes(msg)
    df = pd.DataFrame(messages)
    df['timestamp'] = df['created_at'].apply(s_datetime)
    df.sort_values('timestamp', ascending=False, inplace=True)
    df['total_likes'] = df['favorited_by'].str.len()
    columns = ['id', 'name', 'sender_id', 'text', 'user_id', 'timestamp', 'total_likes']
    df = df[df.columns]
    return df


def parse_like_pairs(df):
    id_map = df.drop_duplicates('sender_id')[['sender_id', 'name']].set_index('sender_id')
    likes = {}
    for i, row in df.iterrows():
        sender = row['sender_id']
        likers = row['favorited_by']
        if not likers:
            continue
        for liker in likers:
            key = (sender, liker)
            if key not in likes:
                likes[key] = 1
            else:
                likes[key] += 1
    like_pairs = pd.Series(likes, name='likes')
    like_pairs.sort_values(ascending=False, inplace=True)
    like_pairs.index.names = ['sender', 'liker']
    mapper = id_map['name'].to_dict()
    like_pairs_mapping = lambda x: (mapper.get(x[0], np.nan), mapper.get(x[1], np.nan))
    like_pairs.index = like_pairs.index.map(like_pairs_mapping)
    return like_pairs


def upload_to_image_service(path):
    data = open(path, 'rb').read()
    url = 'https://image.groupme.com/pictures'
    headers = {'Content-Type': 'image/jpeg', 'X-Access-Token': token}
    response = requests.post(url=url, data=data, headers=headers)
    status_code = response.status_code
    assert status_code == 200, f'image service upload request failed: with status_code {status_code}'
    picture_url = response.json()['payload']['picture_url']
    return picture_url


def post_message(text, picture_url=None):
    url = urljoin(base_url, 'bots/post')
    headers = {'X-Access-Token': token}
    params = {
        'bot_id': bot_id,
        'text': text,
        'picture_url': picture_url
        }
    return requests.post(url=url, params=params, headers=headers)

def update_aggregate(df, path, agg_func, msg):
    agg_func(df)
    picture_url = upload_to_image_service(path)
    return post_message(msg, picture_url)

class TotalLikes:
    path = './pics/total_likes.png'

    def agg_func(self, df):
        agg_col_name = 'total_likes'
        id_map = df.drop_duplicates('sender_id')[['sender_id', 'name']].set_index('sender_id')
        agg = df.groupby('sender_id')['total_likes'].sum()
        agg = pd.concat([agg, id_map], axis=1).set_index('name')
        agg.sort_values(agg_col_name, ascending=False).iloc[:5, :].plot(kind='bar', y=agg_col_name, legend=False, title='Total Likes')
        plt.tight_layout()
        plt.savefig(self.path)

    def update(self, df):
        last_message_time = df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S')
        msg = f'All Karma gained since: \n{last_message_time}'
        return update_aggregate(df, self.path, self.agg_func, msg)

class TotalMessages:
    path = './pics/total_messages.png'

    def agg_func(self, df):
        agg_col_name = 'total_messages'
        id_map = df.drop_duplicates('sender_id')[['sender_id', 'name']].set_index('sender_id')
        agg = df.groupby('sender_id').size()
        agg.name = agg_col_name
        agg = pd.concat([agg, id_map], axis=1).set_index('name')
        agg.sort_values(agg_col_name, ascending=False).iloc[:5, :].plot(kind='bar', y=agg_col_name, legend=False, title='Total Messages')
        plt.tight_layout()
        plt.savefig(self.path)

    def update(self, df):
        last_message_time = df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S')
        msg = f'All Messages sent since: \n{last_message_time}'
        return update_aggregate(df, self.path, self.agg_func, msg)

messages = get_messages()
df = parse_to_df(messages, self_likes=True)
like_pairs = parse_like_pairs(df)

#print(df.head().to_string())
#for obj in [TotalLikes(), TotalMessages()]:
#    response = obj.update(df)


#list(zip(*arrays))
