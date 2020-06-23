import time, datetime
from posixpath import join as urljoin
import requests
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
    df = df[columns]
    return df


def save_userlikes(df, path):
    id_map = df.drop_duplicates('sender_id')[['sender_id', 'name']].set_index('sender_id')
    user_likes = df.groupby('sender_id')['total_likes'].sum()
    user_likes = pd.concat([user_likes, id_map], axis=1).set_index('name')
    user_likes.query('total_likes > 0').plot(kind='bar', y='total_likes', legend=False, title='Total Likes')
    plt.tight_layout()
    plt.savefig(path)


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


def update_total_likes(df):
    path = './pics/chart.png'
    save_userlikes(df, path)
    picture_url = upload_to_image_service(path)
    last_message_time = df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S')
    text = f'All Karma gained since: \n{last_message_time}'
    return post_message(text, picture_url)


messages = get_messages()
df = parse_to_df(messages, self_likes=True)
response = update_total_likes(df)