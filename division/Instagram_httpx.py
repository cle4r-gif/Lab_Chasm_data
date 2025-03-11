import json
import httpx
import pandas as pd
import time
import random
from datetime import date
client = httpx.Client(
    headers={
        # this is internal ID of an instegram backend app. It doesn't change often.
        "x-ig-app-id": "936619743392459",
        # use browser-like features
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
    }
)

def scrape_user(username: str):
    try:
        """Scrape Instagram user's data"""
        result = client.get(
            f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}",
        )
        data = json.loads(result.content)
        profile = data['data']['user']
    except Exception as e:
        profile = None
    return profile

df = pd.read_csv("artist_meta.csv")

follower_lst = []
for i, row in df.iterrows():
    artist_id = row['artist_id']
    artist_id_instagram = row['artist_id_instagram']
    profile = scrape_user(artist_id_instagram)
    if profile == None:
        followers = 0
        time.sleep(600)
        profile = scrape_user(artist_id_instagram)
    else:
        followers = profile['edge_followed_by']['count']
    print(artist_id_instagram, followers)  
    follower_lst.append({'artist_id':artist_id, 'artist_id_instagram':artist_id_instagram, 'followers':followers})
    time.sleep(random.uniform(90,120))

df['followers'] = follower_lst
df.to_csv(f'{date.today()} followers_instagram.csv')