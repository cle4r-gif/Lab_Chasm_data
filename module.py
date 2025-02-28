#!/usr/bin/env python
# coding: utf-8

# # parameters

# In[2]:


# path
artist_meta_file_path = "{root_path}/data/meta/artist_meta.csv"
artist_file_path = "{root_path}/data/daily/{date}/artist_{platform}.csv"
song_file_path   = "{root_path}/data/daily/{date}/song_{platform}.csv"
album_file_path  = "{root_path}/data/daily/{date}/album_{platform}.csv"
video_file_path = "{root_path}/data/daily/{date}/video_{platform}.csv"
artist_summary_file_path = "{root_path}/data/daily/{date}/artist.csv"

# url
spotify_url = 'https://open.spotify.com/artist/{artistid}'
instagram_url = 'https://instagram.com/{artistid}'
x_url = 'https://x.com/{artistid}'

melon_artist_songs_url   = "https://www.melon.com/artist/song.htm?artistId={artistid}#params%5BlistType%5D=A&params%5BorderBy%5D=ISSUE_DATE&params%5BartistId%5D={artistid}&po=pageObj&startIndex={song_idx}"
melon_artist_albums_url = "https://www.melon.com/artist/album.htm?artistId={artistid}#params%5BlistType%5D=0&params%5BorderBy%5D=ISSUE_DATE&params%5BartistId%5D={artistid}&po=pageObj&startIndex={album_idx}"

genie_artist_url        = "https://www.genie.co.kr/detail/artistInfo?xxnm={artistid}"
genie_artist_songs_url  = "https://www.genie.co.kr/detail/artistSong?xxnm={artistid}"
genie_artist_albums_url = "https://www.genie.co.kr/detail/artistAlbum?xxnm={artistid}"
genie_song_url = "https://www.genie.co.kr/detail/songInfo?xgnm={songid}"


# # Library

# In[2]:


import pandas as pd

import requests

import selenium
print(selenium.__version__)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager as CM

from bs4 import BeautifulSoup
from bs4 import BeautifulSoup as bs

import re
from datetime import date
import json

import time
import random
import os


# # 공통 함수

# In[ ]:


page_load_wait_sec = 10

def get_random_wait_sec(wait_sec_min, wait_sec_max):
    return random.randint(wait_sec_min*1000, wait_sec_max*1000) / 1000


# # Spotify 관련 함수

# In[ ]:


import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

#url에서 ID 추출
def extract_artist_id(spotify_url):
    try:
        return spotify_url.split("artist/")[1].split('?')[0]
    except Exception as e:
        return None

# 문자열에서 숫자만 추출(ex.'77,932 monthly listeners' -> 77932)
def extract_numbers(input_string):
    numbers = re.findall(r'\d+', input_string)
    return int(''.join(numbers))

# 스크랩으로 월간 청취자 추출
def get_listener(spotify_url):
    try:
        response = requests.get(spotify_url)
        print(f"Response Status Code: {response.status_code}")
        soup = BeautifulSoup(response.text, 'html.parser')
        listener_data = extract_numbers(soup.find('div', {'data-testid': 'monthly-listeners-label'}).text.strip())
        error_occur=False
    except Exception as e:
        listener_data = None
        error_occur = True
    return listener_data, error_occur



# api로 팔로워와 popularity 추출
def get_follower_popularity(artist_id_spotify, sp):
    try:
        api_results = sp.artist(artist_id_spotify)
        follower_data = api_results['followers']['total']
    except Exception as e:
        print('follower: error_occur')
        follower_data = None
    try:
        popularity_data = api_results['popularity']
    except Exception as e:
        print('popularity: error_occur')
        popularity_data = None
    return follower_data, popularity_data



# # Youtube  관련 함수

# In[ ]:


# !pip install google-api-python-client


# In[ ]:


from googleapiclient.discovery import build

def get_subscriber_count(api_service, channel_id):
    try:
        request = api_service.channels().list(
            part="statistics",
            id=channel_id
        )
        response = request.execute()
        if response['items']:
            return int(response['items'][0]['statistics']['subscriberCount'])
        else:
            print("채널 정보를 찾을 수 없습니다.")
            return None
    except Exception as e:
        print(f"구독자를 가져오는 중 오류 발생: {e}")
        return None

def get_video_ids(api_service, channel_id):
    try:
        request = api_service.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        if response['items']:
            uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            video_ids = []
            next_page_token = None
            while True:
                playlist_request = api_service.playlistItems().list(
                    part="contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                playlist_response = playlist_request.execute()
                for item in playlist_response['items']:
                    video_ids.append(item['contentDetails']['videoId'])
                next_page_token = playlist_response.get('nextPageToken')
                if not next_page_token:
                    break
            return video_ids
        else:
            print("채널 정보를 찾을 수 없습니다.")
            return []
    except Exception as e:
        print(f"비디오 ID를 가져오는 중 오류 발생: {e}")
        return []

def get_video_views(api_service, video_id):
    try:
        request = api_service.videos().list(
            part="statistics",
            id=video_id
        )
        response = request.execute()
        if response['items']:
            return int(response['items'][0]['statistics']['viewCount'])
        else:
            print("비디오 정보를 찾을 수 없습니다.")
            return 0
    except Exception as e:
        print(f"비디오 조회수를 가져오는 중 오류 발생: {e}")
        return 0


# # Melon 관련 함수

# In[ ]:


def get_artist_info(html):
  follower_cnt = html.find(id="d_like_count").text # ex. "89,694"
  follower_cnt = int(follower_cnt.replace(",", ""))
  song_cnt = html.find('a', 'ico_radio on').text # ex. "발매(88)"
  song_cnt = int(song_cnt.split("(")[1].strip(")"))
  return follower_cnt, song_cnt

def get_song_info(html):
  song_name = html.find('a', 'btn_icon_detail').text
  like_cnt = html.find('span', 'cnt').text # ex. "\n총건수\n12,036"
  try:
    like_cnt = int((like_cnt.split("\n")[-1]).replace(",", ""))
  except:
    like_cnt = 0
  return song_name, like_cnt

def get_album_info(html):
  album_type = html.find('span', 'vdo_name').text
  album_name = html.find('a', 'ellipsis').text
  like_cnt = html.find('a', 'btn_like d_btn').text
  try:
    like_cnt = int((like_cnt.split("총건수")[1]).replace(",", ""))
  except:
    like_cnt = 0
  return album_type, album_name, like_cnt


# In[ ]:





# # Instagram 관련 함수

# In[1]:


def login(bot, username, password):
    bot.get('https://www.instagram.com/accounts/login/')
    time.sleep(5)
    # Check if cookies need to be accepted
    try:
        element = bot.find_element(By.XPATH, "/html/body/div[4]/div/div/div[3]/div[2]/button")
        element.click()
    except NoSuchElementException:
        print("[Info] - Instagram did not require to accept cookies this time.")

    print("[Info] - Logging in...")
    username_input = WebDriverWait(bot, 60).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='username']")))
    password_input = WebDriverWait(bot, 60).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='password']")))
    username_input.clear()
    username_input.send_keys(username)
    password_input.clear()
    password_input.send_keys(password)

    # login_button = WebDriverWait(bot, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']")))
    # login_button.click()
    password_input.send_keys(Keys.RETURN)
    time.sleep(10)


def scrape_insta(username, password, url_lst, artist_lst):

    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--lang=en")
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")  # 창 크기 지정
    options.add_argument("--disable-gpu")  # GPU 비활성화 (Headless 안정성 증가)
    options.add_argument("--disable-dev-shm-usage")  # 메모리 부족 문제 해결
    options.add_argument('--no-sandbox')
    options.add_argument("--log-level=3")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    # mobile_emulation = {
    #     "userAgent": "Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 5 Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/90.0.1025.166 Mobile Safari/535.19"}
    # options.add_experimental_option("mobileEmulation", mobile_emulation)


    bot = webdriver.Chrome(service=service, options=options)
    bot.set_page_load_timeout(15) # Set the page load timeout to 15 seconds

    login(bot, username, password)

    followers = []
    for artist, url in zip(artist_lst, url_lst):
        if pd.isna(url):
            followers_count = None
            followers.append({'artist_name': artist, 'instagram_follower_cnt': followers_count})
            continue
        if '*' in artist:
            followers_count = '*'
            followers.append({'artist_name': artist, 'instagram_follower_cnt': followers_count})
            continue
        try:
            bot.get(url)
            artist_name = url.split('.com/')[1].split('/')[0]
            print(f"[Info] - Scraping # of followers for {artist}, ID:{artist_name}...")
            time.sleep(random.uniform(8, 20))
            
            # Locate the parent <a> tag
            parent_a_tag = None
            for _ in range(10):  # Retry up to 10 times
                try:
                    parent_a_tag = bot.find_element(
                        By.XPATH,
                        f"//a[@href='/{artist_name}/followers/']"
                    )
                    break
                except NoSuchElementException:
                    time.sleep(2)  # Wait for 2 seconds before retrying
            
            if parent_a_tag is None:
                raise NoSuchElementException("Parent <a> tag not found")

            # Find the nested <span> with the title attribute
            followers_element = parent_a_tag.find_element(
                By.XPATH,
                ".//span[@title]"
            )

            followers_count = followers_element.get_attribute('title')

            if ',' in followers_count:
                followers_count = int(followers_count.replace(',',''))
            print(followers_count)
        except Exception as e:
            print(e)
            followers_count = None
        followers.append({'artist_name':artist, 'instagram_follower_cnt': followers_count})
    bot.quit()
    return followers


# # X 관련 함수

# In[ ]:


def login_X(bot, username, password):
    bot.get('https://x.com/i/flow/login')
    time.sleep(5)
    print("[Info] - Logging in...")
    username_input = WebDriverWait(bot, 10).until(
        EC.presence_of_element_located((By.XPATH, '//input[@name="text"]'))
    )
    username_input.send_keys(username)
    username_input.send_keys(Keys.RETURN)
    time.sleep(1.5)
    password_input = WebDriverWait(bot, 10).until(
        EC.presence_of_element_located((By.XPATH, '//input[@name="password"]'))
    )
    password_input.send_keys(password)
    password_input.send_keys(Keys.RETURN)

    time.sleep(10)

def scrape_X(username, password, url_lst, artist_lst):
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--lang=en")
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")  # 창 크기 지정
    options.add_argument("--disable-gpu")  # GPU 비활성화 (Headless 안정성 증가)
    options.add_argument("--disable-dev-shm-usage")  # 메모리 부족 문제 해결
    options.add_argument('--no-sandbox')
    options.add_argument("--log-level=3")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    # mobile_emulation = {
        # "userAgent": "Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 5 Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/90.0.1025.166 Mobile Safari/535.19"}
    # options.add_experimental_option("mobileEmulation", mobile_emulation)

    bot = webdriver.Chrome(service=service, options=options)
    bot.set_page_load_timeout(15)  # Set the page load timeout to 15 seconds

    login_X(bot, username, password)

    followers = []

    for artist, url in zip(artist_lst, url_lst):
        if pd.isna(url):
            followers_count = None
            followers.append({'artist_name': artist, 'X_follower_cnt': followers_count})
            continue
        if '*' in artist:
            followers_count = '*'
            followers.append({'artist_name': artist, 'X_follower_cnt': followers_count})
            continue
        try:
            bot.get(url)
            artist_name = url.split('.com/')[1].split('/')[0]
            print(f"[Info] - Scraping # of followers for {artist}, ID:{artist_name}...")
            time.sleep(random.uniform(5, 20))

            # Wait until the script tag is found or timeout
            script_tag = None
            for _ in range(10):  # Retry up to 10 times
                try:
                    script_tag = bot.find_element(By.XPATH, "//script[@type='application/ld+json' and @data-testid='UserProfileSchema-test']")
                    break
                except NoSuchElementException:
                    time.sleep(2)  # Wait for 2 seconds before retrying

            if script_tag is None:
                raise NoSuchElementException("Script tag not found")

            json_data = script_tag.get_attribute('innerHTML')
            data = json.loads(json_data)

            # Extract the followers count
            for interaction in data['mainEntity']['interactionStatistic']:
                if interaction['name'] == 'Follows':
                    followers_count = interaction['userInteractionCount']
                    break

            print(followers_count)
        except Exception as e:
            print(e)
            followers_count = None
        followers.append({'artist_name': artist, 'X_follower_cnt': followers_count})
    bot.quit()
    return followers

