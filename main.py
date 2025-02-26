#!/usr/bin/env python
# coding: utf-8

# In[1]:


root_path = '.' # !pwd 실행 결과


# # Module

# In[ ]:


get_ipython().run_line_magic('run', './module.py')


# In[5]:


date = date.today().strftime("%Y-%m-%d")
print(date)


# # Main

# ## 0. Setting

# In[6]:


if not os.path.exists(f"{root_path}/data/daily/{date}".format(root_path, date)):
    os.makedirs(f"{root_path}/data/daily/{date}".format(root_path, date))


# In[7]:


df = pd.read_csv('artist_meta.csv')
df


# ## 1. Spotify (api o)
# * df_spotify : ```artist_name | artist_id | artist_id_spotify | spotify_{monthly_listner, follower_cnt, popularity}```

# In[8]:


spotify_client_id = '92628e1c3af84c00b03953362a8d9b38'
spotify_client_secret = '3a159c7819fe4af9bb81c592beefdbdb'

client_credentials_manager = SpotifyClientCredentials(client_id = spotify_client_id, client_secret = spotify_client_secret )
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


# In[10]:


spotify_listener= []
spotify_listener_error = []
spotify_follower_popularity = []

for i, row in df.iterrows():
    artist_id         = row['artist_id']
    artist_id_spotify = row['artist_id_spotify']
    url = spotify_url.format(artistid=artist_id_spotify)
    artist_name = row['artist_name']

    print(artist_name)

    listener_data, error_occur = get_listener(url)

    if error_occur and not (pd.isna(artist_id_spotify)):
        spotify_listener_error.append(i)
    else:
        spotify_listener.append({
            'artist_id':artist_id, 'artist_id_spotify':artist_id_spotify, 'artist_name':artist_name,
            'spotify_monthly_listener': listener_data
        })

    follower_data, popularity_data = get_follower_popularity(url)
    spotify_follower_popularity.append({
        'artist_id':artist_id, 'artist_id_spotify':artist_id_spotify, 'artist_name':artist_name,
        'spotify_follower_cnt': follower_data, 'spotify_popularity': popularity_data
    })
    time.sleep(get_random_wait_sec(1, 3))

# 페이지 크롤링으로 수집하는 월간 청취자 데이터 쪽에서 가끔씩 호출오류 생김
# 앞서 호출 오류 생긴 애들만 다시 크롤링(될 때까지)
while spotify_listener_error:
    artist_name = df.loc[spotify_listener_error[0], 'name']
    print(df.loc[spotify_listener_error[0], 'name'])
    listener_data, error_occur = get_listener(df.loc[spotify_listener_error[0], 'spotify_url'])
    if not error_occur:
        spotify_listener.append({
            'artist_id':spotify_listener_error.pop(0),
            'artist_id_spotify':artist_id_spotify, 'artist_name':artist_name,
            'spotify_monthly_listener': listener_data
        })
        (get_random_wait_sec(1, 2))


# In[11]:


# artist_name | artist_id | artist_id_spotify | spotify_{monthly_listner, follower_cnt, popularity}
df_spotify = pd.DataFrame(spotify_listener)
df_spotify_follower_popularity = pd.DataFrame(spotify_follower_popularity)
df_spotify = df_spotify.merge(df_spotify_follower_popularity, on=['artist_id', 'artist_id_spotify', 'artist_name'], how='right')
df_spotify.to_csv(artist_file_path.format(root_path=root_path, date=date, platform='spotify'), index=False)
df_spotify


# ## 2. Youtube (api o)
# * df_youtube : ```artist_name | artist_id | artist_id_youtube | youtube_{follower_cnt, video_cnt, total_views}```

# In[12]:


# Quota가 부족해서 오류 발생할 수 있음
# api 두 개 있으므로 한쪽 Quota 넘칠 경우 다른 쪽 주석 해제해서 사용
youtube_api_key_1 = 'AIzaSyBam8SFhHQf9ImS2bhpbjlQQ2k0HNkT20A'
youtube_api_key_2 = 'AIzaSyD-Iv1vOcKsaBb-3tuwKxqePvTcw76jIcQ'

api_service = build('youtube', 'v3', developerKey=youtube_api_key_1)


# In[14]:


# 메인 실행 코드
youtube_channel_lst = []
youtube_video_lst = []

for i, row in df.iterrows():
    artist_id = row['artist_id']
    artist_id_youtube = row['artist_id_youtube']
    artist_name = row['artist_name']

    if '*' in artist_name:
        subscribers = '*'
        total_views = '*'
        youtube_channel_lst.append({
        'artist_id':artist_id, 'artist_id_youtube':artist_id_youtube, 'artist_name':artist_name,
        'youtube_follower_cnt': subscribers, 'youtube_video_cnt': video_cnt,'youtube_total_views': total_views
    })
        continue
    if pd.isna(artist_id_youtube):
        subscribers = None
        total_views = None
        video_cnt = None
        youtube_channel_lst.append({
        'artist_id':artist_id, 'artist_id_youtube':artist_id_youtube, 'artist_name':artist_name,
        'youtube_follower_cnt': subscribers, 'youtube_video_cnt': video_cnt,'youtube_total_views': total_views
    })
        continue
    try:
        subscribers = get_subscriber_count(api_service, artist_id_youtube)
        video_id_lst = get_video_ids(api_service, artist_id_youtube)
        
        video_view_lst = []
        for video_id in video_id_lst:
            video_view = get_video_views(api_service, video_id)
            video_view_lst.append(video_view)
            youtube_video_lst.append({'artist_id':artist_id, 'artist_name': artist_name, 'video_id':video_id, 'video_view':video_view})

        video_cnt = len(video_id_lst)
        total_views = sum(video_view_lst)

    except Exception as e:
        print(f"Error processing URL {artist_name}: {e}")
        subscribers = None
        total_views = None

    print(artist_name)
    print(f"Subscribers: {subscribers}, Video counts: {len(video_id_lst)}, Total Views: {total_views}")
    youtube_channel_lst.append({
        'artist_id':artist_id, 'artist_id_youtube':artist_id_youtube, 'artist_name':artist_name,
        'youtube_follower_cnt': subscribers, 'youtube_video_cnt': video_cnt,'youtube_total_views': total_views
    })
    time.sleep(get_random_wait_sec(1, 2))


# In[15]:


df_youtube = pd.DataFrame(youtube_channel_lst)
df_youtube.to_csv(artist_file_path.format(root_path=root_path, date=date, platform='youtube'), index=False)
df_youtube


# In[16]:


df_youtube_videos = pd.DataFrame(youtube_video_lst)
df_youtube_videos.to_csv(video_file_path.format(root_path=root_path, date=date, platform='youtube'), index=False)
df_youtube_videos


# ## Melon (api x, login x)
# df_melon : ```artist_name | artist_id | artist_id_melon | melon_{follower_cnt, song_cnt, album_cnt} | (total/max/min/mean)_(song/album)_likes}```

# In[17]:


options = webdriver.ChromeOptions()
options.add_argument('--headless') # Head-less 설정
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')


# In[18]:


driver = webdriver.Chrome(options=options)
driver.implicitly_wait(page_load_wait_sec) # 각 페이지 로딩(driver.get())까지의 최대 대기 시간 설정


# In[ ]:


artist_info_df, song_info_df, album_info_df = [], [], []

for i, row in df.iterrows():
  artist_name, artist_id_melon, artist_id = row['artist_name'], row['artist_id_melon'], row['artist_id']
  print(artist_name)

  # song info : song_name / like_cnt
  song_idx = 1
  while song_idx == 1 or song_idx <= song_cnt:
    url = melon_artist_songs_url.format(artistid=artist_id_melon, song_idx=song_idx)
    driver.get(url)
    time.sleep(get_random_wait_sec(0.8, 2.2))
    html = bs(driver.page_source, 'html.parser')
    # ==============================================================
    if song_idx == 1:
      follower_cnt, song_cnt = get_artist_info(html)
      print(f"\tfollower_cnt = {follower_cnt}\n\tsong_cnt = {song_cnt}")
    # ==============================================================
    song_list = html.find(id='frm').find_all('tr')[1:] # 첫번째 값은 column info (생략)
    for song_element in song_list:
      song_name, like_cnt = get_song_info(song_element)
      song_info_df.append([artist_id, artist_id_melon, artist_name, song_name, like_cnt])
      # print("\t", name, song_name, like_cnt)
    song_idx += len(song_list)

  # album info : album_name / album_type / like_cnt
  album_idx = 1
  while album_idx == 1 or album_idx <= album_cnt:
    url = melon_artist_albums_url.format(artistid=artist_id_melon, album_idx=album_idx)
    driver.get(url)
    time.sleep(get_random_wait_sec(0.8, 2.2))
    html = bs(driver.page_source, 'html.parser')
    # ==============================================================
    if album_idx == 1:
      album_cnt = html.find('a', 'ico_radio on').find('span', 'text').text
      album_cnt = int(album_cnt.split("(")[1].strip(")"))
      print(f"\talbum_cnt = {album_cnt}")
    # ==============================================================
    album_list = html.find(id='frm').find_all('li', "album11_li")
    for album_element in album_list:
      album_type, album_name, like_cnt = get_album_info(album_element)
      album_info_df.append([artist_id, artist_id_melon, artist_name, album_name, album_type, like_cnt])
      # print("\t", name, album_name, album_type, like_cnt)
    album_idx += len(album_list)
  artist_info_df.append([artist_id, artist_id_melon, artist_name, follower_cnt, song_cnt, album_cnt])

artist_info_df = pd.DataFrame(artist_info_df, columns=['artist_id', 'artist_id_melon', 'artist_name', 'melon_follower_cnt', 'melon_song_cnt', 'melon_album_cnt'])
song_info_df   = pd.DataFrame(song_info_df,   columns=['artist_id', 'artist_id_melon', 'artist_name', 'song_name',  'melon_song_likes'])
album_info_df  = pd.DataFrame(album_info_df,  columns=['artist_id', 'artist_id_melon', 'artist_name', 'album_name', 'album_type', 'melon_album_likes'])

driver.quit()


# In[ ]:


# aggregate (song / album) info by artistid
song_info_df_agg= (
    song_info_df
    .groupby(['artist_id'])
    .agg(
        melon_total_song_likes = ('melon_song_likes', 'sum'),
        melon_max_song_likes   = ('melon_song_likes', 'max'),
        melon_min_song_likes   = ('melon_song_likes', 'min'),
        melon_mean_song_likes  = ('melon_song_likes', 'mean'),
    )
    .reset_index()
)
album_info_df_agg = (
    album_info_df
    .groupby(['artist_id'])
    .agg(
        melon_total_album_likes = ('melon_album_likes', 'sum'),
        melon_max_album_likes   = ('melon_album_likes', 'max'),
        melon_min_album_likes   = ('melon_album_likes', 'min'),
        melon_mean_album_likes  = ('melon_album_likes', 'mean'),
    )
    .reset_index()
)
# join aggregated (song / album) into to artist table
df_melon = pd.merge(left=artist_info_df, right=song_info_df_agg, how="inner", on='artist_id')
df_melon = pd.merge(left=df_melon, right=album_info_df_agg, how='inner', on='artist_id')


# In[ ]:


df_melon.to_csv(artist_file_path.format(root_path=root_path, date=date, platform='melon'), index=False)
song_info_df.to_csv(song_file_path.format(root_path=root_path, date=date, platform='melon'), index=False)
album_info_df.to_csv(album_file_path.format(root_path=root_path, date=date, platform='melon'), index=False)


# In[ ]:


df_melon


# ## Instagram (api x, login o)
# ```artist_name | artist_id | artist_id_instagram | instagram_follower_cnt```

# In[19]:


instagram_username = 'botbotnotsaram'
instagram_password = 'botforthesaram@'
cookie = "botforthesaram@"


# In[20]:


url_lst = [instagram_url.format(artistid=artist_id) for artist_id in df['artist_id_instagram']]

instagram_followers = scrape_insta(
    username = instagram_username, password = instagram_password,
    url_lst = url_lst, artist_lst = df['artist_name'])


# In[ ]:


df_insta = pd.DataFrame(instagram_followers)
df_insta.to_csv(artist_file_path.format(root_path=root_path, date=date, platform='instagram'), index=False)
df_insta


# ## Youtube Music (api x, login o)
# df_ym : ```artist_name | artist_id | artist_id_youtubemusic | youtubemusic_{total, max, min, mean}_stream_cnt```

# In[ ]:


song_info_df = # ...
song_info_df = song_info_df.rename(columns={'song_likes' : 'youtubemusic_song_likes', 'stream_cnt':'youtubemusic_stream_cnt'})
song_info_df.to_csv(song_file_path.format(root_path=root_path, date=date, platform='youtubemusic'), index=False)


# In[ ]:


stream_cnt_list = []
for i, row in song_info_df.iterrows():
    stream_cnt_str = row['youtubemusic_stream_cnt']
    if '천회' in stream_cnt_str:
        stream_cnt = float(stream_cnt_str.split("천회")[0]) * 1000
    elif '만회' in stream_cnt_str:
        stream_cnt = float(stream_cnt_str.split("만회")[0]) * 10000
    elif '억' in stream_cnt_str:
        stream_cnt = float(stream_cnt_str.split("억")[0]) * 100000000
    else:
        stream_cnt = float(stream_cnt_str.split("회")[0])
    stream_cnt_list.append(stream_cnt)
song_info_df['youtubemusic_stream_cnt'] = stream_cnt_list


# In[ ]:


df_ym = (
    song_info_df
    .groupby(['artist_name', 'artist_id', 'artist_id_youtubemusic'])
    .agg(
        youtubemusic_total_stream_cnt = ('youtubemusic_stream_cnt', 'sum'),
        youtubemusic_max_song_stream_cnt   = ('youtubemusic_stream_cnt', 'max'),
        youtubemusic_min_song_stream_cnt   = ('youtubemusic_stream_cnt', 'min'),
        youtubemusic_mean_song_stream_cnt  = ('youtubemusic_stream_cnt', 'mean'),
    )
    .reset_index()
)


# In[ ]:


df_ym.to_csv(artist_file_path.format(root_path=root_path, date=date, platform='youtubemusic'), index=False)
df_ym


# ## X (api x, login o)
# df_x : ```artist_name | artist_id | artist_id_x | X_follower_cnt```

# In[ ]:


x_username = 'botbotnotsaram'
x_password = 'botforthesaram@'


# In[ ]:


url_lst = url_lst = [x_url.format(artistid=artist_id) for artist_id in df['artist_id_x']]
artist_lst = df['artist_name']

X_followers = scrape_X(x_username, x_password, url_lst, artist_lst)


# In[ ]:


df_x = pd.DataFrame(X_followers)
df_x.to_csv(artist_file_path.format(root_path=root_path, date=date, platform='x'), index=False)

df_x


# ## Merge

# In[ ]:


from functools import reduce

# df_spotify = pd.read_csv(artist_file_path.format(root_path=root_path, date=date, platform='spotify'), index=False)
# df_youtube = pd.read_csv(artist_file_path.format(root_path=root_path, date=date, platform='youtube'), index=False)
# df_insta = pd.read_csv(artist_file_path.format(root_path=root_path, date=date, platform='instagram'), index=False)
# df_X = pd.read_csv(artist_file_path.format(root_path=root_path, date=date, platform='x'), index=False)
# df_melon = pd.read_csv(artist_file_path.format(root_path=root_path, date=date, platform='melon'), index=False)
# df_ym = pd.read_csv(artist_file_path.format(root_path=root_path, date=date, platform='youtubemusic'), index=False)

dfs = [df[['artist_id', 'artist_name']], df_spotify, df_youtube, df_ym, df_insta, df_x, df_melon]
df_all = reduce(lambda left, right: left.merge(right, on=["artist_id", "artist_name"], how="left"), dfs)

df_all.to_csv(artist_file_path.format(root_path=root_path, date=date, platform='all'), index=False)


# In[ ]:


df_all

