
root_path = '.' # !pwd 실행 결과

from module import *

date = date.today().strftime("%Y-%m-%d")
print(date)


if not os.path.exists(f"{root_path}/data/daily/{date}".format(root_path, date)):
    os.makedirs(f"{root_path}/data/daily/{date}".format(root_path, date))

df = pd.read_csv('artist_meta_for_check.csv')
df

instagram_username = 'botbotnotsaram'
instagram_password = 'botforthesaram@'
cookie = "botforthesaram@"
url_lst = [instagram_url.format(artistid=artist_id) if not pd.isna(artist_id) else None for artist_id in df['artist_id_instagram']]

instagram_followers = scrape_insta(
    username = instagram_username, password = instagram_password,
    url_lst = url_lst, artist_lst = df['artist_name'], artist_id_lst = df['artist_id'])

df_insta = pd.DataFrame(instagram_followers)
df_insta.to_csv(artist_file_path.format(root_path=root_path, date=date, platform='instagram'), index=False)
df_insta
