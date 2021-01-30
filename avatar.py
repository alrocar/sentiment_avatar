import tweepy
import os
import re
from textblob import TextBlob
import time
from datetime import datetime
import requests
import csv
import json

from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# import nltk
# from nltk.sentiment import SentimentIntensityAnalyzer
# sia = SentimentIntensityAnalyzer()

# nltk.download([
#     "names",
#     "stopwords",
#     "state_union",
#     "twitter_samples",
#     "movie_reviews",
#     "averaged_perceptron_tagger",
#     "vader_lexicon",
#     "punkt",
# ])

# # import ipdb; ipdb.set_trace(context=30)
# stopwords = nltk.corpus.stopwords.words("english") + nltk.corpus.stopwords.words("spanish")

targettwitterprofile = 'alrocar'

consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_token_secret = os.environ['ACCESS_TOKEN_SECRET']
tb_token = os.environ['TB_TOKEN']
read_token = os.environ['READ_TOKEN']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

mode = 'append'
datasource = f'{targettwitterprofile}_tweets'
datasource_raw = f'{targettwitterprofile}_tweets_raw'
token = tb_token

url = f'https://api.tinybird.co/v0/datasources?mode={mode}&name={datasource}'
url_raw = f'https://api.tinybird.co/v0/datasources?mode={mode}&name={datasource_raw}'

retry = Retry(total=5, backoff_factor=10)
adapter = HTTPAdapter(max_retries=retry)
_session = requests.Session()
_session.mount('http://', adapter)
_session.mount('https://', adapter)

csv_chunk = StringIO()
writer = csv.writer(csv_chunk, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

csv_chunk_raw = StringIO()
writer_raw = csv.writer(csv_chunk_raw, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

writer.writerow(["id", "date", "text", "polarity"])
writer_raw.writerow(["tweet"])

max_id_url = f'https://api.tinybird.co/v0/pipes/alrocar_timeline_max_id.json?token={read_token}'
# import ipdb; ipdb.set_trace(context=30)
response = _session.get(max_id_url)
since_id = response.json()['data'][0]['since_id']

# for page in tweepy.Cursor(api.user_timeline, id=targettwitterprofile, count=200).pages(20):
for page in tweepy.Cursor(api.home_timeline, since_id=since_id, count=200).pages(20):
    for tweet in page:
        tweetid = tweet.id
        tweetdate = str(tweet.created_at)
        tweettext = tweet.text
        tt = " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", tweettext).split())
        # polarity = sia.polarity_scores(tt)['compound']
        polarity = round(TextBlob(tt).sentiment.polarity,4)

        writer.writerow([tweetid, tweetdate, tweettext, polarity])
        writer_raw.writerow([json.dumps(tweet._json)])

    data = csv_chunk.getvalue()
    data_raw = csv_chunk_raw.getvalue()
    headers = {
        'Authorization': f'Bearer {token}',
        'X-TB-Client': 'alrocar-tweets-0.1',
    }

    if data:
        response = _session.post(url, headers=headers, files=dict(csv=data))
        ok = response.status_code < 400
        if not ok:
            raise Exception(json.dumps(response.json()))

    if data_raw:
        response = _session.post(url_raw, headers=headers, files=dict(csv=data_raw))
        ok = response.status_code < 400
        if not ok:
            raise Exception(json.dumps(response.json()))
    time.sleep(65)

# import ipdb; ipdb.set_trace(context=30)
polarity_url = f'https://api.tinybird.co/v0/pipes/alrocar_timeline_moving_average.json?token={read_token}'
response = _session.get(polarity_url)
polarity = float(response.json()['data'][0]['polarity'])

from PIL import Image
import numpy as np

def rgb_to_hsv(rgb):
    # Translated from source of colorsys.rgb_to_hsv
    # r,g,b should be a numpy arrays with values between 0 and 255
    # rgb_to_hsv returns an array of floats between 0.0 and 1.0.
    rgb = rgb.astype('float')
    hsv = np.zeros_like(rgb)
    # in case an RGBA array was passed, just copy the A channel
    hsv[..., 3:] = rgb[..., 3:]
    r, g, b = rgb[..., 0], rgb[..., 1], rgb[..., 2]
    maxc = np.max(rgb[..., :3], axis=-1)
    minc = np.min(rgb[..., :3], axis=-1)
    hsv[..., 2] = maxc
    mask = maxc != minc
    hsv[mask, 1] = (maxc - minc)[mask] / maxc[mask]
    rc = np.zeros_like(r)
    gc = np.zeros_like(g)
    bc = np.zeros_like(b)
    rc[mask] = (maxc - r)[mask] / (maxc - minc)[mask]
    gc[mask] = (maxc - g)[mask] / (maxc - minc)[mask]
    bc[mask] = (maxc - b)[mask] / (maxc - minc)[mask]
    hsv[..., 0] = np.select(
        [r == maxc, g == maxc], [bc - gc, 2.0 + rc - bc], default=4.0 + gc - rc)
    hsv[..., 0] = (hsv[..., 0] / 6.0) % 1.0
    return hsv

def hsv_to_rgb(hsv):
    # Translated from source of colorsys.hsv_to_rgb
    # h,s should be a numpy arrays with values between 0.0 and 1.0
    # v should be a numpy array with values between 0.0 and 255.0
    # hsv_to_rgb returns an array of uints between 0 and 255.
    rgb = np.empty_like(hsv)
    rgb[..., 3:] = hsv[..., 3:]
    h, s, v = hsv[..., 0], hsv[..., 1], hsv[..., 2]
    i = (h * 6.0).astype('uint8')
    f = (h * 6.0) - i
    p = v * (1.0 - s)
    q = v * (1.0 - s * f)
    t = v * (1.0 - s * (1.0 - f))
    i = i % 6
    conditions = [s == 0.0, i == 1, i == 2, i == 3, i == 4, i == 5]
    rgb[..., 0] = np.select(conditions, [v, q, p, p, t, v], default=v)
    rgb[..., 1] = np.select(conditions, [v, v, v, q, p, p], default=t)
    rgb[..., 2] = np.select(conditions, [v, p, t, v, v, q], default=p)
    return rgb.astype('uint8')


def shift_hue(arr,hout):
    hsv=rgb_to_hsv(arr)
    hsv[...,0]=hout
    rgb=hsv_to_rgb(hsv)
    return rgb

img = Image.open('avatar.png').convert('RGBA')
cc = Image.open('cc.png').convert('RGBA')
arr = np.array(img)

day_of_year = datetime.now().timetuple().tm_yday

# for i in range(-100, 100, 10):
    # hue = (180-i)
hue = (polarity + 100) * 1.8/720

new_img = Image.fromarray(shift_hue(arr,hue), 'RGBA')
avatar = f'_avatar{str(hue)}.png'
# new_img.paste(cc, (10, 10))
new_img.save(avatar)

csv_chunk = StringIO()
writer = csv.writer(csv_chunk, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

writer.writerow(["date", "polarity", "hue"])
writer.writerow([str(datetime.now()), polarity, hue])
data = csv_chunk.getvalue()
headers = {
    'Authorization': f'Bearer {token}',
    'X-TB-Client': 'alrocar-tweets-0.1',
}

if data:   
    # updating the profile picture 
    api.update_profile_image(avatar)
    datasource = f'{targettwitterprofile}_polarity_log'
    url = f'https://api.tinybird.co/v0/datasources?mode={mode}&name={datasource}'
    response = _session.post(url, headers=headers, files=dict(csv=data))
    ok = response.status_code < 400
    if not ok:
        raise Exception(json.dumps(response.json()))
