import tweepy
import os
import re
from textblob import TextBlob
import time
from datetime import datetime
import requests
import csv
import json
from PIL import Image
import numpy as np

from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


TWITTER_HANDLE = 'alrocar'

CONSUMER_KEY = os.environ['CONSUMER_KEY']
CONSUMER_SECRET = os.environ['CONSUMER_SECRET']
ACCESS_TOKEN = os.environ['ACCESS_TOKEN']
ACCESS_TOKEN_SECRET = os.environ['ACCESS_TOKEN_SECRET']
TB_TOKEN = os.environ['TB_TOKEN']
READ_TOKEN = os.environ['READ_TOKEN']

TB_API_URL = 'https://api.tinybird.co/v0'

datasource = f'{TWITTER_HANDLE}_tweets'
datasource_raw = f'{TWITTER_HANDLE}_tweets_raw'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)


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


def shift_hue(arr, hout):
    hsv = rgb_to_hsv(arr)
    hsv[..., 0] = hout
    rgb = hsv_to_rgb(hsv)
    return rgb


def get_requests_session():
    retry = Retry(total=5, backoff_factor=10)
    adapter = HTTPAdapter(max_retries=retry)
    _session = requests.Session()
    _session.mount('http://', adapter)
    _session.mount('https://', adapter)
    return _session


def get_last_tweet_id():
    max_id_url = f'{TB_API_URL}/pipes/alrocar_timeline_max_id.json?token={READ_TOKEN}'
    response = get_requests_session().get(max_id_url)
    data = response.json()['data']
    if len(data) == 0:
        return
    return data[0]['since_id']


def get_polarity_mvng_avg():
    max_id_url = f'{TB_API_URL}/pipes/alrocar_timeline_moving_average.json?token={READ_TOKEN}'
    response = get_requests_session().get(max_id_url)
    data = response.json()['data']
    if len(data) == 0:
        return
    return data


def get_tweets(since_id=None, user=None):
    # for page in tweepy.Cursor(api.user_timeline, id=TWITTER_HANDLE, count=200).pages(20):
    raw = []
    for page in tweepy.Cursor(api.home_timeline, since_id=since_id, count=200).pages(20):
        for tweet in page:
            raw.append(tweet)
        time.sleep(65)
    return raw


def parse_tweets(tweets):
    result = []
    for tweet in tweets:
        tweetid = tweet.id
        tweetdate = str(tweet.created_at)
        tweettext = tweet.text
        tt = " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", tweettext).split())
        result.append([tweetid, tweetdate, tt])
    return result


def enrich_polarity(tweets):
    result = []
    for tweet in tweets:
        tweet.append(round(TextBlob(tweet[2]).sentiment.polarity, 4))
        result.append(tweet)
    return result


def to_tinybird(rows, datasource_name, columns, token=TB_TOKEN, mode='append'):
    url = f'{TB_API_URL}/datasources?mode={mode}&name={datasource_name}'

    csv_chunk = StringIO()
    writer = csv.writer(csv_chunk, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

    writer.writerow(columns)
    for row in rows:
        writer.writerow(row)

    data = csv_chunk.getvalue()
    headers = {
        'Authorization': f'Bearer {token}',
        'X-TB-Client': 'alrocar-tweets-0.1',
    }

    if data:
        response = get_requests_session().post(url, headers=headers, files=dict(csv=data))
        ok = response.status_code < 400
        if not ok:
            raise Exception(json.dumps(response.json()))


def get_polarity():
    polarity_url = f'{TB_API_URL}/pipes/polarity.json?token={READ_TOKEN}'
    response = get_requests_session().get(polarity_url)
    data = response.json()['data']
    if len(data) == 0:
        return
    return float(data[0]['polarity'])


def polarity2hue(polarity):
    return (polarity + 100) #* 1.8/720


def update_avatar(hue, polarity):
    img = Image.open('avatar.png').convert('RGBA')
    arr = np.array(img)
    new_img = Image.fromarray(shift_hue(arr, hue), 'RGBA')
    avatar = f'_avatar{str(hue)}.png'
    new_img.save(avatar)

    api.update_profile_image(avatar)
    to_tinybird([[str(datetime.now()), polarity, hue]], f'{TWITTER_HANDLE}_polarity_log', ["date", "polarity", "hue"])


def update_header():
    pass


def create_stripes(data):
    stripes = Image.new(mode="RGB", size=(1500, 500))
    i = 0
    for p in data:
        img = Image.open('stripe.png').convert('RGBA')
        aa = img.load()

        data = np.array(img)

        # r1, g1, b1 = 0, 0, 0 # Original value
        # import ipdb; ipdb.set_trace(context=30)
        r1 = aa[0, 0][0] # Original value
        g1 = aa[0, 0][1] # Original value
        b1 = aa[0, 0][2] # Original value
        r2, g2, b2 = 255 - p['polarity'] + 100, 0 + p['polarity'] + 100, 0 # Value that we want to replace it with

        red, green, blue = data[:,:,0], data[:,:,1], data[:,:,2]
        mask = (red == r1) & (green == g1) & (blue == b1)
        data[:,:,:3][mask] = [r2, g2, b2]

        new_img = Image.fromarray(data)
        Image.Image.paste(stripes, new_img, (10 * i, 0))
        i += 1
    stripes.save('stripes.png')
    # for i in range(0, 200, 10):
    #     img = Image.open('stripe.png').convert('RGBA')
    #     aa = img.load()

    #     data = np.array(img)

    #     # r1, g1, b1 = 0, 0, 0 # Original value
    #     # import ipdb; ipdb.set_trace(context=30)
    #     r1 = aa[0, 0][0] # Original value
    #     g1 = aa[0, 0][1] # Original value
    #     b1 = aa[0, 0][2] # Original value
    #     r2, g2, b2 = 255 -i, 0 + i, 0 # Value that we want to replace it with

    #     red, green, blue = data[:,:,0], data[:,:,1], data[:,:,2]
    #     mask = (red == r1) & (green == g1) & (blue == b1)
    #     data[:,:,:3][mask] = [r2, g2, b2]

    #     im = Image.fromarray(data)
    #     # arr = np.array(img)
    #     # hue = polarity2hue(i)
    #     # new_img = Image.fromarray(shift_hue(arr, hue), 'RGBA')
    #     im.save(f'avatar___{str(i)}.png')




# import ipdb; ipdb.set_trace(context=30)
# data = get_polarity_mvng_avg()
# data = {}
# create_stripes(data)
import ipdb; ipdb.set_trace(context=30)
since_id = get_last_tweet_id()
tweets_raw = get_tweets(since_id)
tweets = [[tweet.id, str(tweet.created_at), " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", tweet.text).split())] for tweet in tweets_raw]
to_tinybird([[tweet + [round(TextBlob(tweet[2]).sentiment.polarity, 4)]] for tweet in tweets], datasource, ["id", "date", "text", "polarity"])
to_tinybird([json.dumps(tweet._json) for tweet in tweets_raw], datasource_raw, ["tweet"])
polarity = get_polarity()
if polarity:
    hue = polarity2hue(polarity)
    update_avatar(hue, polarity)
    
