import tweepy
import os
import re
from textblob import TextBlob
from textblob.exceptions import TranslatorError
import time
from threading import Timer
import requests
import csv
import json

from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from email.utils import parsedate_to_datetime


TWITTER_HANDLE = 'alrocar'

CONSUMER_KEY = os.environ['CONSUMER_KEY']
CONSUMER_SECRET = os.environ['CONSUMER_SECRET']
ACCESS_TOKEN = os.environ['ACCESS_TOKEN']
ACCESS_TOKEN_SECRET = os.environ['ACCESS_TOKEN_SECRET']
TB_TOKEN = os.environ['TB_TOKEN']
READ_TOKEN = os.environ['READ_TOKEN']

TB_API_URL = 'https://api.tinybird.co/v0'
datasource = 'tweets'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, timeout=300)


def get_requests_session():
    retry = Retry(total=5, backoff_factor=10)
    adapter = HTTPAdapter(max_retries=retry)
    _session = requests.Session()
    _session.mount('http://', adapter)
    _session.mount('https://', adapter)
    return _session


def parse_tweets(tweets):
    result = []
    for tweet in tweets:
        tweetid = tweet.id
        tweetdate = str(tweet.created_at)
        tweettext = tweet.text
        tt = " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", tweettext).split())
        result.append([tweetid, tweetdate, tt])
    return result


def enrich_polarity(tweet):
    try:
        analysis = TextBlob(tweet)
        # language = analysis.detect_language()
        # if language != 'en':
        #     analysis = analysis.translate(to='en')
        return round(analysis.sentiment.polarity, 4)
    except Exception as e:
        print(e)
        raise e


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


class TinybirdApiSink():
    def __init__(self, token, datasource, endpoint=TB_API_URL):
        super().__init__()
        self.endpoint = endpoint
        self.token = token
        self.datasource = datasource
        self.url = f'{self.endpoint}/datasources?mode=append&name={self.datasource}'
        retry = Retry(total=5, backoff_factor=0.2)
        adapter = HTTPAdapter(max_retries=retry)
        self._session = requests.Session()
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)
        self.reset()
        self.wait = False

    def reset(self):
        self.csv_chunk = StringIO()
        self.writer = csv.writer(self.csv_chunk, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

    def append(self, value):
        try:
            self.writer.writerow(value)
        except Exception as e:
            print(e)

    def tell(self):
        return self.csv_chunk.tell()

    def flush(self):
        self.wait = True
        data = self.csv_chunk.getvalue()
        self.reset()
        headers = {
            'Authorization': f'Bearer {self.token}',
            # 'X-TB-Client': f'{__version__.__name__}-{__version__.__version__}',
        }

        ok = False
        try:
            response = self._session.post(self.url, headers=headers, files=dict(csv=data))
            print('flush response')
            print(response)
            ok = response.status_code < 400
            self.wait = False
            return ok
        except Exception as e:
            self.wait = False
            print(e)
            return ok


class MyStreamListener(tweepy.StreamListener):

    def __init__(self, name, api, search_term, max_wait_seconds=300,
                 max_wait_records=10000,
                 max_wait_bytes=1024*1024*1):
        self.name = name
        self.records = 0
        self.api = api
        self.search_term = search_term
        self.sink = TinybirdApiSink(TB_TOKEN, datasource)
        self.max_wait_seconds = max_wait_seconds
        self.max_wait_records = max_wait_records
        self.max_wait_bytes = max_wait_bytes
        self.records = 0
        self.timer = None
        self.timer_start = None
        self.tr_timer = None
        self.tr_timer_start = None

    def append(self, record):
        if self.records % 100 == 0:
            print('append')
        self.sink.append(record)
        self.records += 1
        if self.records < self.max_wait_records and self.sink.tell() < self.max_wait_bytes:
            if not self.timer:
                self.timer_start = time.monotonic()
                self.timer = Timer(self.max_wait_seconds, self.flush)
                self.timer.name = f"f{self.name}_timer"
                self.timer.start()
        else:
            self.flush()

    def flush(self):
        print('flush')
        if self.timer:
            self.timer.cancel()
            self.timer = None
            self.timer_start = None
        if not self.records:
            return
        self.sink.flush()
        self.records = 0

    def on_data(self, raw_data):
        while self.sink.wait:
            print('wait flush')
            time.sleep(1)
        super().on_data(raw_data)
        tweet = json.loads(raw_data)
        if 'created_at' not in tweet or 'id' not in tweet or 'text' not in tweet:
            return
        date = str(tweet['created_at'])
        tweet = [tweet['id'], parsedate_to_datetime(date).strftime("%Y-%m-%d %H:%M:%S"), " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", tweet['text']).split()), self.search_term]
        try:
            polarity = enrich_polarity(tweet[2])
        except Exception:
            polarity = 0

        self.append(tweet + [polarity])


def connect():
    try:
        myStreamListener = MyStreamListener('tweets', api, 'covid')
        myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

        myStream.filter(track=['covid'])
    except Exception as e:
        print(e)


while True:
    print('connect')
    connect()
