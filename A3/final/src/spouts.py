import tweepy
import time, random, json
from streamparse import Spout
import queue
import os
current_folder = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_folder, 'api.json')

with open(file_path) as f:
    twitter_api = json.loads(f.read())

consumer_key = twitter_api['consumer_key']
consumer_secret = twitter_api['consumer_secret_key']
access_token = twitter_api['access_token']
access_token_secret = twitter_api['access_token_secret']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

class TweetListener(tweepy.StreamListener):

    def __init__(self, queue):
        super(TweetListener, self).__init__(api)
        self.queue = queue

    def on_status(self, status):

        if ('RT @' not in status.text):
            user_id = status.user.id_str
            try:
                self.queue.put(str(user_id), timeout = 0.01)
            except:
                pass

    def on_error(self, status_code):
        if status_code == 420:
            return False

class TweetSpout(Spout):
    outputs = ["tweet"]

    def initialize(self, stormconf, context):
        self.queue = queue.Queue(maxsize = 1000)
        self.stream_listener = TweetListener(self.queue)
        self.stream = tweepy.Stream(auth=api.auth, listener=self.stream_listener)
        self.stream.filter(languages=["en"], track=["#corona", "#coronavirus", "#covid-19", "#lockdown"], is_async=True)

    def next_tuple(self):
        try:
            tweet = self.queue.get(timeout = 0.1)
            if tweet:
                self.queue.task_done()
                self.emit([tweet])
        except queue.Empty:
            time.sleep(0.1) 

# class WordSpout(Spout):
#     outputs = ["word"]

#     def initialize(self, stormconf, context):
#         self.words = ["dog", "cat", "zebra", "elephant"]

#     def next_tuple(self):
#         word = random.choice(self.words)
#         self.emit([word])
#         time.sleep(1)

