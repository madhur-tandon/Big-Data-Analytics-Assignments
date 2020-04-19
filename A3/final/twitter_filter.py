import tweepy
import datetime
import json

file_path = './src/api.json'

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

    def on_status(self, status):

        if ('RT @' not in status.text):
            tweet_item = {
                'text': status.text,
                'user_id': status.user.id_str,
                'received_at': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            print(tweet_item)

    def on_error(self, status_code):
        if status_code == 420:
            return False

stream_listener = TweetListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=["#corona", "#coronavirus", "#Covid-19", "#lockdown"], languages=["en"], is_async=True)
