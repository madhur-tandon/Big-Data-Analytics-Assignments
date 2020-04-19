"""
Word count topology (in Redis)
"""

# from bolts import RedisWordCountBolt
# from spouts import WordSpout
from bolts import RedisUserTweetCountBolt
from spouts import TweetSpout
from streamparse import Grouping, Topology


class WordCount(Topology):
    # word_spout = WordSpout.spec()
    tweet_spout = TweetSpout.spec()
    # count_bolt = RedisWordCountBolt.spec(inputs={word_spout: Grouping.fields("word")}, par=4)
    count_bolt = RedisUserTweetCountBolt.spec(inputs={tweet_spout: Grouping.fields("tweet")}, par=4)

