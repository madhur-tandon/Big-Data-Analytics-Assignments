from bolts import AMSBolt
from spouts import TweetSpout
from streamparse import Grouping, Topology

class SurpriseNumberTwitter(Topology):
    tweet_spout = TweetSpout.spec()
    count_bolt = AMSBolt.spec(inputs={tweet_spout: Grouping.fields("tweet")}, par=1)
