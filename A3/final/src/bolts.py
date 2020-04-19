from datetime import datetime
from redis import StrictRedis
from streamparse import Bolt
from utils import MaxDictReservoirSampling

DATE_FMT = "%Y-%m-%d %H:%M:%S"

# class RedisWordCountBolt(Bolt):
#     outputs = ["word", "count"]

#     def initialize(self, conf, ctx):
#         self.redis = StrictRedis()
#         self.counter = {}
#         self.total = 0

#     def _increment(self, word, inc_by):
#         if word not in self.counter:
#             self.counter[word] = inc_by
#         else:
#             self.counter[word] += inc_by
#         self.total += inc_by

#     def process(self, tup):
#         datetime_now = datetime.now().strftime(DATE_FMT)
#         word = tup.values[0]
#         count = self._increment(word, 10 if word == "dog" else 1)
#         if self.total % 1000 == 0:
#             self.logger.info("counted %i words", self.total)
#         self.redis.publish("WordCountTopology", word + "|" + str(self.counter[word]))

class RedisUserTweetCountBolt(Bolt):
    outputs = ["user_id", "count"]

    def initialize(self, conf, ctx):
        self.redis = StrictRedis()
        self.reservoir = MaxDictReservoirSampling(10000)
        self.total = 0

    def _increment(self, user_id, inc_by):
        if user_id not in self.reservoir:
            self.reservoir[user_id] = inc_by
        else:
            self.reservoir[user_id] += inc_by
        self.total += inc_by
        try:
            return self.reservoir[user_id]
        except:
            return 0

    def process(self, tup):
        datetime_now = datetime.now().strftime(DATE_FMT)
        user_id = tup.values[0]
        count = self._increment(user_id, 1)
        if self.total % 1000 == 0:
            self.logger.info("counted %i words", self.total)
        self.redis.publish("WordCountTopology", user_id + "|" + str(count))