from datetime import datetime
from redis import StrictRedis
from streamparse import Bolt
from utils import MaxDictReservoirSampling

class AMSBolt(Bolt):
    outputs = ["surprise_number", "num_unique_users"]

    def initialize(self, conf, ctx):
        self.redis = StrictRedis(host='127.0.0.1', port=6379, db=0)
        self.reservoir = MaxDictReservoirSampling(10000)

    def _increment(self, user_id, inc_by):
        if user_id not in self.reservoir:
            self.reservoir[user_id] = inc_by
        else:
            self.reservoir[user_id] += inc_by

        value = self.reservoir[user_id]
        if value != None:
            return value
        else:
            return 'error'
    
    def ams_algorithm(self):
        all_moments_sum = 0
        for i in self.reservoir:
            each_moment = (self.reservoir.stream_counter)*((2*self.reservoir[i]) - 1)
            all_moments_sum += each_moment
        surprise_number = all_moments_sum / len(self.reservoir)
        return surprise_number

    def process(self, tup):
        user_id = tup.values[0]
        count = self._increment(user_id, 1)
        surprise_number = self.ams_algorithm()
        self.emit([surprise_number, len(self.reservoir)])
        self.redis.publish("SurpriseNumberTopology", str(surprise_number) + "|" + str(len(self.reservoir)))