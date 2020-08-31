import random

class MaxDictReservoirSampling(object):
    def __init__(self, max_size):
        self.max_size = max_size
        self.stream_counter = 0
        self.counter = 0
        self.dict = {}
        self.n = 0

    def __getitem__(self, key):
        try:
            return self.dict[key]
        except Exception as e:
            pass
    
    def __setitem__(self, key, value):
        self.stream_counter += 1
        if key in self.dict:
            self.n += ((2*value) - 1) - ((2*self.dict[key]) - 1)
            self.dict[key] = value
        else:
            self.counter+=1
            if self.counter <= self.max_size:
                self.n += ((2*value) - 1)
                self.dict[key] = value
            else:
                x = random.uniform(0, 1)
                if x <= self.max_size / self.counter:
                    random_key = random.choice(list(self.dict))
                    self.n += ((2*value) - 1) - ((2*self.dict[random_key]) - 1)
                    del self.dict[random_key]
                    self.dict[key] = value

    def __delitem__(self, key):
        del self.dict[key]
    
    def __iter__(self):
        return self.dict.__iter__()

    def __len__(self):
        return len(self.dict)
