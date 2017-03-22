
import random


class ReservoirSample(object):

    def __init__(self, size=30):
        self.count = 0
        self.size = size
        self.samples = []

    def update(self, data):
        if self.count < self.size:
            self.samples.append(data)
        else:
            chance = random.randint(0, self.count)
            if chance < self.size:
                self.samples[chance] = data
        self.count += 1


def coin_tosses():
    for coin in xrange(90000):
        yield random.choice('TH')


if __name__ == '__main__':

    s = ReservoirSample(size=10)

    for toss in coin_tosses():
        s.update(toss)

    print len(s.samples)
    print s.samples

    results = {'tails': 0,
               'heads': 0}

    for datum in s.samples:
        if datum == 'T':
            results['tails'] += 1
        else:
            results['heads'] += 1

    print results
