from pipeline import *
import time
import numpy as np

class TimingStage(Stage):
    def start(self):
        self.counter = 0
        self.target = 2000
        return True

    def process(self, packet):
        if self.counter == 0:
            self.start = time.time()
        if self.counter == self.target:
            self.end = time.time()
            self.send(None)
            return

        self.send(packet)
        self.counter += 1

    def stop(self):
        print(self.end - self.start)
        return True

class PassStage(Stage):
    def process(self, packet):
        self.send(packet)

pipe = Pipeline()

ping = TimingStage('ping', 1)
pipe.addStage(ping)
pong = PassStage('pong', 1)
pipe.addStage(pong)

ping.link(pong)
pong.link(ping)

pipe.put(np.zeros((2000, 2000), dtype=np.uint8))

pipe.start()
#pipe.stop()
