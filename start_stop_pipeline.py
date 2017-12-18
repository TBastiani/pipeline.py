from stage import Stage
from pipeline import Pipeline
import time

class DoubleStage(Stage):
    def process(self, value):
        self.send(2 * value)

class ProgressStage(Stage):
    def start(self):
        self.count = 0
        return True

    def process(self, value):
        self.count += 1
        self.send(value)
        if self.count % 1000 == 0:
            print("Processed {} values".format(self.count))

class SumStage(Stage):
    def start(self):
        self.sum = 0
        return True

    def process(self, packet):
        self.sum += packet

    def stop(self):
        print('Sum = {}'.format(self.sum))
        return True

# Setup pipeline
pipe = Pipeline()

double = DoubleStage('double', 10)
pipe.addStage(double)

progress = ProgressStage('progress', 10)
pipe.addStage(progress)
double.link(progress)

sum = SumStage('sum', 10)
pipe.addStage(sum)
progress.link(sum)

# Start pipeline
pipe.start()

# Feed pipeline
for i in range(1000000):
    pipe.put(i)

# Stop pipeline
pipe.stop()
