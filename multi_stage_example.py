from pipeline import *
import time

class PassStage(MultiStage):
    def process(self, packet):
        time.sleep(1)
        self.send(packet)

class PrintStage(Stage):
    def process(self, value):
        print(value)

# Setup pipeline
pipe = Pipeline()

multi = PassStage('multi', 10, 4)
pipe.addStage(multi)

printer = PrintStage('printer', 10)
pipe.addStage(printer)
multi.link(printer)

# Start pipeline
pipe.start()

# Feed pipeline
for i in range(100):
    pipe.put(i)

# Stop pipeline
pipe.stop()
