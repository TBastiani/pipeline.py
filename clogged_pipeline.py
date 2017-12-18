from pipeline import *
import time

class DoubleStage(Stage):
    def process(self, value):
        self.send(2 * value)

class StallStage(Stage):
    def process(self, value):
        time.sleep(0.001)
        self.send(value)

class PrintStage(Stage):
    def process(self, packet):
        print(packet)

# Setup pipeline
pipe = Pipeline()

double = DoubleStage('double', 10)
pipe.addStage(double)

stall = StallStage('stall', 10)
pipe.addStage(stall)
double.link(stall)

printer = PrintStage('printer', 10)
pipe.addStage(printer)
stall.link(printer)

# Start pipeline
pipe.start()

# Feed pipeline
for i in range(1000000):
    pipe.put(i)

# Stop pipeline
pipe.stop()
