from pipeline import *

class PowerStage(Stage):
    def process(self, value):
        self.send(value * value)

class PrintStage(Stage):
    def process(self, packet):
        print(packet)

# Setup pipeline
pipe = Pipeline()

pow = PowerStage('pow', 10)
pipe.addStage(pow)

printer = PrintStage('printer', 10)
pipe.addStage(printer)
pow.link(printer)

# Start pipeline
pipe.start()

# Feed pipeline
for i in range(1000):
    pipe.put(i)

# Stop pipeline
pipe.stop()
