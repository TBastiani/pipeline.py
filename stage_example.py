from stage import Stage

class PrintStage(Stage):
    def process(self, packet):
        print(packet)

printer = PrintStage('printer', 10)
printer.start__()

for i in range(1000):
    printer.queue.put(i)

printer.queue.put(None)
printer.stop__()
