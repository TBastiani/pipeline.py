class Pipeline:
    def __init__(self):
        self.stages = []
        self.first = None

    def addStage(self, stage):
        self.stages.append(stage)
        if self.first is None:
            self.first = stage

    def start(self):
        for stage in self.stages:
            stage.start__()

    def stop(self):
        self.first.queue.put(None)
        for stage in self.stages:
            stage.stop__()

    def put(self, packet):
        self.first.queue.put(packet)
