import multiprocessing as mp
import sys
import traceback

class Stage:
    def __init__(self, name, size):
        self.name = name

        if size <= 0:
            raise 'Size needs to be strictly positive'
        self.queue = mp.Queue(size)
        self.oqueue = None

    def start__(self):
        sys.stdout.write('Starting stage "{}"...\n'.format(self.name))
        self.thread = mp.Process(
                target=Stage.process__,
                args=(self, self.name, self.queue, self.oqueue))
        self.thread.start()

    def stop__(self):
        sys.stdout.write('Stopping stage "{}"...\n'.format(self.name))
        self.thread.join()

    def start(self):
        return True

    def stop(self):
        return True

    def link(self, otherStage):
        self.oqueue = otherStage.queue

    def send(self, packet):
        if self.oqueue is None:
            return
        self.oqueue.put(packet)

    def process__(self, name, iqueue, oqueue):
        self.name = name
        self.queue = iqueue
        self.oqueue = oqueue

        # Initialise stage
        if not self.start():
            sys.stderr.write('Failed to start "{}" stage\n'.format(self.name))
            return

        try:
            stop = False
            while not stop:
                # Read packet from queue
                packet = self.queue.get()
                if packet is None:
                    stop = True
                    self.send(None)
                    continue

                # Pass packet to main function
                self.process(packet)
        except:
            sys.stderr.write('Exception in "{}" stage\n'.format(self.name))
            sys.stderr.write(traceback.format_exc())

        # Teardown stage
        if not self.stop():
            sys.stderr.write('Failed to stop "{}" stage\n'.format(self.name))

    def process(self, packet):
        pass


class MultiStage(Stage):
    def __init__(self, name, size, threadcount):
        super().__init__(name, size)

        self.iqueues = []
        self.oqueues = []
        self.threadcount = threadcount

    def start__(self):
        sys.stdout.write('Starting stage "{}"...\n'.format(self.name))

        # Start N worker threads
        self.threads = []
        for i in range(self.threadcount):
            self.iqueues.append(mp.Queue(self.queue.qsize()))
            self.oqueues.append(mp.Queue(self.queue.qsize()))
            self.threads.append(
                    mp.Process(
                        target=Stage.process__,
                        args=(self, '{} - {}'.format(self.name, i), self.iqueues[i], self.oqueues[i])))
            self.threads[i].start()

        # Start dispatcher and reducer thread
        self.dispatcher = mp.Process(
                target=MultiStage.dispatch__,
                args=(self, '{} - Dispatcher'.format(self.name), self.queue, self.iqueues))
        self.reducer = mp.Process(
                target=MultiStage.reduce__,
                args=(self, '{} - Reducer'.format(self.name), self.oqueues, self.oqueue))
        self.dispatcher.start()
        self.reducer.start()

    def stop__(self):
        sys.stdout.write('Stopping stage "{}"...\n'.format(self.name))

        for i in range(self.threadcount):
            self.threads[i].join()

        self.dispatcher.join()
        self.reducer.join()

    def dispatch__(self, name, iqueue, oqueues):
        self.threadcount = len(oqueues)
        self.name = name
        self.queue = iqueue
        self.oqueues = oqueues

        try:
            counter = 0
            stop = False
            while not stop:
                # Read packet from queue
                packet = self.queue.get()
                if packet is None:
                    stop = True
                    for i in range(self.threadcount):
                        self.oqueues[i].put(None)
                    continue

                # Round robin packet dispatch
                self.oqueues[counter].put(packet)
                counter = (counter + 1) % self.threadcount
        except:
            sys.stderr.write('Exception in "{}" stage\n'.format(self.name))
            sys.stderr.write(traceback.format_exc())

    def reduce__(self, name, iqueues, oqueue):
        self.threadcount = len(iqueues)
        self.name = name
        self.queues = iqueues
        self.oqueue = oqueue

        try:
            counter = 0
            stop = False
            while not stop:
                # Read packet from current queue
                packet = self.queues[counter].get()
                if packet is None:
                    stop = True
                    if not self.oqueue is None:
                        self.oqueue.put(None)
                    continue

                # Pass packet forward
                self.oqueue.put(packet)
                counter = (counter + 1) % self.threadcount
        except:
            sys.stderr.write('Exception in "{}" stage\n'.format(self.name))
            sys.stderr.write(traceback.format_exc())


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

