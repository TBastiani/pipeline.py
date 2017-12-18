import multiprocessing as mp
from stage import Stage
import sys
import traceback

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

