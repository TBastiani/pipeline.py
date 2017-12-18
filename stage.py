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
