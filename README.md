# Pipeline.py

## What's this

This is a minimal Python module to help create multithreaded pipelines. Because 
the way Python works this is based on [multiprocessing](https://docs.python.org/3/library/multiprocessing.html).

It's really quite similar to [mpipe](http://vmlaker.github.io/mpipe/) but where 
`mpipe` has a mechanism to [skip work if a stage can't keep up](http://vmlaker.github.io/mpipe/examples3.html)
 `Pipeline.py` blocks until the worker can catch up and doesn't tend to blow up 
if one doesn't use filters...

## What it's useful for

I've successfully used this to speed up a (GPU-backed but) heavily CPU-bound 
[Tensorflow](https://www.tensorflow.org/) training process by a factor of 7+ on 
an [i7-5960X](https://ark.intel.com/products/82930/Intel-Core-i7-5960X-Processor-Extreme-Edition-20M-Cache-up-to-3_50-GHz)

This is somewhat similar to what is described [here](https://hanxiao.github.io/2017/07/07/Get-10x-Speedup-in-Tensorflow-Multi-Task-Learning-using-Python-Multiprocessing/)

## Example

```Python
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
```
