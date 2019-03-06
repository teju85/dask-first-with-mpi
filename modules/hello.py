import dask.distributed
import time


def run():
    w = dask.distributed.get_worker()
    print("Hello World! from ip=%s worker=%s" %(w.address, w.name))
    time.sleep(2)
    print("Worker=%s finished" % w.name)
