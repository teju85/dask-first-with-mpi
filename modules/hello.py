import dask.distributed
import time


def run(nWorkers):
    w = dask.distributed.get_worker()
    print("Hello World! from ip=%s worker=%s/%d" %(w.address, w.name, nWorkers))
    time.sleep(2)
    print("Worker=%s finished" % w.name)
