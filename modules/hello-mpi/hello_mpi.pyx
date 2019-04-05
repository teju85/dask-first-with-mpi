# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

import dask.distributed
from libcpp.string cimport string
import re


# Current set of assumptions:
#  1. single node
#  2. ports starting from 9000 to 9000+nWorkers is not already taken
#  3. only once dask -> mpi hand-off per dask session is supported
#  4. no elasticity


cdef extern from "hello_mpi_c.h" namespace "HelloMPI":
    void mpi_run(int workerId, int nWorkers, int basePort)


# Port where to perform MPI_Open_port
BasePort = 9000


def run(nWorkers):
    w = dask.distributed.get_worker()
    print("Hello World! from ip=%s worker=%s/%d" %(w.address, w.name, nWorkers))
    workerId = int(w.name)
    mpi_run(workerId, nWorkers, BasePort)
    print("Worker=%s finished" % w.name)
