# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

import dask.distributed
import re
import os


# Current set of assumptions:
#  1. single node
#  2. ports starting from 9000 to 9000+nWorkers is not already taken
#  3. only once dask -> mpi hand-off per dask session is supported
#  4. no elasticity
#  5. OpenMPI (and for GPU apps, CUDA-aware ompi)


cdef extern from "hello_mpi_c.h" namespace "HelloMPI":
    void mpi_run(int workerId, int nWorkers)


def run(nWorkers, ompiServerUri):
    if ompiServerUri is None:
        raise Exception("ompiServerUri is mandatory!")
    os.environ["OMPI_MCA_pmix_server_uri"] = ompiServerUri
    w = dask.distributed.get_worker()
    print("Hello World! from ip=%s worker=%s/%d uri=%s" % \
          (w.address, w.name, nWorkers, ompiServerUri))
    workerId = int(w.name)
    mpi_run(workerId, nWorkers)
    print("Worker=%s finished" % w.name)
