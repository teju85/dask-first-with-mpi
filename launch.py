import subprocess as subp
import time
import argparse
from dask.distributed import Client
import os


class DaskEnv:
    """Wrapper RAII class to launch dask scheduler and workers. Currently,
    this has been designed to only run on a single node."""

    SleepTime = 2

    def __init__(self, args):
        print("Launching dask scheduler...")
        self.sched = self.__runInBackground(["dask-scheduler",
                                             "--port", "%d" % args.schedPort,
                                             "--bokeh",
                                             "--bokeh-port",
                                             "%d" % args.bokehPort])
        time.sleep(DaskEnv.SleepTime)
        print("Launching dask workers...")
        self.workers = []
        ipAddr = "%s:%d" % (args.schedIp, args.schedPort)
        for i in range(args.nWorkers):
            proc = self.__runInBackground(["dask-worker",
                                           ipAddr,
                                           "--memory-limit=auto",
                                           "--nprocs=1",
                                           "--nthreads=1",
                                           "--bokeh-port",
                                           "%d" % args.bokehPort,
                                           "--name", "%d" % i])
            self.workers.append(proc)
        if args.mpi:
            print("Launching ompi-server...")
            self.mpiServer = self.__runInBackground(["ompi-server",
                                                     "--no-daemonize",
                                                     "-r", "ompi.server.uri"])
            time.sleep(DaskEnv.SleepTime)
            with open("ompi.server.uri", "r") as fp:
                self.mpiServerUri = fp.read().rstrip()
            print("ompi-server URI = %s" % self.mpiServerUri)
        else:
            self.mpiServer = None
            self.mpiServerUri = None
        time.sleep(DaskEnv.SleepTime)

    def __del__(self):
        # kill the ompi-server, if launched
        if self.mpiServer is not None:
            print("Cleaning up the ompi-server...")
            time.sleep(DaskEnv.SleepTime)
            self.mpiServer.kill()
            os.remove("ompi.server.uri")
        # delete workers first, followed by scheduler!
        print("Cleaning up workers and scheduler...")
        time.sleep(DaskEnv.SleepTime)
        for proc in self.workers:
            proc.kill()
        time.sleep(DaskEnv.SleepTime)
        self.sched.kill()

    def __runInBackground(self, cmd):
        cmdStr = "exec " + " ".join(cmd)
        print("CMD: " + cmdStr)
        proc = subp.Popen(cmdStr, shell=True)
        return proc


def main():
    parser = argparse.ArgumentParser(description="Dask launcher script")
    parser.add_argument("-bokehPort", default=8888, type=int,
                        help="Port for bokeh server")
    parser.add_argument("-nWorkers", default=8, type=int,
                        help="Number of dask workers to launch")
    parser.add_argument("-schedIp", default="localhost", type=str,
                        help="Scheduler IP address needed for dask-worker's")
    parser.add_argument("-schedPort", default=8787, type=int,
                        help="Port for the dask-scheduler")
    parser.add_argument("-module", default=None, type=str,
                        help="Name of the dask-aware module to be executed")
    parser.add_argument("-mpi", default=False, action='store_true',
                        help="Whether this is an MPI run or not")
    args = parser.parse_args()
    if not args.module:
        raise Exception("'-module' is mandatory!")
    de = DaskEnv(args)
    module = __import__(args.module)
    ipAddr = "%s:%d" % (args.schedIp, args.schedPort)
    client = Client(ipAddr)
    client.run(module.run, args.nWorkers, de.mpiServerUri)


if __name__ == "__main__":
    main()
