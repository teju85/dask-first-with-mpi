#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include "hello_mpi_c.h"
#include <string>
#include <stdexcept>

#define THROW(fmt, ...)                                         \
    do {                                                        \
        std::string msg;                                        \
        char errMsg[2048];                                      \
        sprintf(errMsg, "Exception occured! file=%s line=%d: ", \
                __FILE__, __LINE__);                            \
        msg += errMsg;                                          \
        sprintf(errMsg, fmt, ##__VA_ARGS__);                    \
        msg += errMsg;                                          \
        throw std::runtime_error(msg);                          \
    } while(0)

#define ASSERT(check, fmt, ...)                  \
    do {                                         \
        if(!(check))  THROW(fmt, ##__VA_ARGS__); \
    } while(0)

#define COMM_CHECK(call)                                        \
  do {                                                          \
    auto status = call;                                         \
    ASSERT(status == 0, "FAIL: call='%s'!", #call);             \
  } while (0)


namespace HelloMPI {

class MpiWorldBuilder {
public:
    /**
     * @brief ctor
     * @param wid worker Id as received from dask world. It thus expects that
     * the dask-workers have been created with some way of identifying their IDs!
     * @param num number of workers
     * @param base base port from where to start MPI_Open_port
     * @param host current host name where to start MPI_Open_port
     */
    MpiWorldBuilder(int wid, int num, int base, const std::string& host="localhost"):
        workerId(wid), nWorkers(num), basePort(base), hostName(host), portName() {
        openPort();
        getPort();
        connectToClients();
        connectToServer();
    }

    /** dtor */
    ~MpiWorldBuilder() {
        if(workerId == 0) {
            printf("worker=%d: server closing port\n", workerId);
            remove("server.port");
            COMM_CHECK(MPI_Close_port(portName.c_str()));
        }
    }

private:
    /** current dask worker ID received from python world */
    int workerId;
    /** number of workers */
    int nWorkers;
    /** base port */
    int basePort;
    /** current host name on which to perform MPI_Open_port */
    std::string hostName;
    /** port name returned by MPI_Open_port */
    std::string portName;
    /** comm handle for all the connected processes so far */
    MPI_Comm intracomm;
    /** comm handle for the current newly connected MPI_Comm */
    MPI_Comm intercomm;

    void openPort() {
        if(workerId != 0) return;
        char _portName[MPI_MAX_PORT_NAME];
        ///@todo: clean up this text-file based communication hack. It is dirty!
        COMM_CHECK(MPI_Open_port(MPI_INFO_NULL, _portName));
        portName = _portName;
        printf("worker=%d port opened on %s\n", workerId, portName.c_str());
        FILE *fp = fopen("server.port", "w");
        fprintf(fp, "%s", portName.c_str());
        fclose(fp);
    }

    void getPort() {
        if(workerId == 0) return;
        char _portName[MPI_MAX_PORT_NAME];
        ///@todo: clean up this text-file based communication hack. It is dirty!
        FILE* fp;
        while((fp = fopen("server.port", "r")) == NULL)
            sleep(1);
        fscanf(fp, "%s", _portName);
        fclose(fp);
        portName = _portName;
        printf("worker=%d server port obtained to be %s\n", workerId,
               portName.c_str());
    }

    void connectToClients() {
        if(workerId != 0) return;
        for(int i=1;i<nWorkers;++i) {
            COMM_CHECK(MPI_Comm_accept(portName.c_str(), MPI_INFO_NULL, 0,
                                       i == 1? MPI_COMM_WORLD : intracomm,
                                       &intercomm));
            printf("worker=%d: server: accepted connection from client=%d\n",
                   workerId, i);
            COMM_CHECK(MPI_Intercomm_merge(intercomm, 0, &intracomm));
            int rank, nranks;
            MPI_Comm_rank(intracomm, &rank);
            MPI_Comm_size(intracomm, &nranks);
            printf("worker=%d: server: after merging from client=%d rank=%d/%d\n",
                   workerId, i, rank, nranks);
        }
    }

    void connectToServer() {
        if(workerId == 0) return;
        COMM_CHECK(MPI_Comm_connect(portName.c_str(), MPI_INFO_NULL, 0,
                                    MPI_COMM_WORLD, &intercomm));
        printf("worker=%d: client: connected to server\n", workerId);
        COMM_CHECK(MPI_Intercomm_merge(intercomm, 1, &intracomm));
        int rank, nranks;
        MPI_Comm_rank(intracomm, &rank);
        MPI_Comm_size(intracomm, &nranks);
        printf("worker=%d: client: after merging with server rank=%d/%d\n",
               workerId, rank, nranks);
    }
};


void mpi_run(int workerId, int nWorkers, int basePort) {
    int rank, nranks;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);
    printf("Hello from rank=%d/%d worker=%d/%d\n", rank, nranks, workerId, nWorkers);
    {
        MpiWorldBuilder builder(workerId, nWorkers, basePort);
        sleep(5);
    }
    printf("Bye from rank=%d/%d worker=%d\n", rank, nranks, workerId);
    MPI_Finalize();
}

} // end namespace HelloMPI
