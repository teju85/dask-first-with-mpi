#include <mpi.h>
#include <stdio.h>
#include <unistd.h>

extern "C" void mpi_run();

void mpi_run() {
    int rank, nranks;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);
    printf("Hello from rank=%d/%d\n", rank, nranks);
    sleep(5);
    printf("Bye from rank=%d\n", rank);
    MPI_Finalize();
}
