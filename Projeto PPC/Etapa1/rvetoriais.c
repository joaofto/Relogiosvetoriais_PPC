#include <stdio.h>
#include <mpi.h>

typedef struct Clock {
    int p[3];
} Clock;

/* TAGS */
#define TAG_B   1
#define TAG_H   2
#define TAG_DM  3
#define TAG_LE  4
#define TAG_FJ  5

void PrintClock(const char *evento, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    printf("P%d - %s -> (%d,%d,%d)\n",
           pid, evento, clock->p[0], clock->p[1], clock->p[2]);
    fflush(stdout);
}

/* Evento interno */
void InternalEvent(Clock *clock, const char *label) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    clock->p[pid]++;
    PrintClock(label, clock);
}

/* ENVIO SEM INCREMENTO (REGRA DO PROFESSOR) */
void SendEvent(int dest, Clock *clock, const char *label, int tag) {
    MPI_Send(clock->p, 3, MPI_INT, dest, tag, MPI_COMM_WORLD);
    PrintClock(label, clock);
}

/* Recebimento com atualização vetorial */
void ReceiveEvent(int src, Clock *clock, const char *label, int tag) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    int received[3];

    MPI_Recv(received, 3, MPI_INT, src, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    printf("P%d - Recebendo de P%d: (%d,%d,%d) - antes: (%d,%d,%d)\n", 
           pid, src, received[0], received[1], received[2], 
           clock->p[0], clock->p[1], clock->p[2]);
    fflush(stdout);

    for (int i = 0; i < 3; i++) {
        if (received[i] > clock->p[i]) {
            clock->p[i] = received[i];
        }
    }

    clock->p[pid]++;
    PrintClock(label, clock);
}
