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

/* ================= PROCESSOS ================= */

void process0() {
    Clock clock = {{0,0,0}};

    InternalEvent(&clock, "a");                 // (1,0,0)

    SendEvent(1, &clock, "b", TAG_B);           // (1,0,0)

    ReceiveEvent(1, &clock, "c", TAG_H);        // recebe h de P1

    InternalEvent(&clock, "d");                 // d

    SendEvent(2, &clock, "d->m", TAG_DM);       // envia p/ P2

    ReceiveEvent(2, &clock, "e", TAG_LE);       // recebe l->e de P2

    InternalEvent(&clock, "f");                 // f

    SendEvent(1, &clock, "f->j", TAG_FJ);       // envia p/ P1

    InternalEvent(&clock, "g");                 // g
}

void process1() {
    Clock clock = {{0,0,0}};

    SendEvent(0, &clock, "h", TAG_H);           // (0,1,0) after send? NO - send doesn't increment

    ReceiveEvent(0, &clock, "i", TAG_B);        // recebe b de P0

    ReceiveEvent(0, &clock, "j", TAG_FJ);       // recebe f->j de P0
}

void process2() {
    Clock clock = {{0,0,0}};

    InternalEvent(&clock, "k");                 // k - (0,0,1)

    InternalEvent(&clock, "l");                 // l - (0,0,2)

    ReceiveEvent(0, &clock, "m", TAG_DM);       // recebe d->m de P0

    SendEvent(0, &clock, "l->e", TAG_LE);       // envia p/ P0
}

/* ================= MAIN ================= */

int main(void) {
    int my_rank;

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    MPI_Barrier(MPI_COMM_WORLD);

    printf("Processo %d iniciado\n", my_rank);
    fflush(stdout);

    if (my_rank == 0)
        process0();
    else if (my_rank == 1)
        process1();
    else if (my_rank == 2)
        process2();

    MPI_Finalize();
    return 0;
}
