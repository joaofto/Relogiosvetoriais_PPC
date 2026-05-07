#include <stdio.h>
#include <mpi.h>

typedef struct Clock {
    int p[3];
} Clock;


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


void InternalEvent(Clock *clock, const char *label) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    clock->p[pid]++;
    PrintClock(label, clock);
}


void SendEvent(int dest, Clock *clock, const char *label, int tag) {
    MPI_Send(clock->p, 3, MPI_INT, dest, tag, MPI_COMM_WORLD);
    PrintClock(label, clock);
}


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

    InternalEvent(&clock, "a");                 

    SendEvent(1, &clock, "b", TAG_B);           

    ReceiveEvent(1, &clock, "c", TAG_H);        

    InternalEvent(&clock, "d");                

    SendEvent(2, &clock, "d->m", TAG_DM);       

    ReceiveEvent(2, &clock, "e", TAG_LE);       

    InternalEvent(&clock, "f");                 

    SendEvent(1, &clock, "f->j", TAG_FJ);       

    InternalEvent(&clock, "g");                 
}

void process1() {
    Clock clock = {{0,0,0}};

    SendEvent(0, &clock, "h", TAG_H);           

    ReceiveEvent(0, &clock, "i", TAG_B);        

    ReceiveEvent(0, &clock, "j", TAG_FJ);       
}

void process2() {
    Clock clock = {{0,0,0}};

    InternalEvent(&clock, "k");                 

    InternalEvent(&clock, "l");                 

    ReceiveEvent(0, &clock, "m", TAG_DM);      

    SendEvent(0, &clock, "l->e", TAG_LE);      
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
