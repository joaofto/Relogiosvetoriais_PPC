/**
 * Template base para implementação de Relógios Vetoriais (MPI)
 * -------------------------------------------------------------
 * Implementar, tomando como base a sequência de operações da figura
 * do link: https://drive.google.com/file/d/1IOAJPpJWUoRC0kygZKr6ERe0hpIICRTP/view?usp=sharing
 *
 * Compilação: mpicc -o rvet rvet.c
 * Execução:   mpiexec -n 3 ./rvet
 */

#include <stdio.h>
#include <string.h>
#include <mpi.h>

/* Estrutura do relógio vetorial (3 processos). */
typedef struct Clock {
    int p[3];
} Clock;

/*
 * Imprime o estado atual do relógio vetorial.
 * Mostra: qual processo realizou a atualização e o vetor resultante.
 */
void PrintClock(const char *acao, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    printf("[Atualizado por P%d] %s -> Clock = (%d,%d,%d)\n",
           pid, acao, clock->p[0], clock->p[1], clock->p[2]);
}

/*
 * Evento interno (sem comunicação).
 * Incrementa o contador local do processo.
 */
void Event(Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    clock->p[pid]++;
    
    // No final, imprime relógio atualizado
    PrintClock("Evento interno", clock);
}

/*
 * Envio de mensagem.
 * Deve:
 *   1. Obter o rank.
 *   2. Incrementar clock local (evento de envio).
 *   3. Enviar clock->p via MPI_Send.
 */
void Send(int dest, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    // TODO: incrementar clock local e enviar clock->p via MPI_Send
    
    // No final, imprime relógio atualizado:
    PrintClock("Envio de mensagem", clock);
}

/*
 * Recebimento de mensagem.
 * Deve:
 *   1. Obter o rank.
 *   2. Receber vetor remoto via MPI_Recv.
 *   3. Incrementar clock local (evento de recebimento).
 *   4. Fazer fusão (max elemento a elemento) com vetor recebido.
 */
void Receive(int src, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    // TODO: receber vetor e atualizar clock local
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
    // No final, imprime relógio atualizado
    PrintClock(label, clock);
}

/*
 * Cada processo define sua sequência de eventos e comunicações
 * conforme o diagrama da figura de referência.
 */

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
    // TODO: Send/Receive conforme diagrama
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

