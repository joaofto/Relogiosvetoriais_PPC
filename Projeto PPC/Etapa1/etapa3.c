#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <mpi.h>

/* ====================================================================
 * ETAPA 3 – Relógios Vetoriais + Produtor/Consumidor com Threads MPI
 * ==================================================================== */

#define NUM_PROC   3
#define BUF_SIZE   64

typedef struct {
    int p[NUM_PROC];   /* Contadores de cada processo */
    int dest;          /* Destino MPI (usado na fila de envio) */
} Clock;

typedef struct {
    Clock   buf[BUF_SIZE];
    int     count;
    pthread_mutex_t mutex;
    pthread_cond_t  cond_not_full;
    pthread_cond_t  cond_not_empty;
} Queue;

void queue_init(Queue *q) {
    q->count = 0;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond_not_full, NULL);
    pthread_cond_init(&q->cond_not_empty, NULL);
}

void queue_destroy(Queue *q) {
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond_not_full);
    pthread_cond_destroy(&q->cond_not_empty);
}

void queue_push(Queue *q, Clock item) {
    pthread_mutex_lock(&q->mutex);
    while (q->count == BUF_SIZE)
        pthread_cond_wait(&q->cond_not_full, &q->mutex);

    q->buf[q->count++] = item;

    pthread_cond_signal(&q->cond_not_empty);
    pthread_mutex_unlock(&q->mutex);
}

Clock queue_pop(Queue *q) {
    pthread_mutex_lock(&q->mutex);
    while (q->count == 0)
        pthread_cond_wait(&q->cond_not_empty, &q->mutex);

    Clock item = q->buf[0];
    for (int i = 0; i < q->count - 1; i++)
        q->buf[i] = q->buf[i + 1];
    q->count--;

    pthread_cond_signal(&q->cond_not_full);
    pthread_mutex_unlock(&q->mutex);
    return item;
}

typedef struct {
    int    pid;
    Clock  clock;
    pthread_mutex_t clock_mutex;
    Queue  recv_queue;
    Queue  send_queue;
} ProcState;

ProcState state;

void print_clock(const char *acao) {
    printf("[P%d] %s -> Clock = (%d,%d,%d)\n", state.pid, acao,
           state.clock.p[0], state.clock.p[1], state.clock.p[2]);
    fflush(stdout);
}

void Event(void) {
    pthread_mutex_lock(&state.clock_mutex);
    state.clock.p[state.pid]++;
    print_clock("Evento interno");
    pthread_mutex_unlock(&state.clock_mutex);
}

void Send(int dest) {
    pthread_mutex_lock(&state.clock_mutex);
    state.clock.p[state.pid]++;
    Clock to_send = state.clock;
    to_send.dest  = dest;
    print_clock("Envio de mensagem (enfileirado)");
    pthread_mutex_unlock(&state.clock_mutex);

    queue_push(&state.send_queue, to_send);
}

void Receive(void) {
    Clock received = queue_pop(&state.recv_queue);

    pthread_mutex_lock(&state.clock_mutex);
    state.clock.p[state.pid]++;
    for (int i = 0; i < NUM_PROC; i++)
        if (received.p[i] > state.clock.p[i])
            state.clock.p[i] = received.p[i];
    print_clock("Recebimento de mensagem");
    pthread_mutex_unlock(&state.clock_mutex);
}

/* Fluxos de execução dos processos */
void process0_flow(void) {
    Event(); Send(1); Receive(); Send(2); Receive(); Send(1); Event();
}

void process1_flow(void) {
    Send(0); Receive(); Receive();
}

void process2_flow(void) {
    Event(); Send(0); Receive();
}

void *central_thread(void *arg) {
    (void)arg;
    if      (state.pid == 0) process0_flow();
    else if (state.pid == 1) process1_flow();
    else if (state.pid == 2) process2_flow();
    return NULL;
}

int expected_receives[] = {2, 2, 1};

void *receiver_thread(void *arg) {
    (void)arg;
    int n = expected_receives[state.pid];
    for (int i = 0; i < n; i++) {
        int buf[NUM_PROC];
        MPI_Status status;
        MPI_Recv(buf, NUM_PROC, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

        Clock c;
        memcpy(c.p, buf, sizeof(int) * NUM_PROC);
        c.dest = -1;

        printf("[P%d] Thread receptora: recebeu de P%d -> (%d,%d,%d)\n",
               state.pid, status.MPI_SOURCE, c.p[0], c.p[1], c.p[2]);
        fflush(stdout);

        queue_push(&state.recv_queue, c);
    }
    return NULL;
}

int expected_sends[] = {3, 1, 1};

void *sender_thread(void *arg) {
    (void)arg;
    int n = expected_sends[state.pid];
    for (int i = 0; i < n; i++) {
        Clock c = queue_pop(&state.send_queue);
        MPI_Send(c.p, NUM_PROC, MPI_INT, c.dest, 0, MPI_COMM_WORLD);

        printf("[P%d] Thread emissora: enviou para P%d -> (%d,%d,%d)\n",
               state.pid, c.dest, c.p[0], c.p[1], c.p[2]);
        fflush(stdout);
    }
    return NULL;
}

int main(void) {
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &state.pid);

    memset(state.clock.p, 0, sizeof(state.clock.p));
    pthread_mutex_init(&state.clock_mutex, NULL);

    queue_init(&state.recv_queue);
    queue_init(&state.send_queue);

    printf("[P%d] Estado inicial -> Clock = (0,0,0)\n", state.pid);
    fflush(stdout);

    pthread_t t_receiver, t_central, t_sender;
    pthread_create(&t_receiver, NULL, receiver_thread, NULL);
    pthread_create(&t_central,  NULL, central_thread,  NULL);
    pthread_create(&t_sender,   NULL, sender_thread,   NULL);

    pthread_join(t_central,  NULL);
    pthread_join(t_receiver, NULL);
    pthread_join(t_sender,   NULL);

    pthread_mutex_destroy(&state.clock_mutex);
    queue_destroy(&state.recv_queue);
    queue_destroy(&state.send_queue);

    MPI_Finalize();
    return 0;
}