/*
 * Arquivo:    produtor_consumidor.c
 *
 * Propósito:  Produtor/Consumidor com Relógios Vetoriais
 *             Fila intermediária controlada por variáveis de condição
 *
 * Compile:    gcc -g -Wall -o produtor_consumidor produtor_consumidor.c -lpthread
 * Execução:   ./produtor_consumidor
 *
 * Cenários:
 *   FILA_CHEIA — produtores rápidos (1s), consumidores lentos (2s)
 *   FILA_VAZIA — produtores lentos (2s), consumidores rápidos (1s)
 *
 *   Ative UM dos dois defines abaixo antes de compilar.
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

/* ================= CENÁRIO ================= */
/*
 * Descomente apenas UM:
 */
/*#define FILA_CHEIA*/   /* produtores 1s, consumidores 2s → fila enche     */
#define FILA_VAZIA  /* produtores 2s, consumidores 1s → fila esvazia  */

#if defined(FILA_CHEIA) && defined(FILA_VAZIA)
#error "Ative apenas um cenário: FILA_CHEIA ou FILA_VAZIA"
#endif
#if !defined(FILA_CHEIA) && !defined(FILA_VAZIA)
#error "Ative um cenário: defina FILA_CHEIA ou FILA_VAZIA"
#endif

/* ================= CONSTANTES ================= */
#define PRODUCER_NUM  3
#define CONSUMER_NUM  3
#define BUFFER_SIZE   5

#ifdef FILA_CHEIA
#define PRODUCER_SLEEP 1   /* rápido */
#define CONSUMER_SLEEP 2   /* lento  */
#else
#define PRODUCER_SLEEP 2   /* lento  */
#define CONSUMER_SLEEP 1   /* rápido */
#endif

/* ================= RELÓGIO VETORIAL ================= */
typedef struct {
    int p[PRODUCER_NUM];
} Clock;

/* ================= TASK ================= */
typedef struct {
    Clock clock;
    int   producerId;
} Task;

/* ================= FILA ================= */
static Task taskQueue[BUFFER_SIZE];
static int  taskCount = 0;

/* ================= SINCRONIZAÇÃO ================= */
static pthread_mutex_t mutex    = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  condFull = PTHREAD_COND_INITIALIZER; /* espaço disponível */
static pthread_cond_t  condEmpty= PTHREAD_COND_INITIALIZER; /* item disponível   */

/* ================= CONTROLE DE ENCERRAMENTO ================= */
static volatile int running = 1;

/* ================= AUXILIARES ================= */
static void printClock(const Clock *c) {
    printf("(%d,%d,%d)", c->p[0], c->p[1], c->p[2]);
}

static void printQueueState(void) {
    printf("[fila: %d/%d]", taskCount, BUFFER_SIZE);
}

/* ================= INSERIR TASK ================= */
static void submitTask(Task task) {
    pthread_mutex_lock(&mutex);

    while (taskCount == BUFFER_SIZE && running) {
        printf("[Produtor %d] Fila cheia (%d/%d) — aguardando...\n",
               task.producerId, taskCount, BUFFER_SIZE);
        fflush(stdout);
        pthread_cond_wait(&condFull, &mutex);
    }

    if (!running) {
        pthread_mutex_unlock(&mutex);
        return;
    }

    taskQueue[taskCount] = task;
    taskCount++;

    printf("[Produtor %d] inseriu relógio ", task.producerId);
    printClock(&task.clock);
    printf(" | ");
    printQueueState();
    printf("\n");
    fflush(stdout);

    pthread_cond_signal(&condEmpty);
    pthread_mutex_unlock(&mutex);
}

/* ================= REMOVER TASK ================= */
static int getTask(Task *out) {
    pthread_mutex_lock(&mutex);

    while (taskCount == 0 && running) {
        printf("[Consumidor] Fila vazia (%d/%d) — aguardando...\n",
               taskCount, BUFFER_SIZE);
        fflush(stdout);
        pthread_cond_wait(&condEmpty, &mutex);
    }

    if (taskCount == 0 && !running) {
        pthread_mutex_unlock(&mutex);
        return 0; /* sinaliza encerramento */
    }

    *out = taskQueue[0];
    for (int i = 0; i < taskCount - 1; i++)
        taskQueue[i] = taskQueue[i + 1];
    taskCount--;

    printQueueState();
    printf(" após remoção\n");
    fflush(stdout);

    pthread_cond_signal(&condFull);
    pthread_mutex_unlock(&mutex);
    return 1;
}

/* ================= THREAD PRODUTORA ================= */
static void *producerThread(void *args) {
    long id = (long)args;
    Clock localClock = {{0, 0, 0}};

    while (running) {
        localClock.p[id]++;

        Task t;
        t.producerId = (int)id;
        t.clock      = localClock;

        submitTask(t);
        sleep(PRODUCER_SLEEP);
    }
    return NULL;
}

/* ================= THREAD CONSUMIDORA ================= */
static void *consumerThread(void *args) {
    long id = (long)args;

    while (running) {
        Task task;
        if (!getTask(&task))
            break;

        printf("[Consumidor %ld] consumiu relógio do Produtor %d -> ",
               id, task.producerId);
        printClock(&task.clock);
        printf("\n");
        fflush(stdout);

        sleep(CONSUMER_SLEEP);
    }
    return NULL;
}

/* ================= MAIN ================= */
int main(void) {
#ifdef FILA_CHEIA
    printf("=== CENÁRIO: FILA CHEIA (produtores %ds | consumidores %ds) ===\n\n",
           PRODUCER_SLEEP, CONSUMER_SLEEP);
#else
    printf("=== CENÁRIO: FILA VAZIA (produtores %ds | consumidores %ds) ===\n\n",
           PRODUCER_SLEEP, CONSUMER_SLEEP);
#endif
    fflush(stdout);

    pthread_t producers[PRODUCER_NUM];
    pthread_t consumers[CONSUMER_NUM];

    /* cria produtores */
    for (long i = 0; i < PRODUCER_NUM; i++) {
        if (pthread_create(&producers[i], NULL, producerThread, (void*)i) != 0) {
            perror("Erro ao criar produtor");
            exit(EXIT_FAILURE);
        }
    }

    /* cria consumidores */
    for (long i = 0; i < CONSUMER_NUM; i++) {
        if (pthread_create(&consumers[i], NULL, consumerThread, (void*)i) != 0) {
            perror("Erro ao criar consumidor");
            exit(EXIT_FAILURE);
        }
    }

    /* executa por 20 segundos e encerra */
    sleep(20);
    printf("\n=== Encerrando demonstração ===\n");
    fflush(stdout);

    running = 0;

    /* acorda threads bloqueadas para que possam verificar running=0 */
    pthread_cond_broadcast(&condEmpty);
    pthread_cond_broadcast(&condFull);

    for (int i = 0; i < PRODUCER_NUM; i++)
        pthread_join(producers[i], NULL);
    for (int i = 0; i < CONSUMER_NUM; i++)
        pthread_join(consumers[i], NULL);

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&condFull);
    pthread_cond_destroy(&condEmpty);

    printf("=== Programa encerrado ===\n");
    return 0;
}
