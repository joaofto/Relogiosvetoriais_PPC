/*
 * Produtor/Consumidor com Relógios Vetoriais
 *
 * Compile:
 * gcc -g -Wall -o produtor_consumidor produtor_consumidor.c -lpthread
 *
 * Execução:
 * ./produtor_consumidor
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

#define PRODUCER_NUM 3
#define CONSUMER_NUM 3
#define BUFFER_SIZE 5

/* ================= RELÓGIO VETORIAL ================= */

typedef struct Clock {
    int p[3];
} Clock;

/* ================= TASK ================= */

typedef struct Task {
    Clock clock;
    int producerId;
} Task;

/* ================= FILA ================= */

Task taskQueue[BUFFER_SIZE];
int taskCount = 0;

/* ================= SINCRONIZAÇÃO ================= */

pthread_mutex_t mutex;

pthread_cond_t condFull;
pthread_cond_t condEmpty;

/* ================= CONTROLE DOS CENÁRIOS ================= */

/*
 * CENÁRIO 1 -> FILA CHEIA
 * produtores rápidos
 * consumidores lentos
 */

//#define FILA_CHEIA

/*
 * CENÁRIO 2 -> FILA VAZIA
 * produtores lentos
 * consumidores rápidos
 */
#define FILA_VAZIA

/* ================= FUNÇÕES AUXILIARES ================= */

void printClock(Clock *clock) {
    printf("(%d,%d,%d)",
           clock->p[0],
           clock->p[1],
           clock->p[2]);
}

/* ================= EXECUÇÃO DA TASK ================= */

void executeTask(Task* task, int consumerId) {

    printf("[Consumidor %d] consumiu relógio do Produtor %d -> ",
           consumerId,
           task->producerId);

    printClock(&task->clock);

    printf("\n");

    fflush(stdout);
}

/* ================= REMOVER TASK ================= */

Task getTask() {

    pthread_mutex_lock(&mutex);

    while (taskCount == 0) {

        printf("Fila vazia -> Consumidor aguardando...\n");

        pthread_cond_wait(&condEmpty, &mutex);
    }

    Task task = taskQueue[0];

    for (int i = 0; i < taskCount - 1; i++) {
        taskQueue[i] = taskQueue[i + 1];
    }

    taskCount--;

    printf("Task removida | itens na fila = %d\n", taskCount);

    pthread_mutex_unlock(&mutex);

    pthread_cond_signal(&condFull);

    return task;
}

/* ================= INSERIR TASK ================= */

void submitTask(Task task) {

    pthread_mutex_lock(&mutex);

    while (taskCount == BUFFER_SIZE) {

        printf("Fila cheia -> Produtor aguardando...\n");

        pthread_cond_wait(&condFull, &mutex);
    }

    taskQueue[taskCount] = task;
    taskCount++;

    printf("Task inserida | itens na fila = %d\n", taskCount);

    pthread_mutex_unlock(&mutex);

    pthread_cond_signal(&condEmpty);
}

/* ================= THREAD PRODUTORA ================= */

void *producerThread(void *args) {

    long id = (long) args;
    
    Clock localClock = {{0, 0, 0}};


    while (1) {

        Task t;

    
        t.producerId = id;

        /* Atualiza o relógio vetorial local */
        localClock.p[id]++;

        /* Copia o relógio para a tarefa */
        t.clock = localClock;

        printf("[Produtor %ld] produziu relógio -> ", id);

        printClock(&t.clock);

        printf("\n");

        fflush(stdout);

        submitTask(t);

#ifdef FILA_CHEIA
        sleep(1); /* produção rápida */
#endif

#ifdef FILA_VAZIA
        sleep(2); /* produção lenta */
#endif
    }

    return NULL;
}

/* ================= THREAD CONSUMIDORA ================= */

void *consumerThread(void *args) {

    long id = (long) args;

    while (1) {

        Task task = getTask();

        executeTask(&task, id);

#ifdef FILA_CHEIA
        sleep(2); /* consumo lento */
#endif

#ifdef FILA_VAZIA
        sleep(1); /* consumo rápido */
#endif
    }

    return NULL;
}

/* ================= MAIN ================= */

int main() {

    pthread_mutex_init(&mutex, NULL);

    pthread_cond_init(&condFull, NULL);
    pthread_cond_init(&condEmpty, NULL);

    pthread_t producers[PRODUCER_NUM];
    pthread_t consumers[CONSUMER_NUM];

    srand(time(NULL));

    /* ================= CRIAR PRODUTORES ================= */

    for (long i = 0; i < PRODUCER_NUM; i++) {

        if (pthread_create(&producers[i],
                           NULL,
                           producerThread,
                           (void*) i) != 0) {

            perror("Erro ao criar produtor");
        }
    }

    /* ================= CRIAR CONSUMIDORES ================= */

    for (long i = 0; i < CONSUMER_NUM; i++) {

        if (pthread_create(&consumers[i],
                           NULL,
                           consumerThread,
                           (void*) i) != 0) {

            perror("Erro ao criar consumidor");
        }
    }

    /* ================= JOIN ================= */

    for (int i = 0; i < PRODUCER_NUM; i++) {
        pthread_join(producers[i], NULL);
    }

    for (int i = 0; i < CONSUMER_NUM; i++) {
        pthread_join(consumers[i], NULL);
    }

    /* ================= DESTRUIR ================= */

    pthread_mutex_destroy(&mutex);

    pthread_cond_destroy(&condFull);
    pthread_cond_destroy(&condEmpty);

    return 0;
}