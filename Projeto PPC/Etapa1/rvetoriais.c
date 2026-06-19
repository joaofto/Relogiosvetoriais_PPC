/*
 * ============================================================
 * ETAPA 3 - PARTE 1
 * Estruturas de dados e filas produtor/consumidor
 * ============================================================
 */

#include <pthread.h>

#define BUFFER_SIZE 5

/* ============================================================
 * ESTRUTURA DA MENSAGEM
 * ============================================================
 */

typedef struct {
    Clock clock;
    int origem;
    int destino;
    int tag;
} Message;

/* ============================================================
 * ESTRUTURA DA FILA
 * ============================================================
 */

typedef struct {

    Message buffer[BUFFER_SIZE];

    int inicio;
    int fim;
    int quantidade;

    pthread_mutex_t mutex;
    pthread_cond_t notEmpty;
    pthread_cond_t notFull;

} Queue;

/* ============================================================
 * FILA DE RECEPÇÃO
 * ============================================================
 */

Queue receiveQueue = {
    .inicio = 0,
    .fim = 0,
    .quantidade = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .notEmpty = PTHREAD_COND_INITIALIZER,
    .notFull = PTHREAD_COND_INITIALIZER
};

/* ============================================================
 * FILA DE ENVIO
 * ============================================================
 */

Queue sendQueue = {
    .inicio = 0,
    .fim = 0,
    .quantidade = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .notEmpty = PTHREAD_COND_INITIALIZER,
    .notFull = PTHREAD_COND_INITIALIZER
};

/* ============================================================
 * ENQUEUE (INSERE EM UMA FILA)
 * ============================================================
 */

void enqueue(Queue *queue, Message msg)
{
    pthread_mutex_lock(&queue->mutex);

    while(queue->quantidade == BUFFER_SIZE)
    {
        pthread_cond_wait(&queue->notFull, &queue->mutex);
    }

    queue->buffer[queue->fim] = msg;

    queue->fim = (queue->fim + 1) % BUFFER_SIZE;

    queue->quantidade++;

    pthread_cond_signal(&queue->notEmpty);

    pthread_mutex_unlock(&queue->mutex);
}

/* ============================================================
 * DEQUEUE (REMOVE DA FILA)
 * ============================================================
 */

Message dequeue(Queue *queue)
{
    pthread_mutex_lock(&queue->mutex);

    while(queue->quantidade == 0)
    {
        pthread_cond_wait(&queue->notEmpty, &queue->mutex);
    }

    Message msg = queue->buffer[queue->inicio];

    queue->inicio = (queue->inicio + 1) % BUFFER_SIZE;

    queue->quantidade--;

    pthread_cond_signal(&queue->notFull);

    pthread_mutex_unlock(&queue->mutex);

    return msg;
}

/* ============================================================
 * OPERAÇÕES DA FILA DE ENVIO
 * ============================================================
 */

void enqueueSend(Message msg)
{
    enqueue(&sendQueue, msg);
}

Message dequeueSend()
{
    return dequeue(&sendQueue);
}

/* ============================================================
 * OPERAÇÕES DA FILA DE RECEPÇÃO
 * ============================================================
 */

void enqueueReceive(Message msg)
{
    enqueue(&receiveQueue, msg);
}

Message dequeueReceive()
{
    return dequeue(&receiveQueue);
}

/* ============================================================
 * FUNÇÃO AUXILIAR PARA DEPURAÇÃO
 * ============================================================
 */

void printMessage(Message *msg)
{
    printf("Origem: %d | Destino: %d | TAG: %d | Clock=(%d,%d,%d)\n",
           msg->origem,
           msg->destino,
           msg->tag,
           msg->clock.p[0],
           msg->clock.p[1],
           msg->clock.p[2]);

    fflush(stdout);
}

