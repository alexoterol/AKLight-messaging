/**
 * ============================================================================
 * AKLight v2 - Header Común con TDAs y Definiciones
 * ============================================================================
 * 
 * Tipos de Datos Abstractos implementados:
 * 1. MESSAGE_QUEUE   - Cola de mensajes (lista enlazada + mutex + condvar)
 * 2. PARTITION_TABLE - Tabla hash para particiones (hash + rwlock)
 * 3. RING_BUFFER     - Buffer circular para logs recientes
 * 4. TOPIC_TREE      - Árbol N-ario para tópicos multinivel
 * 
 * Mecanismos de sincronización:
 * - pthread_mutex_t    : Exclusión mutua básica
 * - pthread_rwlock_t   : Read-Write locks (múltiples lectores, un escritor)
 * - sem_t              : Semáforos para control de recursos
 * - pthread_cond_t     : Variables de condición para productor-consumidor
 * ============================================================================
 */

#ifndef AKLIGHT_COMMON_H
#define AKLIGHT_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <stdint.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

/* ============================================================================
 * CONSTANTES DE CONFIGURACIÓN
 * ============================================================================ */

#define AKLIGHT_VERSION         "2.0.0"
#define MAX_TOPIC_LENGTH        256
#define MAX_KEY_LENGTH          128
#define MAX_PAYLOAD_LENGTH      2048
#define MAX_ID_LENGTH           64
#define BUFFER_SIZE             4096

/* Broker */
#define DEFAULT_BROKER_PORT     7000
#define MAX_CLIENTS             50
#define MAX_TOPICS              100
#define MAX_PARTITIONS          8
#define PARTITION_BUCKETS       256

/* TDAs */
#define RING_BUFFER_SIZE        1024
#define TOPIC_TREE_MAX_CHILDREN 64
#define HASH_BUCKET_SIZE        256

/* Comunicación */
#define MSG_DELIMITER           '|'
#define MSG_TERMINATOR          '\n'

/* ============================================================================
 * ESTRUCTURAS DE MENSAJES
 * ============================================================================ */

/**
 * Estructura de un mensaje en el sistema
 */
typedef struct {
    char topic[MAX_TOPIC_LENGTH];
    char key[MAX_KEY_LENGTH];
    char payload[MAX_PAYLOAD_LENGTH];
    time_t timestamp;
    long offset;
    int partition_id;
} aklight_message_t;

/**
 * Tipos de cliente
 */
typedef enum {
    CLIENT_TYPE_UNKNOWN = 0,
    CLIENT_TYPE_PRODUCER = 1,
    CLIENT_TYPE_CONSUMER = 2,
    CLIENT_TYPE_BROKER = 3
} client_type_t;

/* ============================================================================
 * TDA 1: COLA DE MENSAJES (Message Queue)
 * Implementación: Lista enlazada + mutex + condvar
 * Complejidad: O(1) para enqueue y dequeue
 * ============================================================================ */

typedef struct msg_queue_node {
    aklight_message_t message;
    struct msg_queue_node *next;
} msg_queue_node_t;

typedef struct {
    msg_queue_node_t *head;
    msg_queue_node_t *tail;
    size_t size;
    size_t max_size;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} message_queue_t;

static inline void mq_init(message_queue_t *q, size_t max_size) {
    q->head = NULL;
    q->tail = NULL;
    q->size = 0;
    q->max_size = max_size;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

static inline void mq_destroy(message_queue_t *q) {
    pthread_mutex_lock(&q->mutex);
    msg_queue_node_t *current = q->head;
    while (current) {
        msg_queue_node_t *next = current->next;
        free(current);
        current = next;
    }
    q->head = q->tail = NULL;
    q->size = 0;
    pthread_mutex_unlock(&q->mutex);
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
}

static inline int mq_enqueue(message_queue_t *q, const aklight_message_t *msg) {
    msg_queue_node_t *node = (msg_queue_node_t*)malloc(sizeof(msg_queue_node_t));
    if (!node) return -1;
    
    memcpy(&node->message, msg, sizeof(aklight_message_t));
    node->next = NULL;
    
    pthread_mutex_lock(&q->mutex);
    while (q->max_size > 0 && q->size >= q->max_size) {
        pthread_cond_wait(&q->not_full, &q->mutex);
    }
    
    if (q->tail) {
        q->tail->next = node;
    } else {
        q->head = node;
    }
    q->tail = node;
    q->size++;
    
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

static inline int mq_dequeue(message_queue_t *q, aklight_message_t *msg) {
    pthread_mutex_lock(&q->mutex);
    while (q->size == 0) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }
    
    msg_queue_node_t *node = q->head;
    memcpy(msg, &node->message, sizeof(aklight_message_t));
    
    q->head = node->next;
    if (!q->head) q->tail = NULL;
    q->size--;
    
    free(node);
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

static inline int mq_try_dequeue(message_queue_t *q, aklight_message_t *msg) {
    pthread_mutex_lock(&q->mutex);
    if (q->size == 0) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    
    msg_queue_node_t *node = q->head;
    memcpy(msg, &node->message, sizeof(aklight_message_t));
    
    q->head = node->next;
    if (!q->head) q->tail = NULL;
    q->size--;
    
    free(node);
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* ============================================================================
 * TDA 2: TABLA HASH DE PARTICIONES
 * Implementación: Hash table con chaining + RW-lock
 * Complejidad: O(1) promedio
 * ============================================================================ */

typedef struct partition {
    int id;
    char topic_name[MAX_TOPIC_LENGTH];
    FILE *log_file;
    long next_offset;
    pthread_mutex_t mutex;
    struct partition *next;
} partition_t;

typedef struct {
    partition_t *buckets[PARTITION_BUCKETS];
    pthread_rwlock_t rwlock;
    int total_partitions;
    int round_robin_counter;
    pthread_mutex_t rr_mutex;
} partition_table_t;

static inline uint32_t djb2_hash(const char *str) {
    uint32_t hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

static inline void pt_init(partition_table_t *pt) {
    memset(pt->buckets, 0, sizeof(pt->buckets));
    pthread_rwlock_init(&pt->rwlock, NULL);
    pthread_mutex_init(&pt->rr_mutex, NULL);
    pt->total_partitions = 0;
    pt->round_robin_counter = 0;
}

static inline partition_t* pt_find_unlocked(partition_table_t *pt, 
                                            const char *topic, int partition_id) {
    char key[MAX_TOPIC_LENGTH + 16];
    snprintf(key, sizeof(key), "%s_p%d", topic, partition_id);
    uint32_t idx = djb2_hash(key) % PARTITION_BUCKETS;
    
    partition_t *p = pt->buckets[idx];
    while (p) {
        if (strcmp(p->topic_name, topic) == 0 && p->id == partition_id) {
            return p;
        }
        p = p->next;
    }
    return NULL;
}

static inline partition_t* pt_find(partition_table_t *pt, 
                                   const char *topic, int partition_id) {
    pthread_rwlock_rdlock(&pt->rwlock);
    partition_t *p = pt_find_unlocked(pt, topic, partition_id);
    pthread_rwlock_unlock(&pt->rwlock);
    return p;
}

static inline int pt_calculate_partition(partition_table_t *pt, 
                                         const char *key, int num_partitions) {
    if (num_partitions <= 0) return 0;
    
    if (key && strlen(key) > 0) {
        return djb2_hash(key) % num_partitions;
    } else {
        pthread_mutex_lock(&pt->rr_mutex);
        int partition = pt->round_robin_counter % num_partitions;
        pt->round_robin_counter++;
        pthread_mutex_unlock(&pt->rr_mutex);
        return partition;
    }
}

/* ============================================================================
 * TDA 3: RING BUFFER
 * Implementación: Array circular
 * Complejidad: O(1) para push
 * ============================================================================ */

typedef struct {
    time_t timestamp;
    char data[512];
} ring_entry_t;

typedef struct {
    ring_entry_t entries[RING_BUFFER_SIZE];
    int head;
    int count;
    pthread_mutex_t mutex;
} ring_buffer_t;

static inline void rb_init(ring_buffer_t *rb) {
    memset(rb->entries, 0, sizeof(rb->entries));
    rb->head = 0;
    rb->count = 0;
    pthread_mutex_init(&rb->mutex, NULL);
}

static inline void rb_push(ring_buffer_t *rb, const char *data) {
    pthread_mutex_lock(&rb->mutex);
    rb->entries[rb->head].timestamp = time(NULL);
    strncpy(rb->entries[rb->head].data, data, sizeof(rb->entries[0].data) - 1);
    rb->head = (rb->head + 1) % RING_BUFFER_SIZE;
    if (rb->count < RING_BUFFER_SIZE) rb->count++;
    pthread_mutex_unlock(&rb->mutex);
}

/* ============================================================================
 * TDA 4: WILDCARD MATCHING PARA TÓPICOS N-NIVELES
 * Soporta: # (multi-nivel) y + (un nivel)
 * ============================================================================ */

static inline int wildcard_match(const char *pattern, const char *topic) {
    char p_copy[MAX_TOPIC_LENGTH];
    char t_copy[MAX_TOPIC_LENGTH];
    strncpy(p_copy, pattern, sizeof(p_copy) - 1);
    strncpy(t_copy, topic, sizeof(t_copy) - 1);
    p_copy[sizeof(p_copy) - 1] = '\0';
    t_copy[sizeof(t_copy) - 1] = '\0';
    
    char *p_saveptr, *t_saveptr;
    char *p_token = strtok_r(p_copy, "/", &p_saveptr);
    char *t_token = strtok_r(t_copy, "/", &t_saveptr);
    
    while (p_token) {
        if (strcmp(p_token, "#") == 0) {
            return 1;
        } else if (strcmp(p_token, "+") == 0) {
            if (!t_token) return 0;
        } else {
            if (!t_token || strcmp(p_token, t_token) != 0) return 0;
        }
        
        p_token = strtok_r(NULL, "/", &p_saveptr);
        t_token = strtok_r(NULL, "/", &t_saveptr);
    }
    
    return (t_token == NULL);
}

static inline int count_topic_levels(const char *topic) {
    if (!topic || strlen(topic) == 0) return 0;
    int levels = 1;
    const char *p = topic;
    while (*p) {
        if (*p == '/') levels++;
        p++;
    }
    return levels;
}

/* ============================================================================
 * UTILIDADES
 * ============================================================================ */

static inline int parse_message(char *line, char **parts, int max_parts) {
    int count = 0;
    char *start = line;
    char *pos = line;
    
    while (*pos && count < max_parts) {
        if (*pos == MSG_DELIMITER) {
            *pos = '\0';
            parts[count++] = start;
            start = pos + 1;
        }
        pos++;
    }
    
    if (count < max_parts && *start) {
        char *nl = strchr(start, '\n');
        if (nl) *nl = '\0';
        parts[count++] = start;
    }
    
    return count;
}

/* ============================================================================
 * LOGGING
 * ============================================================================ */

#define LOG_DEBUG   0
#define LOG_INFO    1
#define LOG_WARN    2
#define LOG_ERROR   3

static int g_log_level = LOG_INFO;

static inline void aklight_log(int level, const char *component, 
                               const char *fmt, ...) {
    if (level < g_log_level) return;
    
    const char *level_str[] = {"DEBUG", "INFO", "WARN", "ERROR"};
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    char time_buf[32];
    strftime(time_buf, sizeof(time_buf), "%H:%M:%S", tm_info);
    
    fprintf(stdout, "[%s][%s][%s] ", time_buf, level_str[level], component);
    
    va_list args;
    va_start(args, fmt);
    vfprintf(stdout, fmt, args);
    va_end(args);
    
    fprintf(stdout, "\n");
    fflush(stdout);
}

#define LOG_D(comp, ...) aklight_log(LOG_DEBUG, comp, __VA_ARGS__)
#define LOG_I(comp, ...) aklight_log(LOG_INFO, comp, __VA_ARGS__)
#define LOG_W(comp, ...) aklight_log(LOG_WARN, comp, __VA_ARGS__)
#define LOG_E(comp, ...) aklight_log(LOG_ERROR, comp, __VA_ARGS__)

#endif /* AKLIGHT_COMMON_H */
