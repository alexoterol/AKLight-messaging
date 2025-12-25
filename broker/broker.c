/**
 * AKLight Broker - Sistema tipo Apache Kafka ligero
 * 
 * Características:
 * - Tópicos multi-nivel (topic/subtopic1/subtopic2)
 * - Particiones con round-robin o clave
 * - Persistencia de mensajes en disco
 * - Clúster de brokers
 * - Wildcard multi-nivel (#)
 * - Sesiones persistentes/no persistentes
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stdarg.h>


// ============================================
// CONFIGURACIÓN
// ============================================

#define BROKER_PORT 7000
#define CLUSTER_PORT 7100
#define BUFFER_SIZE 8192
#define MAX_CLIENTS 50
#define MAX_TOPICS 100
#define MAX_PARTITIONS 10
#define MAX_SUBSCRIBERS_PER_TOPIC 20
#define MESSAGE_RETENTION_SECONDS 3600  // 1 hora por defecto
#define DATA_DIR "./data"

// ============================================
// ESTRUCTURAS DE DATOS
// ============================================

typedef enum {
    CLIENT_TYPE_PRODUCER,
    CLIENT_TYPE_CONSUMER
} client_type_t;

typedef struct {
    char topic[256];
    int partition_id;
    char key[128];
    char payload[4096];
    time_t timestamp;
    long offset;
} message_t;

typedef struct {
    int id;
    char topic[256];
    FILE *file;
    long next_offset;
    pthread_mutex_t mutex;
} partition_t;

typedef struct {
    char name[256];
    int num_partitions;
    partition_t partitions[MAX_PARTITIONS];
    int next_partition_rr;  // Round-robin counter
    pthread_mutex_t mutex;
} topic_t;

typedef struct {
    int socket;
    client_type_t type;
    char id[64];
    int active;
    // Para consumidores
    char subscribed_topics[10][256];
    int num_subscriptions;
    int persistent_session;
    long offsets[MAX_TOPICS][MAX_PARTITIONS];  // Offset tracking
} client_info_t;

typedef struct {
    int socket;
    char host[128];
    int port;
    int active;
} broker_peer_t;

// ============================================
// VARIABLES GLOBALES
// ============================================

static client_info_t clients[MAX_CLIENTS];
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

static topic_t topics[MAX_TOPICS];
static int num_topics = 0;
static pthread_mutex_t topics_mutex = PTHREAD_MUTEX_INITIALIZER;

static broker_peer_t peers[5];
static int num_peers = 0;
static pthread_mutex_t peers_mutex = PTHREAD_MUTEX_INITIALIZER;

static volatile int running = 1;
static int broker_id = 1;
static char broker_data_dir[512];

// ============================================
// UTILIDADES
// ============================================

static void log_message(const char *level, const char *format, ...) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char timestamp[32];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", t);
    
    printf("[%s] [BROKER-%d] [%s] ", timestamp, broker_id, level);
    
    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    
    printf("\n");
    fflush(stdout);
}

static unsigned long hash_key(const char *key) {
    unsigned long hash = 5381;
    int c;
    
    while ((c = *key++))
        hash = ((hash << 5) + hash) + c;
    
    return hash;
}

// ============================================
// GESTIÓN DE PERSISTENCIA
// ============================================

static void init_data_directory(void) {
    struct stat st = {0};
    
    snprintf(broker_data_dir, sizeof(broker_data_dir), 
             "%s/broker%d", DATA_DIR, broker_id);
    
    if (stat(broker_data_dir, &st) == -1) {
        mkdir(DATA_DIR, 0700);
        mkdir(broker_data_dir, 0700);
        log_message("INFO", "Directorio de datos creado: %s", broker_data_dir);
    }
}

static char* get_partition_filename(const char *topic, int partition_id) {
    static char filename[1024];
    
    // Reemplazar / por _ en el topic para nombre de archivo válido
    char safe_topic[256];
    strncpy(safe_topic, topic, sizeof(safe_topic) - 1);
    for (int i = 0; safe_topic[i]; i++) {
        if (safe_topic[i] == '/') safe_topic[i] = '_';
    }
    
    snprintf(filename, sizeof(filename), 
             "%s/%s_p%d.log", broker_data_dir, safe_topic, partition_id);
    
    return filename;
}

static int init_partition(partition_t *part, const char *topic, int id) {
    part->id = id;
    strncpy(part->topic, topic, sizeof(part->topic) - 1);
    part->next_offset = 0;
    pthread_mutex_init(&part->mutex, NULL);
    
    // Abrir archivo de partición
    char *filename = get_partition_filename(topic, id);
    part->file = fopen(filename, "a+");
    
    if (!part->file) {
        log_message("ERROR", "No se pudo abrir archivo de partición: %s", filename);
        return -1;
    }
    
    // Contar mensajes existentes para determinar next_offset
    fseek(part->file, 0, SEEK_SET);
    char line[8192];
    while (fgets(line, sizeof(line), part->file)) {
        part->next_offset++;
    }
    
    log_message("INFO", "Partición inicializada: %s (offset: %ld)", 
               filename, part->next_offset);
    
    return 0;
}

static int write_message_to_partition(partition_t *part, const message_t *msg) {
    pthread_mutex_lock(&part->mutex);
    
    // Formato: timestamp|offset|key|payload\n
    fprintf(part->file, "%ld|%ld|%s|%s\n", 
            msg->timestamp, part->next_offset, msg->key, msg->payload);
    fflush(part->file);
    
    part->next_offset++;
    
    pthread_mutex_unlock(&part->mutex);
    
    return 0;
}

static int read_messages_from_partition(partition_t *part, long start_offset, 
                                       message_t *messages, int max_messages) {
    pthread_mutex_lock(&part->mutex);
    
    fseek(part->file, 0, SEEK_SET);
    
    char line[8192];
    long current_offset = 0;
    int count = 0;
    
    while (fgets(line, sizeof(line), part->file) && count < max_messages) {
        if (current_offset >= start_offset) {
            // Parsear línea
            long timestamp, offset;
            char key[128], payload[4096];
            
            if (sscanf(line, "%ld|%ld|%127[^|]|%4095[^\n]", 
                      &timestamp, &offset, key, payload) == 4) {
                
                strncpy(messages[count].topic, part->topic, sizeof(messages[count].topic) - 1);
                messages[count].partition_id = part->id;
                strncpy(messages[count].key, key, sizeof(messages[count].key) - 1);
                strncpy(messages[count].payload, payload, sizeof(messages[count].payload) - 1);
                messages[count].timestamp = timestamp;
                messages[count].offset = offset;
                
                count++;
            }
        }
        current_offset++;
    }
    
    pthread_mutex_unlock(&part->mutex);
    
    return count;
}

// ============================================
// GESTIÓN DE TÓPICOS Y PARTICIONES
// ============================================

static topic_t* find_topic(const char *topic_name) {
    for (int i = 0; i < num_topics; i++) {
        if (strcmp(topics[i].name, topic_name) == 0) {
            return &topics[i];
        }
    }
    return NULL;
}

static topic_t* create_topic(const char *topic_name, int num_partitions) {
    pthread_mutex_lock(&topics_mutex);
    
    if (num_topics >= MAX_TOPICS) {
        pthread_mutex_unlock(&topics_mutex);
        return NULL;
    }
    
    topic_t *topic = &topics[num_topics];
    strncpy(topic->name, topic_name, sizeof(topic->name) - 1);
    topic->num_partitions = num_partitions;
    topic->next_partition_rr = 0;
    pthread_mutex_init(&topic->mutex, NULL);
    
    // Inicializar particiones
    for (int i = 0; i < num_partitions; i++) {
        if (init_partition(&topic->partitions[i], topic_name, i) != 0) {
            pthread_mutex_unlock(&topics_mutex);
            return NULL;
        }
    }
    
    num_topics++;
    
    pthread_mutex_unlock(&topics_mutex);
    
    log_message("INFO", "Tópico creado: %s con %d particiones", 
               topic_name, num_partitions);
    
    return topic;
}

static int select_partition(topic_t *topic, const char *key) {
    if (key && strlen(key) > 0) {
        // Por clave: usar hash
        return hash_key(key) % topic->num_partitions;
    } else {
        // Round-robin
        pthread_mutex_lock(&topic->mutex);
        int partition = topic->next_partition_rr;
        topic->next_partition_rr = (topic->next_partition_rr + 1) % topic->num_partitions;
        pthread_mutex_unlock(&topic->mutex);
        return partition;
    }
}

static int match_wildcard(const char *pattern, const char *topic) {
    // Soporta wildcard multi-nivel: topic/subtopic/#
    
    char pattern_copy[256], topic_copy[256];
    strncpy(pattern_copy, pattern, sizeof(pattern_copy) - 1);
    strncpy(topic_copy, topic, sizeof(topic_copy) - 1);
    
    // Si pattern termina en /#, coincide con todo lo que empiece con pattern
    char *wildcard_pos = strstr(pattern_copy, "/#");
    if (wildcard_pos) {
        *wildcard_pos = '\0';
        return strncmp(pattern_copy, topic_copy, strlen(pattern_copy)) == 0;
    }
    
    // Match exacto
    return strcmp(pattern_copy, topic_copy) == 0;
}

// ============================================
// MANEJO DE MENSAJES
// ============================================

static void handle_produce(int client_socket, const char *topic_name, 
                          const char *key, const char *payload) {
    
    // Buscar o crear tópico
    topic_t *topic = find_topic(topic_name);
    if (!topic) {
        topic = create_topic(topic_name, 3);  // 3 particiones por defecto
        if (!topic) {
            log_message("ERROR", "No se pudo crear tópico: %s", topic_name);
            return;
        }
    }
    
    // Seleccionar partición
    int partition_id = select_partition(topic, key);
    
    // Crear mensaje
    message_t msg;
    strncpy(msg.topic, topic_name, sizeof(msg.topic) - 1);
    msg.partition_id = partition_id;
    strncpy(msg.key, key ? key : "", sizeof(msg.key) - 1);
    strncpy(msg.payload, payload, sizeof(msg.payload) - 1);
    msg.timestamp = time(NULL);
    msg.offset = topic->partitions[partition_id].next_offset;
    
    // Escribir a partición
    write_message_to_partition(&topic->partitions[partition_id], &msg);
    
    log_message("INFO", "Mensaje almacenado: %s [P%d] offset=%ld", 
               topic_name, partition_id, msg.offset);
    
    // Enviar ACK al productor
    char response[256];
    snprintf(response, sizeof(response), 
             "ACK|%s|%d|%ld\n", topic_name, partition_id, msg.offset);
    send(client_socket, response, strlen(response), 0);
    
    // Propagar a consumidores suscritos
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].type == CLIENT_TYPE_CONSUMER) {
            // Verificar suscripciones
            for (int j = 0; j < clients[i].num_subscriptions; j++) {
                if (match_wildcard(clients[i].subscribed_topics[j], topic_name)) {
                    // Enviar mensaje al consumidor
                    char consumer_msg[8192];
                    snprintf(consumer_msg, sizeof(consumer_msg),
                            "MESSAGE|%s|%d|%ld|%s|%s\n",
                            topic_name, partition_id, msg.offset, msg.key, msg.payload);
                    send(clients[i].socket, consumer_msg, strlen(consumer_msg), 0);
                    
                    log_message("DEBUG", "Mensaje enviado a consumidor %s", clients[i].id);
                    break;
                }
            }
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

static void handle_subscribe(int client_index, const char *topic_pattern) {
    pthread_mutex_lock(&clients_mutex);
    
    if (clients[client_index].num_subscriptions < 10) {
        strncpy(clients[client_index].subscribed_topics[clients[client_index].num_subscriptions],
               topic_pattern, 255);
        clients[client_index].num_subscriptions++;
        
        log_message("INFO", "Cliente %s suscrito a: %s", 
                   clients[client_index].id, topic_pattern);
        
        // Enviar ACK
        char response[256];
        snprintf(response, sizeof(response), "SUBSCRIBED|%s\n", topic_pattern);
        send(clients[client_index].socket, response, strlen(response), 0);
    }
    
    pthread_mutex_unlock(&clients_mutex);
}

static void handle_fetch(int client_socket, int client_index, 
                        const char *topic_name, int partition_id, long offset) {
    
    topic_t *topic = find_topic(topic_name);
    if (!topic || partition_id >= topic->num_partitions) {
        log_message("ERROR", "Tópico/partición inválida: %s/%d", topic_name, partition_id);
        return;
    }
    
    message_t messages[100];
    int count = read_messages_from_partition(&topic->partitions[partition_id], 
                                            offset, messages, 100);
    
    log_message("INFO", "FETCH: %d mensajes desde %s[P%d] offset %ld", 
               count, topic_name, partition_id, offset);
    
    // Enviar mensajes al consumidor
    for (int i = 0; i < count; i++) {
        char response[8192];
        snprintf(response, sizeof(response),
                "MESSAGE|%s|%d|%ld|%s|%s\n",
                messages[i].topic, messages[i].partition_id, messages[i].offset,
                messages[i].key, messages[i].payload);
        send(client_socket, response, strlen(response), 0);
    }
    
    // Actualizar offset del cliente
    pthread_mutex_lock(&clients_mutex);
    if (count > 0) {
        clients[client_index].offsets[0][partition_id] = messages[count-1].offset + 1;
    }
    pthread_mutex_unlock(&clients_mutex);
}

// ============================================
// THREAD DE CLIENTE
// ============================================

static void* handle_client(void *arg) {
    int *args = (int*)arg;
    int client_socket = args[0];
    int client_index = args[1];
    free(arg);
    
    char buffer[BUFFER_SIZE];
    
    log_message("INFO", "Cliente conectado [socket=%d]", client_socket);
    
    while (running) {
        int bytes = recv(client_socket, buffer, BUFFER_SIZE - 1, 0);
        
        if (bytes <= 0) break;
        
        buffer[bytes] = '\0';
        
        // Parsear comando
        char *line = strtok(buffer, "\n");
        while (line) {
            char command[64], arg1[256], arg2[256], arg3[4096], arg4[256];
            int parsed = sscanf(line, "%63[^|]|%255[^|]|%255[^|]|%4095[^|]|%255s", 
                               command, arg1, arg2, arg3, arg4);
            
            if (parsed >= 1) {
                if (strcmp(command, "REGISTER_PRODUCER") == 0 && parsed >= 2) {
                    pthread_mutex_lock(&clients_mutex);
                    clients[client_index].type = CLIENT_TYPE_PRODUCER;
                    strncpy(clients[client_index].id, arg1, sizeof(clients[client_index].id) - 1);
                    pthread_mutex_unlock(&clients_mutex);
                    
                    log_message("INFO", "Productor registrado: %s", arg1);
                    
                } else if (strcmp(command, "REGISTER_CONSUMER") == 0 && parsed >= 3) {
                    pthread_mutex_lock(&clients_mutex);
                    clients[client_index].type = CLIENT_TYPE_CONSUMER;
                    strncpy(clients[client_index].id, arg1, sizeof(clients[client_index].id) - 1);
                    clients[client_index].persistent_session = atoi(arg2);
                    pthread_mutex_unlock(&clients_mutex);
                    
                    log_message("INFO", "Consumidor registrado: %s (persistente=%d)", 
                               arg1, clients[client_index].persistent_session);
                    
                } else if (strcmp(command, "PRODUCE") == 0 && parsed >= 4) {
                    // PRODUCE|topic|key|payload
                    handle_produce(client_socket, arg1, arg2, arg3);
                    
                } else if (strcmp(command, "SUBSCRIBE") == 0 && parsed >= 2) {
                    // SUBSCRIBE|topic_pattern
                    handle_subscribe(client_index, arg1);
                    
                } else if (strcmp(command, "FETCH") == 0 && parsed >= 4) {
                    // FETCH|topic|partition|offset
                    handle_fetch(client_socket, client_index, arg1, atoi(arg2), atol(arg3));
                }
            }
            
            line = strtok(NULL, "\n");
        }
    }
    
    // Limpiar
    pthread_mutex_lock(&clients_mutex);
    clients[client_index].active = 0;
    pthread_mutex_unlock(&clients_mutex);
    
    close(client_socket);
    log_message("INFO", "Cliente desconectado");
    
    return NULL;
}

// ============================================
// SIGNAL HANDLER
// ============================================

static void signal_handler(int signum) {
    log_message("INFO", "Señal recibida, cerrando broker...");
    running = 0;
}

// ============================================
// FUNCIÓN PRINCIPAL
// ============================================

int main(int argc, char *argv[]) {
    if (argc >= 2) {
        broker_id = atoi(argv[1]);
    }
    
    printf("\n");
    printf("============================================================\n");
    printf("AKLight Broker %d - Sistema tipo Apache Kafka Ligero\n", broker_id);
    printf("============================================================\n\n");
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Inicializar directorio de datos
    init_data_directory();
    
    // Inicializar estructuras
    memset(clients, 0, sizeof(clients));
    memset(topics, 0, sizeof(topics));
    
    // Crear socket
    int broker_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (broker_socket < 0) {
        log_message("ERROR", "No se pudo crear socket");
        return 1;
    }
    
    int opt = 1;
    setsockopt(broker_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in broker_addr;
    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_addr.s_addr = INADDR_ANY;
    broker_addr.sin_port = htons(BROKER_PORT + broker_id - 1);
    
    if (bind(broker_socket, (struct sockaddr*)&broker_addr, sizeof(broker_addr)) < 0) {
        log_message("ERROR", "Error en bind: %s", strerror(errno));
        close(broker_socket);
        return 1;
    }
    
    if (listen(broker_socket, 10) < 0) {
        log_message("ERROR", "Error en listen");
        close(broker_socket);
        return 1;
    }
    
    log_message("INFO", "✓ Broker escuchando en puerto %d", BROKER_PORT + broker_id - 1);
    log_message("INFO", "Directorio de datos: %s", broker_data_dir);
    log_message("INFO", "Esperando clientes...\n");
    
    // Loop principal
    while (running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_socket = accept(broker_socket, 
                                   (struct sockaddr*)&client_addr, 
                                   &client_len);
        
        if (client_socket < 0) {
            if (running) {
                log_message("ERROR", "Error en accept");
            }
            continue;
        }
        
        // Buscar slot libre
        int client_index = -1;
        pthread_mutex_lock(&clients_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active) {
                client_index = i;
                clients[i].active = 1;
                clients[i].socket = client_socket;
                clients[i].num_subscriptions = 0;
                clients[i].persistent_session = 0;
                memset(clients[i].offsets, 0, sizeof(clients[i].offsets));
                break;
            }
        }
        pthread_mutex_unlock(&clients_mutex);
        
        if (client_index < 0) {
            log_message("ERROR", "No hay espacio para más clientes");
            close(client_socket);
            continue;
        }
        
        // Crear thread
        int *args = malloc(2 * sizeof(int));
        args[0] = client_socket;
        args[1] = client_index;
        
        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, handle_client, args) != 0) {
            log_message("ERROR", "Error creando thread");
            pthread_mutex_lock(&clients_mutex);
            clients[client_index].active = 0;
            pthread_mutex_unlock(&clients_mutex);
            close(client_socket);
            free(args);
        } else {
            pthread_detach(thread_id);
        }
    }
    
    log_message("INFO", "Broker cerrado");
    close(broker_socket);
    
    return 0;
}

/**
 * COMPILACIÓN:
 * gcc -o broker broker.c -pthread -Wall -Wextra
 * 
 * EJECUCIÓN:
 * ./broker 1  # Broker ID 1 (puerto 7000)
 * ./broker 2  # Broker ID 2 (puerto 7001)
 */
