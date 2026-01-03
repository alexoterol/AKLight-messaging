/**
 * AKLight Broker con Persistencia y FETCH Automatico
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <time.h>
#include <signal.h>
#include <sys/stat.h>

#define BROKER_PORT 7000
#define BUFFER_SIZE 4096
#define MAX_CLIENTS 20
#define MAX_TOPICS 50
#define DATA_DIR "./data"

typedef struct {
    char topic[256];
    char key[128];
    char payload[2048];
    time_t timestamp;
    long offset;
} message_t;

typedef struct {
    char name[256];
    FILE *file;
    long next_offset;
    pthread_mutex_t mutex;
} topic_t;

typedef struct {
    int socket;
    int type;
    char id[64];
    int active;
    char subscribed_topics[5][256];
    int num_subs;
} client_t;

static client_t clients[MAX_CLIENTS];
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
static topic_t topics[MAX_TOPICS];
static int num_topics = 0;
static pthread_mutex_t topics_mutex = PTHREAD_MUTEX_INITIALIZER;
static volatile int running = 1;
static int broker_id = 1;
static char data_dir[512];

static void init_data_dir(void) {
    snprintf(data_dir, sizeof(data_dir), "%s/broker%d", DATA_DIR, broker_id);
    mkdir(DATA_DIR, 0700);
    mkdir(data_dir, 0700);
    printf("[BROKER-%d] Data directory: %s\n", broker_id, data_dir);
    fflush(stdout);
}

static topic_t* find_topic(const char *name) {
    for (int i = 0; i < num_topics; i++) {
        if (strcmp(topics[i].name, name) == 0) return &topics[i];
    }
    return NULL;
}

static topic_t* create_topic(const char *name) {
    pthread_mutex_lock(&topics_mutex);
    
    if (num_topics >= MAX_TOPICS) {
        pthread_mutex_unlock(&topics_mutex);
        return NULL;
    }
    
    topic_t *t = &topics[num_topics++];
    strncpy(t->name, name, sizeof(t->name) - 1);
    t->next_offset = 0;
    pthread_mutex_init(&t->mutex, NULL);
    
    char filename[512];
    snprintf(filename, sizeof(filename), "%s/%s.log", data_dir, name);
    
    char *ptr = filename + strlen(data_dir) + 1;
    while (*ptr) {
        if (*ptr == '/') *ptr = '_';
        ptr++;
    }
    
    printf("[BROKER-%d] Creating file: %s\n", broker_id, filename);
    fflush(stdout);
    
    t->file = fopen(filename, "a+");
    
    if (!t->file) {
        printf("[BROKER-%d] ERROR: Cannot create file %s\n", broker_id, filename);
        fflush(stdout);
        pthread_mutex_unlock(&topics_mutex);
        return NULL;
    }
    
    setbuf(t->file, NULL);
    
    fseek(t->file, 0, SEEK_SET);
    char line[4096];
    while (fgets(line, sizeof(line), t->file)) t->next_offset++;
    fseek(t->file, 0, SEEK_END);
    
    pthread_mutex_unlock(&topics_mutex);
    printf("[BROKER-%d] Topic creado: %s (offset: %ld)\n", broker_id, name, t->next_offset);
    fflush(stdout);
    return t;
}

static void write_message(topic_t *t, const message_t *msg) {
    pthread_mutex_lock(&t->mutex);
    
    if (!t->file) {
        pthread_mutex_unlock(&t->mutex);
        return;
    }
    
    fprintf(t->file, "%ld|%ld|%s|%s\n", 
            msg->timestamp, t->next_offset, msg->key, msg->payload);
    fflush(t->file);
    t->next_offset++;
    
    pthread_mutex_unlock(&t->mutex);
}

static int read_messages_from_topic(topic_t *topic, long start_offset, 
                                     message_t *messages, int max_messages) {
    pthread_mutex_lock(&topic->mutex);
    
    if (!topic->file) {
        pthread_mutex_unlock(&topic->mutex);
        return 0;
    }
    
    // Asegurar que volvemos al inicio para leer todo el historial
    rewind(topic->file);
    
    char line[4096];
    int count = 0;
    
    while (fgets(line, sizeof(line), topic->file) && count < max_messages) {
        // Quitamos el salto de línea al final
        line[strcspn(line, "\n")] = 0;

        char *ptr = line;
        char *token;
        char *saveptr;

        // Usamos una lógica de tokens manual para soportar campos vacíos
        // Estructura: timestamp|offset|key|payload
        
        long ts = 0, off = 0;
        char key[128] = {0}, payload[2048] = {0};

        // 1. Timestamp
        token = strsep(&ptr, "|");
        if (token) ts = atol(token);
        
        // 2. Offset
        token = strsep(&ptr, "|");
        if (token) off = atol(token);
        
        // 3. Key (puede estar vacío)
        token = strsep(&ptr, "|");
        if (token) strncpy(key, token, sizeof(key)-1);
        
        // 4. Payload
        if (ptr) strncpy(payload, ptr, sizeof(payload)-1);

        // Verificamos si el mensaje cumple el criterio del offset
        if (off >= start_offset) {
            strncpy(messages[count].topic, topic->name, sizeof(messages[count].topic) - 1);
            strncpy(messages[count].key, key, sizeof(messages[count].key) - 1);
            strncpy(messages[count].payload, payload, sizeof(messages[count].payload) - 1);
            messages[count].timestamp = ts;
            messages[count].offset = off;
            count++;
        }
    }
    
    // Volver al final para que las futuras escrituras (PRODUCE) sigan funcionando bien
    fseek(topic->file, 0, SEEK_END);
    pthread_mutex_unlock(&topic->mutex);
    
    return count;
}

static int match_wildcard(const char *pattern, const char *topic) {
    char *wildcard = strstr(pattern, "/#");
    if (wildcard) {
        int len = wildcard - pattern;
        return strncmp(pattern, topic, len) == 0;
    }
    return strcmp(pattern, topic) == 0;
}

static void handle_fetch(int socket, const char *topic_name, long offset) {
    topic_t *topic = find_topic(topic_name);
    if (!topic) {
        printf("[BROKER-%d] FETCH: Topic not found: %s\n", broker_id, topic_name);
        fflush(stdout);
        return;
    }
    
    message_t messages[100];
    int count = read_messages_from_topic(topic, offset, messages, 100);
    
    printf("[BROKER-%d] FETCH: Sending %d messages from %s (offset >= %ld)\n", 
           broker_id, count, topic_name, offset);
    fflush(stdout);
    
    for (int i = 0; i < count; i++) {
        char response[4096];
        snprintf(response, sizeof(response),
                "MESSAGE|%s|%ld|%s|%s\n",
                messages[i].topic, messages[i].offset, 
                messages[i].key, messages[i].payload);
        send(socket, response, strlen(response), 0);
    }
    
    char end_msg[256];
    snprintf(end_msg, sizeof(end_msg), "FETCH_END|%d\n", count);
    send(socket, end_msg, strlen(end_msg), 0);
}

static void handle_produce(int socket, const char *topic_name, const char *key, const char *payload) {
    topic_t *topic = find_topic(topic_name);
    if (!topic) topic = create_topic(topic_name);
    if (!topic) return;
    
    message_t msg;
    strncpy(msg.topic, topic_name, sizeof(msg.topic) - 1);
    strncpy(msg.key, key ? key : "", sizeof(msg.key) - 1);
    strncpy(msg.payload, payload, sizeof(msg.payload) - 1);
    msg.timestamp = time(NULL);
    msg.offset = topic->next_offset;
    
    write_message(topic, &msg);
    
    char response[256];
    snprintf(response, sizeof(response), "ACK|%s|%ld\n", topic_name, msg.offset);
    send(socket, response, strlen(response), 0);
    
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].type == 1) {
            for (int j = 0; j < clients[i].num_subs; j++) {
                if (match_wildcard(clients[i].subscribed_topics[j], topic_name)) {
                    char consumer_msg[4096];
                    snprintf(consumer_msg, sizeof(consumer_msg),
                            "MESSAGE|%s|%ld|%s|%s\n", topic_name, msg.offset, msg.key, msg.payload);
                    send(clients[i].socket, consumer_msg, strlen(consumer_msg), 0);
                    break;
                }
            }
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

static void handle_subscribe(int idx, const char *pattern) {
    pthread_mutex_lock(&clients_mutex);
    if (clients[idx].num_subs < 5) {
        strncpy(clients[idx].subscribed_topics[clients[idx].num_subs++], pattern, 255);
        printf("[BROKER-%d] Client %s subscribed to: %s\n", broker_id, clients[idx].id, pattern);
        fflush(stdout);
        char response[256];
        snprintf(response, sizeof(response), "SUBSCRIBED|%s\n", pattern);
        send(clients[idx].socket, response, strlen(response), 0);
    }
    pthread_mutex_unlock(&clients_mutex);
}

static void* handle_client(void *arg) {
    int *args = (int*)arg;
    int socket = args[0];
    int idx = args[1];
    free(arg);
    
    char buffer[BUFFER_SIZE];
    
    while (running) {
        int bytes = recv(socket, buffer, BUFFER_SIZE - 1, 0);
        if (bytes <= 0) break;
        
        buffer[bytes] = '\0';
        char *line = strtok(buffer, "\n");
        
        while (line) {
            char *parts[4] = {NULL, NULL, NULL, NULL};
            int part_count = 0;
            char *start = line;
            char *pos = line;
            
            while (*pos && part_count < 4) {
                if (*pos == '|') {
                    *pos = '\0';
                    parts[part_count++] = start;
                    start = pos + 1;
                }
                pos++;
            }
            if (part_count < 4 && *start) {
                parts[part_count++] = start;
            }
            
            if (part_count >= 1) {
                char *cmd = parts[0];
                
                if (strcmp(cmd, "REGISTER_PRODUCER") == 0 && part_count >= 2) {
                    clients[idx].type = 0;
                    strncpy(clients[idx].id, parts[1], 63);
                    printf("[BROKER-%d] Producer registered: %s\n", broker_id, parts[1]);
                    fflush(stdout);
                    
                } else if (strcmp(cmd, "REGISTER_CONSUMER") == 0 && part_count >= 2) {
                    clients[idx].type = 1;
                    strncpy(clients[idx].id, parts[1], 63);
                    printf("[BROKER-%d] Consumer registered: %s\n", broker_id, parts[1]);
                    fflush(stdout);
                    
                } else if (strcmp(cmd, "PRODUCE") == 0 && part_count >= 4) {
                    handle_produce(socket, parts[1], parts[2], parts[3]);
                    
                } else if (strcmp(cmd, "SUBSCRIBE") == 0 && part_count >= 2) {
                    handle_subscribe(idx, parts[1]);
                    
                } else if (strcmp(cmd, "FETCH") == 0 && part_count >= 3) {
                    handle_fetch(socket, parts[1], atol(parts[2]));
                }
            }
            
            line = strtok(NULL, "\n");
        }
    }
    
    pthread_mutex_lock(&clients_mutex);
    clients[idx].active = 0;
    pthread_mutex_unlock(&clients_mutex);
    close(socket);
    
    return NULL;
}

static void signal_handler(int sig) {
    running = 0;
}

int main(int argc, char *argv[]) {
    if (argc >= 2) broker_id = atoi(argv[1]);
    
    printf("\n=== AKLight Broker %d ===\n\n", broker_id);
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    init_data_dir();
    memset(clients, 0, sizeof(clients));
    memset(topics, 0, sizeof(topics));
    
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(BROKER_PORT + broker_id - 1);
    
    bind(server_socket, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_socket, 10);
    
    printf("Listening on port %d\n", BROKER_PORT + broker_id - 1);
    fflush(stdout);
    
    while (running) {
        struct sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &len);
        
        if (client_socket < 0) continue;
        
        int idx = -1;
        pthread_mutex_lock(&clients_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active) {
                idx = i;
                clients[i].active = 1;
                clients[i].socket = client_socket;
                clients[i].num_subs = 0;
                break;
            }
        }
        pthread_mutex_unlock(&clients_mutex);
        
        if (idx < 0) {
            close(client_socket);
            continue;
        }
        
        int *args = malloc(2 * sizeof(int));
        args[0] = client_socket;
        args[1] = idx;
        
        pthread_t tid;
        if (pthread_create(&tid, NULL, handle_client, args) == 0) {
            pthread_detach(tid);
        } else {
            free(args);
            clients[idx].active = 0;
            close(client_socket);
        }
    }
    
    close(server_socket);
    return 0;
}