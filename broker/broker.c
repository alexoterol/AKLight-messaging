/**
 * AKLight Broker Simplificado
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
    int type;  // 0=producer, 1=consumer
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
    for (int i = 0; filename[i]; i++) 
        if (filename[i] == '/') filename[i] = '_';
    
    t->file = fopen(filename, "a+");
    
    // Contar mensajes existentes
    if (t->file) {
        fseek(t->file, 0, SEEK_SET);
        char line[4096];
        while (fgets(line, sizeof(line), t->file)) t->next_offset++;
    }
    
    pthread_mutex_unlock(&topics_mutex);
    printf("[BROKER-%d] Topic creado: %s\n", broker_id, name);
    return t;
}

static void write_message(topic_t *t, const message_t *msg) {
    pthread_mutex_lock(&t->mutex);
    fprintf(t->file, "%ld|%ld|%s|%s\n", msg->timestamp, t->next_offset, msg->key, msg->payload);
    fflush(t->file);
    t->next_offset++;
    pthread_mutex_unlock(&t->mutex);
}

static int match_wildcard(const char *pattern, const char *topic) {
    char *wildcard = strstr(pattern, "/#");
    if (wildcard) {
        int len = wildcard - pattern;
        return strncmp(pattern, topic, len) == 0;
    }
    return strcmp(pattern, topic) == 0;
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
    
    // Enviar a consumidores
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
            char cmd[64], arg1[256], arg2[256], arg3[2048];
            int n = sscanf(line, "%63[^|]|%255[^|]|%255[^|]|%2047[^\n]", cmd, arg1, arg2, arg3);
            
            if (n >= 1) {
                if (strcmp(cmd, "REGISTER_PRODUCER") == 0 && n >= 2) {
                    clients[idx].type = 0;
                    strncpy(clients[idx].id, arg1, 63);
                } else if (strcmp(cmd, "REGISTER_CONSUMER") == 0 && n >= 2) {
                    clients[idx].type = 1;
                    strncpy(clients[idx].id, arg1, 63);
                } else if (strcmp(cmd, "PRODUCE") == 0 && n >= 4) {
                    handle_produce(socket, arg1, arg2, arg3);
                } else if (strcmp(cmd, "SUBSCRIBE") == 0 && n >= 2) {
                    handle_subscribe(idx, arg1);
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