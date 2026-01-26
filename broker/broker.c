/**
 * ============================================================================
 * AKLight v2 - Broker con Particiones y Clúster (CORREGIDO v2)
 * ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <signal.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

/* ============================================================================
 * CONFIGURACIÓN
 * ============================================================================ */

#define BROKER_PORT         7000
#define BUFFER_SIZE         4096
#define MAX_CLIENTS         50
#define MAX_TOPICS          100
#define NUM_PARTITIONS      2
#define DATA_DIR            "/app/data"
#define MAX_MESSAGES        1000
#define MAX_SUBSCRIPTIONS   10

/* ============================================================================
 * ESTRUCTURAS DE DATOS
 * ============================================================================ */

typedef struct {
    char topic[256];
    char key[64];
    char payload[1024];
    time_t timestamp;
    long offset;
    int partition;
} message_t;

/* Partición con mensajes dinámicos */
typedef struct {
    message_t *messages;      /* Array dinámico */
    int count;
    int capacity;
    long next_offset;
    pthread_mutex_t mutex;
    char log_path[256];
} partition_t;

typedef struct {
    char name[256];
    partition_t partitions[NUM_PARTITIONS];
    int num_partitions;
    int round_robin_counter;
    pthread_mutex_t rr_mutex;
} topic_t;

typedef struct {
    int socket;
    int type;
    char id[64];
    int active;
    int persistent;
    char subscriptions[MAX_SUBSCRIPTIONS][256];
    int num_subs;
    pthread_mutex_t send_mutex;
} client_t;

/* ============================================================================
 * VARIABLES GLOBALES
 * ============================================================================ */

static client_t clients[MAX_CLIENTS];
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

static topic_t *topics = NULL;  /* Array dinámico de topics */
static int num_topics = 0;
static pthread_rwlock_t topics_rwlock = PTHREAD_RWLOCK_INITIALIZER;

static sem_t connection_semaphore;

static volatile int running = 1;
static int broker_id = 1;
static char data_dir[256];

/* ============================================================================
 * FUNCIONES AUXILIARES
 * ============================================================================ */

static unsigned int djb2_hash(const char *str) {
    unsigned int hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

static int calculate_partition(topic_t *topic, const char *key) {
    if (key && strlen(key) > 0) {
        return djb2_hash(key) % topic->num_partitions;
    } else {
        pthread_mutex_lock(&topic->rr_mutex);
        int partition = topic->round_robin_counter % topic->num_partitions;
        topic->round_robin_counter++;
        pthread_mutex_unlock(&topic->rr_mutex);
        return partition;
    }
}

static void init_data_dir(void) {
    snprintf(data_dir, sizeof(data_dir), "%s/broker%d", DATA_DIR, broker_id);
    
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", data_dir);
    int ret = system(cmd);
    (void)ret;
    
    printf("[BROKER-%d] 📁 Data directory: %s\n", broker_id, data_dir);
    fflush(stdout);
}

static void init_topics_array(void) {
    topics = calloc(MAX_TOPICS, sizeof(topic_t));
    if (!topics) {
        fprintf(stderr, "Error allocating topics array\n");
        exit(1);
    }
}

static int safe_send(client_t *client, const char *msg, size_t len) {
    if (!client || client->socket < 0) return -1;
    
    pthread_mutex_lock(&client->send_mutex);
    ssize_t sent = send(client->socket, msg, len, MSG_NOSIGNAL);
    pthread_mutex_unlock(&client->send_mutex);
    
    return (sent > 0) ? 0 : -1;
}

/* ============================================================================
 * WILDCARD MATCHING
 * ============================================================================ */

static int wildcard_match(const char *pattern, const char *topic) {
    char p_copy[256], t_copy[256];
    strncpy(p_copy, pattern, sizeof(p_copy) - 1);
    strncpy(t_copy, topic, sizeof(t_copy) - 1);
    p_copy[255] = t_copy[255] = '\0';
    
    char *p_save, *t_save;
    char *p_tok = strtok_r(p_copy, "/", &p_save);
    char *t_tok = strtok_r(t_copy, "/", &t_save);
    
    while (p_tok) {
        if (strcmp(p_tok, "#") == 0) {
            return 1;
        } else if (strcmp(p_tok, "+") == 0) {
            if (!t_tok) return 0;
        } else {
            if (!t_tok || strcmp(p_tok, t_tok) != 0) return 0;
        }
        
        p_tok = strtok_r(NULL, "/", &p_save);
        t_tok = strtok_r(NULL, "/", &t_save);
    }
    
    return (t_tok == NULL);
}

/* ============================================================================
 * GESTIÓN DE TÓPICOS Y PARTICIONES
 * ============================================================================ */

static topic_t* find_topic_unlocked(const char *name) {
    for (int i = 0; i < num_topics; i++) {
        if (strcmp(topics[i].name, name) == 0) {
            return &topics[i];
        }
    }
    return NULL;
}

static void persist_message(partition_t *part, const message_t *msg) {
    FILE *fp = fopen(part->log_path, "a");
    if (fp) {
        fprintf(fp, "%ld|%ld|%d|%s|%s\n",
                msg->timestamp, msg->offset, msg->partition,
                msg->key, msg->payload);
        fclose(fp);
    }
}

static void load_partition_messages(partition_t *part, const char *topic_name) {
    FILE *fp = fopen(part->log_path, "r");
    if (!fp) return;
    
    char line[2048];
    while (fgets(line, sizeof(line), fp)) {
        if (part->count >= part->capacity) {
            int new_cap = part->capacity * 2;
            message_t *new_msgs = realloc(part->messages, new_cap * sizeof(message_t));
            if (!new_msgs) break;
            part->messages = new_msgs;
            part->capacity = new_cap;
        }
        
        line[strcspn(line, "\n")] = 0;
        
        message_t *msg = &part->messages[part->count];
        memset(msg, 0, sizeof(*msg));
        
        char *ptr = line;
        char *token;
        
        token = strsep(&ptr, "|");
        if (token) msg->timestamp = atol(token);
        
        token = strsep(&ptr, "|");
        if (token) msg->offset = atol(token);
        
        token = strsep(&ptr, "|");
        if (token) msg->partition = atoi(token);
        
        token = strsep(&ptr, "|");
        if (token) strncpy(msg->key, token, sizeof(msg->key) - 1);
        
        if (ptr) strncpy(msg->payload, ptr, sizeof(msg->payload) - 1);
        
        strncpy(msg->topic, topic_name, sizeof(msg->topic) - 1);
        
        part->count++;
        if (msg->offset >= part->next_offset) {
            part->next_offset = msg->offset + 1;
        }
    }
    
    fclose(fp);
    if (part->count > 0) {
        printf("[BROKER-%d] 📂 Loaded %d messages from %s\n", broker_id, part->count, part->log_path);
        fflush(stdout);
    }
}

static topic_t* create_topic(const char *name) {
    pthread_rwlock_wrlock(&topics_rwlock);
    
    topic_t *existing = find_topic_unlocked(name);
    if (existing) {
        pthread_rwlock_unlock(&topics_rwlock);
        return existing;
    }
    
    if (num_topics >= MAX_TOPICS) {
        pthread_rwlock_unlock(&topics_rwlock);
        printf("[BROKER-%d] ❌ Max topics reached\n", broker_id);
        fflush(stdout);
        return NULL;
    }
    
    topic_t *t = &topics[num_topics];
    memset(t, 0, sizeof(*t));
    strncpy(t->name, name, sizeof(t->name) - 1);
    t->num_partitions = NUM_PARTITIONS;
    t->round_robin_counter = 0;
    pthread_mutex_init(&t->rr_mutex, NULL);
    
    char safe_name[128];
    strncpy(safe_name, name, sizeof(safe_name) - 1);
    for (char *c = safe_name; *c; c++) {
        if (*c == '/') *c = '_';
    }
    
    for (int p = 0; p < NUM_PARTITIONS; p++) {
        partition_t *part = &t->partitions[p];
        memset(part, 0, sizeof(*part));
        part->next_offset = 0;
        part->count = 0;
        part->capacity = 100;
        part->messages = calloc(part->capacity, sizeof(message_t));
        pthread_mutex_init(&part->mutex, NULL);
        
        snprintf(part->log_path, sizeof(part->log_path), "%s/%.100s_p%d.log", 
                 data_dir, safe_name, p);
        
        load_partition_messages(part, name);
        
        printf("[BROKER-%d] 📋 Partition %s_p%d ready (off: %ld, msgs: %d)\n",
               broker_id, name, p, part->next_offset, part->count);
        fflush(stdout);
    }
    
    num_topics++;
    pthread_rwlock_unlock(&topics_rwlock);
    
    printf("[BROKER-%d] ✅ Topic created: %s (%d partitions)\n", 
           broker_id, name, NUM_PARTITIONS);
    fflush(stdout);
    
    return t;
}

static topic_t* get_or_create_topic(const char *name) {
    pthread_rwlock_rdlock(&topics_rwlock);
    topic_t *t = find_topic_unlocked(name);
    pthread_rwlock_unlock(&topics_rwlock);
    
    if (!t) {
        t = create_topic(name);
    }
    return t;
}

/* ============================================================================
 * MANEJADORES DE COMANDOS
 * ============================================================================ */

static void handle_produce(int client_idx, const char *topic_name, 
                          const char *key, const char *payload) {
    if (!topic_name || strlen(topic_name) == 0 || !payload) {
        return;
    }
    
    topic_t *topic = get_or_create_topic(topic_name);
    if (!topic) return;
    
    int partition_id = calculate_partition(topic, key);
    partition_t *partition = &topic->partitions[partition_id];
    
    pthread_mutex_lock(&partition->mutex);
    
    /* Expandir si es necesario */
    if (partition->count >= partition->capacity) {
        int new_cap = partition->capacity * 2;
        if (new_cap > MAX_MESSAGES) new_cap = MAX_MESSAGES;
        if (partition->count < new_cap) {
            message_t *new_msgs = realloc(partition->messages, new_cap * sizeof(message_t));
            if (new_msgs) {
                partition->messages = new_msgs;
                partition->capacity = new_cap;
            }
        }
    }
    
    message_t msg;
    memset(&msg, 0, sizeof(msg));
    strncpy(msg.topic, topic_name, sizeof(msg.topic) - 1);
    strncpy(msg.key, key ? key : "", sizeof(msg.key) - 1);
    strncpy(msg.payload, payload, sizeof(msg.payload) - 1);
    msg.timestamp = time(NULL);
    msg.offset = partition->next_offset;
    msg.partition = partition_id;
    
    if (partition->count < partition->capacity) {
        partition->messages[partition->count++] = msg;
    }
    partition->next_offset++;
    
    persist_message(partition, &msg);
    
    pthread_mutex_unlock(&partition->mutex);
    
    /* Enviar ACK */
    char response[256];
    snprintf(response, sizeof(response), "ACK|%s|%ld|%d\n", 
             topic_name, msg.offset, partition_id);
    
    pthread_mutex_lock(&clients_mutex);
    if (clients[client_idx].active) {
        safe_send(&clients[client_idx], response, strlen(response));
    }
    pthread_mutex_unlock(&clients_mutex);
    
    printf("[BROKER-%d] 📨 PRODUCE %s -> p%d (off=%ld)\n",
           broker_id, topic_name, partition_id, msg.offset);
    fflush(stdout);
    
    /* Push a consumers */
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].type == 1) {
            for (int j = 0; j < clients[i].num_subs; j++) {
                if (wildcard_match(clients[i].subscriptions[j], topic_name)) {
                    char push_msg[2048];
                    snprintf(push_msg, sizeof(push_msg),
                            "MESSAGE|%s|%ld|%d|%s|%s\n",
                            topic_name, msg.offset, partition_id, 
                            msg.key, msg.payload);
                    safe_send(&clients[i], push_msg, strlen(push_msg));
                    break;
                }
            }
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

static void handle_subscribe(int idx, const char *pattern) {
    pthread_mutex_lock(&clients_mutex);
    
    if (clients[idx].num_subs < MAX_SUBSCRIPTIONS) {
        strncpy(clients[idx].subscriptions[clients[idx].num_subs], pattern, 255);
        clients[idx].num_subs++;
        
        printf("[BROKER-%d] 🔔 %s subscribed to: %s\n", 
               broker_id, clients[idx].id, pattern);
        fflush(stdout);
        
        char response[256];
        snprintf(response, sizeof(response), "SUBSCRIBED|%s\n", pattern);
        safe_send(&clients[idx], response, strlen(response));
    }
    
    pthread_mutex_unlock(&clients_mutex);
}

static void handle_fetch(int client_idx, const char *topic_pattern, long offset) {
    int total_sent = 0;
    char response[2048];
    
    pthread_rwlock_rdlock(&topics_rwlock);
    
    for (int t = 0; t < num_topics; t++) {
        if (wildcard_match(topic_pattern, topics[t].name)) {
            for (int p = 0; p < topics[t].num_partitions; p++) {
                partition_t *part = &topics[t].partitions[p];
                
                pthread_mutex_lock(&part->mutex);
                
                for (int m = 0; m < part->count; m++) {
                    message_t *msg = &part->messages[m];
                    if (msg->offset >= offset) {
                        snprintf(response, sizeof(response),
                                "MESSAGE|%s|%ld|%d|%s|%s\n",
                                msg->topic, msg->offset, msg->partition,
                                msg->key, msg->payload);
                        
                        pthread_mutex_lock(&clients_mutex);
                        if (clients[client_idx].active) {
                            safe_send(&clients[client_idx], response, strlen(response));
                            total_sent++;
                        }
                        pthread_mutex_unlock(&clients_mutex);
                    }
                }
                
                pthread_mutex_unlock(&part->mutex);
            }
        }
    }
    
    pthread_rwlock_unlock(&topics_rwlock);
    
    snprintf(response, sizeof(response), "FETCH_END|%d\n", total_sent);
    pthread_mutex_lock(&clients_mutex);
    if (clients[client_idx].active) {
        safe_send(&clients[client_idx], response, strlen(response));
    }
    pthread_mutex_unlock(&clients_mutex);
    
    printf("[BROKER-%d] 📥 FETCH %s (off>=%ld) -> %d\n",
           broker_id, topic_pattern, offset, total_sent);
    fflush(stdout);
}

/* ============================================================================
 * MANEJO DE CLIENTES
 * ============================================================================ */

static void* handle_client(void *arg) {
    int *args = (int*)arg;
    int socket = args[0];
    int idx = args[1];
    free(arg);
    
    char buffer[BUFFER_SIZE];
    char line_buffer[BUFFER_SIZE * 2];
    int line_pos = 0;
    
    printf("[BROKER-%d] 🔗 Client connected (slot %d)\n", broker_id, idx);
    fflush(stdout);
    
    while (running) {
        int bytes = recv(socket, buffer, BUFFER_SIZE - 1, 0);
        if (bytes <= 0) break;
        
        buffer[bytes] = '\0';
        
        for (int i = 0; i < bytes; i++) {
            if (buffer[i] == '\n') {
                line_buffer[line_pos] = '\0';
                
                if (line_pos > 0) {
                    char *parts[6] = {NULL};
                    int part_count = 0;
                    char *start = line_buffer;
                    
                    for (char *pos = line_buffer; *pos && part_count < 6; pos++) {
                        if (*pos == '|') {
                            *pos = '\0';
                            parts[part_count++] = start;
                            start = pos + 1;
                        }
                    }
                    if (part_count < 6 && *start) {
                        parts[part_count++] = start;
                    }
                    
                    if (part_count >= 1) {
                        char *cmd = parts[0];
                        
                        if (strcmp(cmd, "REGISTER_PRODUCER") == 0 && part_count >= 2) {
                            pthread_mutex_lock(&clients_mutex);
                            clients[idx].type = 0;
                            strncpy(clients[idx].id, parts[1], 63);
                            pthread_mutex_unlock(&clients_mutex);
                            
                            send(socket, "OK|REGISTERED\n", 14, 0);
                            printf("[BROKER-%d] 🏭 Producer: %s\n", broker_id, parts[1]);
                            fflush(stdout);
                            
                        } else if (strcmp(cmd, "REGISTER_CONSUMER") == 0 && part_count >= 2) {
                            pthread_mutex_lock(&clients_mutex);
                            clients[idx].type = 1;
                            strncpy(clients[idx].id, parts[1], 63);
                            clients[idx].persistent = (part_count >= 3) ? atoi(parts[2]) : 0;
                            pthread_mutex_unlock(&clients_mutex);
                            
                            send(socket, "OK|REGISTERED\n", 14, 0);
                            printf("[BROKER-%d] 👁️ Consumer: %s\n", broker_id, parts[1]);
                            fflush(stdout);
                            
                        } else if (strcmp(cmd, "PRODUCE") == 0 && part_count >= 4) {
                            handle_produce(idx, parts[1], parts[2], parts[3]);
                            
                        } else if (strcmp(cmd, "SUBSCRIBE") == 0 && part_count >= 2) {
                            handle_subscribe(idx, parts[1]);
                            
                        } else if (strcmp(cmd, "FETCH") == 0 && part_count >= 3) {
                            handle_fetch(idx, parts[1], atol(parts[2]));
                            
                        } else if (strcmp(cmd, "PING") == 0) {
                            send(socket, "PONG\n", 5, 0);
                        }
                    }
                }
                
                line_pos = 0;
            } else if (line_pos < (int)sizeof(line_buffer) - 1) {
                line_buffer[line_pos++] = buffer[i];
            }
        }
    }
    
    pthread_mutex_lock(&clients_mutex);
    printf("[BROKER-%d] 🔌 %s disconnected\n", broker_id, clients[idx].id);
    fflush(stdout);
    clients[idx].active = 0;
    clients[idx].socket = -1;
    pthread_mutex_unlock(&clients_mutex);
    
    close(socket);
    sem_post(&connection_semaphore);
    
    return NULL;
}

/* ============================================================================
 * SEÑALES
 * ============================================================================ */

static void signal_handler(int sig) {
    (void)sig;
    printf("\n[BROKER-%d] 🛑 Shutting down...\n", broker_id);
    fflush(stdout);
    running = 0;
}

/* ============================================================================
 * MAIN
 * ============================================================================ */

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    
    char *env_id = getenv("BROKER_ID");
    if (env_id) {
        broker_id = atoi(env_id);
    } else if (argc >= 2) {
        broker_id = atoi(argv[1]);
    }
    
    int port = BROKER_PORT + broker_id - 1;
    
    printf("\n");
    printf("╔═══════════════════════════════════════════╗\n");
    printf("║       AKLight v2 - Broker %d               ║\n", broker_id);
    printf("╠═══════════════════════════════════════════╣\n");
    printf("║  Port: %-35d ║\n", port);
    printf("║  Partitions: %-29d ║\n", NUM_PARTITIONS);
    printf("╚═══════════════════════════════════════════╝\n\n");
    fflush(stdout);
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);
    
    init_data_dir();
    init_topics_array();
    
    memset(clients, 0, sizeof(clients));
    for (int i = 0; i < MAX_CLIENTS; i++) {
        pthread_mutex_init(&clients[i].send_mutex, NULL);
        clients[i].socket = -1;
    }
    
    sem_init(&connection_semaphore, 0, MAX_CLIENTS);
    
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        perror("socket");
        return 1;
    }
    
    int opt = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    
    if (bind(server_socket, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    
    if (listen(server_socket, 10) < 0) {
        perror("listen");
        return 1;
    }
    
    printf("[BROKER-%d] ✅ Listening on port %d\n\n", broker_id, port);
    fflush(stdout);
    
    while (running) {
        struct sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        
        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &len);
        if (client_socket < 0) {
            if (running && errno != EINTR) perror("accept");
            continue;
        }
        
        if (sem_trywait(&connection_semaphore) != 0) {
            close(client_socket);
            continue;
        }
        
        int idx = -1;
        pthread_mutex_lock(&clients_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active) {
                idx = i;
                clients[i].active = 1;
                clients[i].socket = client_socket;
                clients[i].num_subs = 0;
                clients[i].id[0] = '\0';
                break;
            }
        }
        pthread_mutex_unlock(&clients_mutex);
        
        if (idx < 0) {
            close(client_socket);
            sem_post(&connection_semaphore);
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
            pthread_mutex_lock(&clients_mutex);
            clients[idx].active = 0;
            clients[idx].socket = -1;
            pthread_mutex_unlock(&clients_mutex);
            close(client_socket);
            sem_post(&connection_semaphore);
        }
    }
    
    close(server_socket);
    sem_destroy(&connection_semaphore);
    pthread_rwlock_destroy(&topics_rwlock);
    
    if (topics) free(topics);
    
    printf("[BROKER-%d] 👋 Goodbye!\n", broker_id);
    fflush(stdout);
    return 0;
}
