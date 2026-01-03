/**
 * AKLight Consumer con FETCH Manual
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>

#define BUFFER_SIZE 4096

static int broker_socket = -1;
static volatile int running = 1;
static char consumer_id[64] = "consumer1";
static char broker_host[128] = "broker1";
static int broker_port = 7000;
static char topics[10][256];
static int num_topics = 0;
static unsigned int msg_count = 0;

static int connect_to_broker(void) {
    struct hostent *host = gethostbyname(broker_host);
    if (!host) {
        printf("[%s] ERROR: Cannot resolve %s\n", consumer_id, broker_host);
        fflush(stdout);
        return -1;
    }
    
    broker_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (broker_socket < 0) {
        printf("[%s] ERROR: Cannot create socket\n", consumer_id);
        fflush(stdout);
        return -1;
    }
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    memcpy(&addr.sin_addr, host->h_addr, host->h_length);
    addr.sin_port = htons(broker_port);
    
    if (connect(broker_socket, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        printf("[%s] ERROR: Cannot connect to %s:%d\n", consumer_id, broker_host, broker_port);
        fflush(stdout);
        close(broker_socket);
        broker_socket = -1;
        return -1;
    }
    
    printf("[%s] Connected to %s:%d\n", consumer_id, broker_host, broker_port);
    fflush(stdout);
    
    char reg[256];
    snprintf(reg, sizeof(reg), "REGISTER_CONSUMER|%s|1\n", consumer_id);
    send(broker_socket, reg, strlen(reg), 0);
    
    return 0;
}

static void subscribe_topic(const char *topic) {
    if (broker_socket < 0) return;
    
    char sub[512];
    snprintf(sub, sizeof(sub), "SUBSCRIBE|%s\n", topic);
    send(broker_socket, sub, strlen(sub), 0);
    printf("[%s] Subscribed to: %s\n", consumer_id, topic);
    fflush(stdout);
}

static void fetch_messages(const char *topic, long offset) {
    if (broker_socket < 0) return;
    
    char fetch_msg[512];
    snprintf(fetch_msg, sizeof(fetch_msg), "FETCH|%s|%ld\n", topic, offset);
    send(broker_socket, fetch_msg, strlen(fetch_msg), 0);
    printf("[%s] 📥 Fetching messages from %s (offset >= %ld)\n", consumer_id, topic, offset);
    fflush(stdout);
}

static void* receive_thread(void *arg) {
    char buffer[BUFFER_SIZE];
    
    printf("[%s] Receive thread started\n", consumer_id);
    fflush(stdout);
    
    while (running && broker_socket >= 0) {
        int bytes = recv(broker_socket, buffer, BUFFER_SIZE - 1, 0);
        if (bytes <= 0) {
            printf("[%s] Connection lost\n", consumer_id);
            fflush(stdout);
            running = 0;
            break;
        }
        
        buffer[bytes] = '\0';
        char *line = strtok(buffer, "\n");
        
        while (line) {
            if (strncmp(line, "MESSAGE|", 8) == 0) {
                msg_count++;
                
                char *parts[5] = {NULL, NULL, NULL, NULL, NULL};
                int part_count = 0;
                char *start = line;
                char *pos = line;
                
                while (*pos && part_count < 5) {
                    if (*pos == '|') {
                        *pos = '\0';
                        parts[part_count++] = start;
                        start = pos + 1;
                    }
                    pos++;
                }
                if (part_count < 5 && *start) {
                    parts[part_count++] = start;
                }
                
                if (part_count >= 5) {
                    char *topic = parts[1];
                    long offset = atol(parts[2]);
                    char *key = parts[3] && strlen(parts[3]) > 0 ? parts[3] : "";
                    char *payload = parts[4];
                    
                    printf("\n========================================\n");
                    printf("[MSG #%u] %s\n", msg_count, topic);
                    printf("Offset: %ld\n", offset);
                    printf("Key: %s\n", key);
                    printf("Payload: %s\n", payload);
                    printf("========================================\n");
                    fflush(stdout);
                }
                
            } else if (strncmp(line, "SUBSCRIBED|", 11) == 0) {
                printf("[%s] ✓ Subscription confirmed: %s\n", consumer_id, line + 11);
                fflush(stdout);
                
            } else if (strncmp(line, "FETCH_END|", 10) == 0) {
                int count = atoi(line + 10);
                printf("[%s] ✓ Fetch completed: %d messages retrieved\n", consumer_id, count);
                fflush(stdout);
            }
            
            line = strtok(NULL, "\n");
        }
    }
    
    printf("[%s] Receive thread ended\n", consumer_id);
    fflush(stdout);
    return NULL;
}

static void signal_handler(int sig) {
    running = 0;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Uso: %s [id] [persistent] [broker_host] [broker_port] [topic1] ...\n", argv[0]);
        return 1;
    }
    
    int arg_idx = 1;
    
    if (argc > arg_idx) strncpy(consumer_id, argv[arg_idx++], 63);
    if (argc > arg_idx) arg_idx++;
    if (argc > arg_idx) strncpy(broker_host, argv[arg_idx++], 127);
    if (argc > arg_idx) broker_port = atoi(argv[arg_idx++]);
    
    while (arg_idx < argc && num_topics < 10) {
        strncpy(topics[num_topics++], argv[arg_idx++], 255);
    }
    
    if (num_topics == 0) {
        strncpy(topics[0], "metrics/docker/#", 255);
        num_topics = 1;
    }
    
    printf("\n=== AKLight Consumer: %s ===\n", consumer_id);
    printf("Broker: %s:%d\n", broker_host, broker_port);
    for (int i = 0; i < num_topics; i++) {
        printf("Topic: %s\n", topics[i]);
    }
    printf("\n");
    fflush(stdout);
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    if (connect_to_broker() != 0) {
        printf("Error connecting to broker\n");
        return 1;
    }
    
    // Iniciar thread de recepción
    pthread_t tid;
    pthread_create(&tid, NULL, receive_thread, NULL);
    
    // Suscribirse
    sleep(1);
    for (int i = 0; i < num_topics; i++) {
        subscribe_topic(topics[i]);
    }
    
    // FETCH MANUAL de mensajes históricos
    sleep(1);
    printf("\n[%s] 🔍 Fetching historical messages...\n", consumer_id);
    fflush(stdout);
    
    for (int i = 0; i < num_topics; i++) {
        char topic_base[256];
        strncpy(topic_base, topics[i], sizeof(topic_base) - 1);
        
        // Si tiene wildcard, hacer fetch de topics conocidos
        if (strstr(topic_base, "/#")) {
            char *wildcard = strstr(topic_base, "/#");
            if (wildcard) *wildcard = '\0';
            
            char specific_topic[256];
            snprintf(specific_topic, sizeof(specific_topic), "%s/cpu", topic_base);
            fetch_messages(specific_topic, 0);
            sleep(1);
            
            snprintf(specific_topic, sizeof(specific_topic), "%s/memory", topic_base);
            fetch_messages(specific_topic, 0);
            sleep(1);
            
            snprintf(specific_topic, sizeof(specific_topic), "%s/disk", topic_base);
            fetch_messages(specific_topic, 0);
            sleep(1);
            
            snprintf(specific_topic, sizeof(specific_topic), "%s/network", topic_base);
            fetch_messages(specific_topic, 0);
            sleep(1);
        } else {
            fetch_messages(topics[i], 0);
            sleep(1);
        }
    }
    
    pthread_join(tid, NULL);
    
    if (broker_socket >= 0) close(broker_socket);
    
    return 0;
}