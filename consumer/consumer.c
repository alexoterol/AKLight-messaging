/**
 * AKLight Consumer Simplificado
 * Uso: ./consumer [id] [persistent] [broker_host] [broker_port] [topic1] [topic2] ...
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
    if (!host) return -1;
    
    broker_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (broker_socket < 0) return -1;
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    memcpy(&addr.sin_addr, host->h_addr, host->h_length);
    addr.sin_port = htons(broker_port);
    
    if (connect(broker_socket, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        close(broker_socket);
        broker_socket = -1;
        return -1;
    }
    
    printf("[%s] Connected to %s:%d\n", consumer_id, broker_host, broker_port);
    
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
}

static void* receive_thread(void *arg) {
    char buffer[BUFFER_SIZE];
    
    while (running && broker_socket >= 0) {
        int bytes = recv(broker_socket, buffer, BUFFER_SIZE - 1, 0);
        if (bytes <= 0) {
            running = 0;
            break;
        }
        
        buffer[bytes] = '\0';
        char *line = strtok(buffer, "\n");
        
        while (line) {
            if (strncmp(line, "MESSAGE|", 8) == 0) {
                msg_count++;
                char topic[256], key[128], payload[2048];
                long offset;
                
                if (sscanf(line, "MESSAGE|%255[^|]|%ld|%127[^|]|%2047[^\n]", 
                          topic, &offset, key, payload) == 4) {
                    printf("\n[MSG #%u] %s [%ld] %s\n", msg_count, topic, offset, payload);
                }
            }
            line = strtok(NULL, "\n");
        }
    }
    return NULL;
}

static void signal_handler(int sig) {
    running = 0;
}

int main(int argc, char *argv[]) {
    // Argumentos: consumer_id persistent broker_host broker_port topic1 topic2 ...
    if (argc < 2) {
        printf("Uso: %s [id] [persistent] [broker_host] [broker_port] [topic1] ...\n", argv[0]);
        printf("Ejemplo: %s consumer1 1 broker1 7000 metrics/docker/#\n", argv[0]);
        return 1;
    }
    
    int arg_idx = 1;
    
    if (argc > arg_idx) strncpy(consumer_id, argv[arg_idx++], 63);
    if (argc > arg_idx) arg_idx++;  // skip persistent flag
    if (argc > arg_idx) strncpy(broker_host, argv[arg_idx++], 127);
    if (argc > arg_idx) broker_port = atoi(argv[arg_idx++]);
    
    // Recoger topics
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
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    if (connect_to_broker() != 0) {
        printf("Error connecting to broker\n");
        return 1;
    }
    
    for (int i = 0; i < num_topics; i++) {
        subscribe_topic(topics[i]);
    }
    
    pthread_t tid;
    pthread_create(&tid, NULL, receive_thread, NULL);
    pthread_join(tid, NULL);
    
    if (broker_socket >= 0) close(broker_socket);
    
    return 0;
}