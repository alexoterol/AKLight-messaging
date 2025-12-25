/**
 * AKLight Consumer - Consumidor con sesiones persistentes/no persistentes
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
#include <time.h>
#include <stdarg.h>


#define BROKER_HOST "broker1"
#define BROKER_PORT 7000
#define BUFFER_SIZE 8192

static int broker_socket = -1;
static volatile int running = 1;
static char consumer_id[64] = "consumer1";
static int persistent_session = 1;  // 1=persistente, 0=no persistente

static void log_message(const char *level, const char *format, ...) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char timestamp[32];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", t);
    
    printf("[%s] [%s] [%s] ", timestamp, consumer_id, level);
    
    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    
    printf("\n");
    fflush(stdout);
}

static int connect_to_broker(void) {
    struct sockaddr_in broker_addr;
    struct hostent *host;
    
    log_message("INFO", "Conectando a broker: %s:%d", BROKER_HOST, BROKER_PORT);
    
    host = gethostbyname(BROKER_HOST);
    if (host == NULL) {
        log_message("ERROR", "No se pudo resolver hostname");
        return -1;
    }
    
    broker_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (broker_socket < 0) {
        log_message("ERROR", "No se pudo crear socket");
        return -1;
    }
    
    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    memcpy(&broker_addr.sin_addr, host->h_addr, host->h_length);
    broker_addr.sin_port = htons(BROKER_PORT);
    
    if (connect(broker_socket, (struct sockaddr*)&broker_addr, 
               sizeof(broker_addr)) != 0) {
        log_message("ERROR", "Error conectando al broker");
        close(broker_socket);
        broker_socket = -1;
        return -1;
    }
    
    log_message("INFO", "✓ Conectado al broker");
    
    // Registrarse como consumidor
    char reg_msg[256];
    snprintf(reg_msg, sizeof(reg_msg), "REGISTER_CONSUMER|%s|%d\n", 
            consumer_id, persistent_session);
    send(broker_socket, reg_msg, strlen(reg_msg), 0);
    
    return 0;
}

static void subscribe_topic(const char *topic_pattern) {
    if (broker_socket < 0) return;
    
    char sub_msg[512];
    snprintf(sub_msg, sizeof(sub_msg), "SUBSCRIBE|%s\n", topic_pattern);
    send(broker_socket, sub_msg, strlen(sub_msg), 0);
    
    log_message("INFO", "Suscrito a: %s", topic_pattern);
}

static void* receive_thread(void *arg) {
    char buffer[BUFFER_SIZE];
    unsigned int message_count = 0;
    
    while (running && broker_socket >= 0) {
        int bytes = recv(broker_socket, buffer, BUFFER_SIZE - 1, 0);
        
        if (bytes <= 0) {
            log_message("ERROR", "Conexión perdida con broker");
            running = 0;
            break;
        }
        
        buffer[bytes] = '\0';
        
        char *line = strtok(buffer, "\n");
        while (line) {
            if (strncmp(line, "MESSAGE|", 8) == 0) {
                message_count++;
                
                char topic[256], key[128], payload[4096];
                int partition;
                long offset;
                
                if (sscanf(line, "MESSAGE|%255[^|]|%d|%ld|%127[^|]|%4095s", 
                          topic, &partition, &offset, key, payload) == 5) {
                    
                    printf("\n");
                    printf("============================================================\n");
                    printf("[MENSAJE #%u]\n", message_count);
                    printf("  Topic:     %s\n", topic);
                    printf("  Partition: %d\n", partition);
                    printf("  Offset:    %ld\n", offset);
                    printf("  Key:       %s\n", key);
                    printf("  Payload:   %s\n", payload);
                    printf("============================================================\n");
                    fflush(stdout);
                }
            } else if (strncmp(line, "SUBSCRIBED|", 11) == 0) {
                log_message("INFO", "✓ Confirmación de suscripción: %s", line + 11);
            }
            
            line = strtok(NULL, "\n");
        }
    }
    
    return NULL;
}

static void signal_handler(int signum) {
    log_message("INFO", "Señal recibida, cerrando consumidor...");
    running = 0;
}

int main(int argc, char *argv[]) {
    if (argc >= 2) {
        strncpy(consumer_id, argv[1], sizeof(consumer_id) - 1);
    }
    
    if (argc >= 3) {
        persistent_session = atoi(argv[2]);
    }
    
    printf("\n");
    printf("============================================================\n");
    printf("AKLight Consumer - %s\n", consumer_id);
    printf("Sesión: %s\n", persistent_session ? "PERSISTENTE" : "NO PERSISTENTE");
    printf("============================================================\n\n");
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    if (connect_to_broker() != 0) {
        log_message("ERROR", "No se pudo conectar al broker");
        return 1;
    }
    
    // Suscribirse a tópicos con wildcard
    subscribe_topic("metrics/docker/#");
    
    log_message("INFO", "Esperando mensajes... (Ctrl+C para salir)\n");
    
    // Crear thread de recepción
    pthread_t recv_tid;
    pthread_create(&recv_tid, NULL, receive_thread, NULL);
    
    // Esperar
    pthread_join(recv_tid, NULL);
    
    if (broker_socket >= 0) {
        close(broker_socket);
    }
    
    log_message("INFO", "Consumidor cerrado");
    
    return 0;
}

/**
 * COMPILACIÓN:
 * gcc -o consumer consumer.c -pthread -Wall -Wextra
 * 
 * EJECUCIÓN:
 * ./consumer consumer1 1  # Sesión persistente
 * ./consumer consumer1 0  # Sesión no persistente
 */
