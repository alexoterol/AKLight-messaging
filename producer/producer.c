/**
 * AKLight Producer - Productor de métricas Docker
 * 
 * Envía métricas del contenedor Docker:
 * - Uso de CPU
 * - Uso de memoria
 * - Uso de disco
 * - Latencia de red
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <signal.h>
#include <stdarg.h>


// ============================================
// CONFIGURACIÓN
// ============================================

#define BROKER_HOST "broker1"  // Nombre del servicio Docker
#define BROKER_PORT 7000
#define BUFFER_SIZE 8192
#define METRICS_INTERVAL 5  // Segundos entre métricas

// ============================================
// VARIABLES GLOBALES
// ============================================

static int broker_socket = -1;
static volatile int running = 1;
static char producer_id[64] = "producer1";
static char base_topic[256] = "metrics/docker";

// ============================================
// UTILIDADES
// ============================================

static void log_message(const char *level, const char *format, ...) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char timestamp[32];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", t);
    
    printf("[%s] [%s] [%s] ", timestamp, producer_id, level);
    
    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    
    printf("\n");
    fflush(stdout);
}

// ============================================
// RECOLECCIÓN DE MÉTRICAS DOCKER
// ============================================

typedef struct {
    double cpu_usage_percent;
    long memory_usage_mb;
    long memory_limit_mb;
    double memory_percent;
    long disk_usage_mb;
    long network_rx_bytes;
    long network_tx_bytes;
    double latency_ms;
} docker_metrics_t;

static double read_cpu_usage(void) {
    FILE *f = fopen("/sys/fs/cgroup/cpu/cpuacct.usage", "r");
    if (!f) {
        // Intentar cgroup v2
        f = fopen("/sys/fs/cgroup/cpu.stat", "r");
        if (!f) return 0.0;
    }
    
    static long prev_usage = 0;
    static time_t prev_time = 0;
    
    long usage;
    fscanf(f, "%ld", &usage);
    fclose(f);
    
    time_t now = time(NULL);
    
    double percent = 0.0;
    if (prev_time > 0) {
        long cpu_delta = usage - prev_usage;
        long time_delta = (now - prev_time) * 1000000000;  // nanoseconds
        
        if (time_delta > 0) {
            percent = (double)cpu_delta / time_delta * 100.0;
        }
    }
    
    prev_usage = usage;
    prev_time = now;
    
    return percent;
}

static void read_memory_usage(long *usage_mb, long *limit_mb, double *percent) {
    FILE *f = fopen("/sys/fs/cgroup/memory/memory.usage_in_bytes", "r");
    if (!f) {
        // Intentar cgroup v2
        f = fopen("/sys/fs/cgroup/memory.current", "r");
        if (!f) {
            *usage_mb = 0;
            *limit_mb = 0;
            *percent = 0.0;
            return;
        }
    }
    
    long usage;
    fscanf(f, "%ld", &usage);
    fclose(f);
    
    f = fopen("/sys/fs/cgroup/memory/memory.limit_in_bytes", "r");
    if (!f) {
        f = fopen("/sys/fs/cgroup/memory.max", "r");
        if (!f) {
            *usage_mb = usage / (1024 * 1024);
            *limit_mb = 0;
            *percent = 0.0;
            return;
        }
    }
    
    long limit;
    fscanf(f, "%ld", &limit);
    fclose(f);
    
    *usage_mb = usage / (1024 * 1024);
    *limit_mb = limit / (1024 * 1024);
    *percent = (double)usage / limit * 100.0;
}

static long read_disk_usage(void) {
    FILE *f = fopen("/proc/diskstats", "r");
    if (!f) return 0;
    
    char line[512];
    long total_kb = 0;
    
    while (fgets(line, sizeof(line), f)) {
        int major, minor;
        char device[64];
        long reads, writes, sectors_read, sectors_written;
        
        if (sscanf(line, "%d %d %s %*d %*d %ld %*d %*d %*d %ld", 
                  &major, &minor, device, &sectors_read, &sectors_written) == 5) {
            // Cada sector = 512 bytes típicamente
            total_kb += (sectors_read + sectors_written) / 2;
        }
    }
    
    fclose(f);
    return total_kb / 1024;  // MB
}

static void read_network_stats(long *rx_bytes, long *tx_bytes) {
    FILE *f = fopen("/proc/net/dev", "r");
    if (!f) {
        *rx_bytes = 0;
        *tx_bytes = 0;
        return;
    }
    
    char line[512];
    *rx_bytes = 0;
    *tx_bytes = 0;
    
    // Saltar las dos primeras líneas (headers)
    fgets(line, sizeof(line), f);
    fgets(line, sizeof(line), f);
    
    while (fgets(line, sizeof(line), f)) {
        char iface[64];
        long rx, tx;
        
        if (sscanf(line, "%[^:]: %ld %*d %*d %*d %*d %*d %*d %*d %ld", 
                  iface, &rx, &tx) == 3) {
            // Sumar todas las interfaces excepto lo
            if (strcmp(iface, "lo") != 0) {
                *rx_bytes += rx;
                *tx_bytes += tx;
            }
        }
    }
    
    fclose(f);
}

static double measure_latency_ms(void) {
    // Medir latencia de red simple (ping al broker)
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    
    // Intentar conectar (si ya está conectado, será rápido)
    if (broker_socket >= 0) {
        char ping_msg[] = "PING\n";
        send(broker_socket, ping_msg, strlen(ping_msg), MSG_DONTWAIT);
    }
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    double latency = (end.tv_sec - start.tv_sec) * 1000.0 +
                     (end.tv_nsec - start.tv_nsec) / 1000000.0;
    
    return latency;
}

static void collect_metrics(docker_metrics_t *metrics) {
    metrics->cpu_usage_percent = read_cpu_usage();
    read_memory_usage(&metrics->memory_usage_mb, &metrics->memory_limit_mb, 
                     &metrics->memory_percent);
    metrics->disk_usage_mb = read_disk_usage();
    read_network_stats(&metrics->network_rx_bytes, &metrics->network_tx_bytes);
    metrics->latency_ms = measure_latency_ms();
}

// ============================================
// COMUNICACIÓN CON BROKER
// ============================================

static int connect_to_broker(void) {
    struct sockaddr_in broker_addr;
    struct hostent *host;
    
    log_message("INFO", "Conectando a broker: %s:%d", BROKER_HOST, BROKER_PORT);
    
    host = gethostbyname(BROKER_HOST);
    if (host == NULL) {
        log_message("ERROR", "No se pudo resolver hostname: %s", BROKER_HOST);
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
    
    // Registrarse como productor
    char reg_msg[256];
    snprintf(reg_msg, sizeof(reg_msg), "REGISTER_PRODUCER|%s\n", producer_id);
    send(broker_socket, reg_msg, strlen(reg_msg), 0);
    
    return 0;
}

static int send_metric(const char *metric_name, const char *value, const char *key) {
    if (broker_socket < 0) {
        if (connect_to_broker() != 0) {
            return -1;
        }
    }
    
    // Crear tópico: metrics/docker/producer_id/metric_name
    char topic[512];
    snprintf(topic, sizeof(topic), "%s/%s/%s", base_topic, producer_id, metric_name);
    
    // Crear mensaje: PRODUCE|topic|key|payload
    char message[BUFFER_SIZE];
    snprintf(message, sizeof(message), "PRODUCE|%s|%s|%s\n", 
            topic, key ? key : "", value);
    
    ssize_t sent = send(broker_socket, message, strlen(message), 0);
    if (sent <= 0) {
        log_message("ERROR", "Error enviando mensaje");
        close(broker_socket);
        broker_socket = -1;
        return -1;
    }
    
    // Esperar ACK (opcional)
    char ack[256];
    recv(broker_socket, ack, sizeof(ack) - 1, MSG_DONTWAIT);
    
    return 0;
}

// ============================================
// PUBLICACIÓN DE MÉTRICAS
// ============================================

static void publish_metrics(const docker_metrics_t *metrics) {
    char value[256];
    char timestamp_key[64];
    
    // Usar timestamp como clave para particionar por tiempo
    snprintf(timestamp_key, sizeof(timestamp_key), "%ld", time(NULL));
    
    // CPU
    snprintf(value, sizeof(value), "{\"cpu_percent\":%.2f,\"timestamp\":%ld}", 
            metrics->cpu_usage_percent, time(NULL));
    send_metric("cpu", value, timestamp_key);
    log_message("INFO", "CPU: %.2f%%", metrics->cpu_usage_percent);
    
    // Memoria
    snprintf(value, sizeof(value), 
            "{\"usage_mb\":%ld,\"limit_mb\":%ld,\"percent\":%.2f,\"timestamp\":%ld}", 
            metrics->memory_usage_mb, metrics->memory_limit_mb, 
            metrics->memory_percent, time(NULL));
    send_metric("memory", value, timestamp_key);
    log_message("INFO", "Memoria: %ld MB / %ld MB (%.2f%%)", 
               metrics->memory_usage_mb, metrics->memory_limit_mb, 
               metrics->memory_percent);
    
    // Disco
    snprintf(value, sizeof(value), "{\"usage_mb\":%ld,\"timestamp\":%ld}", 
            metrics->disk_usage_mb, time(NULL));
    send_metric("disk", value, timestamp_key);
    log_message("INFO", "Disco: %ld MB", metrics->disk_usage_mb);
    
    // Red
    snprintf(value, sizeof(value), 
            "{\"rx_bytes\":%ld,\"tx_bytes\":%ld,\"timestamp\":%ld}", 
            metrics->network_rx_bytes, metrics->network_tx_bytes, time(NULL));
    send_metric("network", value, timestamp_key);
    log_message("INFO", "Red: RX=%ld bytes, TX=%ld bytes", 
               metrics->network_rx_bytes, metrics->network_tx_bytes);
    
    // Latencia
    snprintf(value, sizeof(value), "{\"latency_ms\":%.2f,\"timestamp\":%ld}", 
            metrics->latency_ms, time(NULL));
    send_metric("latency", value, timestamp_key);
    log_message("INFO", "Latencia: %.2f ms", metrics->latency_ms);
}

// ============================================
// SIGNAL HANDLER
// ============================================

static void signal_handler(int signum) {
    log_message("INFO", "Señal recibida, cerrando productor...");
    running = 0;
}

// ============================================
// FUNCIÓN PRINCIPAL
// ============================================

int main(int argc, char *argv[]) {
    if (argc >= 2) {
        strncpy(producer_id, argv[1], sizeof(producer_id) - 1);
    }
    
    if (argc >= 3) {
        strncpy(base_topic, argv[2], sizeof(base_topic) - 1);
    }
    
    printf("\n");
    printf("============================================================\n");
    printf("AKLight Producer - %s\n", producer_id);
    printf("Enviando métricas Docker al broker\n");
    printf("============================================================\n\n");
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Conectar al broker
    if (connect_to_broker() != 0) {
        log_message("ERROR", "No se pudo conectar al broker inicialmente");
        log_message("INFO", "Reintentaré en cada ciclo de métricas");
    }
    
    docker_metrics_t metrics;
    
    log_message("INFO", "Iniciando recolección de métricas (intervalo: %d segundos)", 
               METRICS_INTERVAL);
    log_message("INFO", "Tópico base: %s", base_topic);
    
    // Loop principal
    while (running) {
        log_message("INFO", "\n========== Recolectando métricas ==========");
        
        // Recolectar métricas
        collect_metrics(&metrics);
        
        // Publicar métricas
        publish_metrics(&metrics);
        
        log_message("INFO", "==========================================\n");
        
        // Esperar intervalo
        sleep(METRICS_INTERVAL);
    }
    
    if (broker_socket >= 0) {
        close(broker_socket);
    }
    
    log_message("INFO", "Productor cerrado");
    
    return 0;
}

/**
 * COMPILACIÓN:
 * gcc -o producer producer.c -pthread -Wall -Wextra
 * 
 * EJECUCIÓN:
 * ./producer producer1 metrics/docker
 * ./producer producer2 metrics/docker
 */
