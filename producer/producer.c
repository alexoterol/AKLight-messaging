/**
 * AKLight Producer Simplificado
 * Uso: ./producer [id] [base_topic] [broker_host] [broker_port]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <time.h>
#include <signal.h>

#define BUFFER_SIZE 4096
#define INTERVAL 5

static int broker_socket = -1;
static volatile int running = 1;
static char producer_id[64] = "producer1";
static char base_topic[256] = "metrics/docker";
static char broker_host[128] = "broker1";
static int broker_port = 7000;

typedef struct {
    double cpu;
    long mem_mb;
    long disk_mb;
    long net_rx;
    long net_tx;
} metrics_t;

static double get_cpu(void) {
    static long prev = 0;
    static time_t prev_time = 0;
    
    FILE *f = fopen("/sys/fs/cgroup/cpu/cpuacct.usage", "r");
    if (!f) f = fopen("/sys/fs/cgroup/cpu.stat", "r");
    if (!f) return (rand() % 100) / 10.0;  // Fallback: valores aleatorios
    
    long usage;
    fscanf(f, "%ld", &usage);
    fclose(f);
    
    time_t now = time(NULL);
    double percent = 0.0;
    
    if (prev_time > 0) {
        long cpu_delta = usage - prev;
        long time_delta = (now - prev_time) * 1000000000;
        if (time_delta > 0) percent = (double)cpu_delta / time_delta * 100.0;
    }
    
    prev = usage;
    prev_time = now;
    return percent;
}

static long get_memory(void) {
    FILE *f = fopen("/sys/fs/cgroup/memory/memory.usage_in_bytes", "r");
    if (!f) f = fopen("/sys/fs/cgroup/memory.current", "r");
    if (!f) return (rand() % 500) + 100;  // Fallback
    
    long usage;
    fscanf(f, "%ld", &usage);
    fclose(f);
    return usage / (1024 * 1024);
}

static void get_network(long *rx, long *tx) {
    FILE *f = fopen("/proc/net/dev", "r");
    if (!f) {
        *rx = rand() % 100000;
        *tx = rand() % 100000;
        return;
    }
    
    char line[512];
    *rx = *tx = 0;
    
    fgets(line, sizeof(line), f);
    fgets(line, sizeof(line), f);
    
    while (fgets(line, sizeof(line), f)) {
        char iface[64];
        long r, t;
        if (sscanf(line, "%[^:]: %ld %*d %*d %*d %*d %*d %*d %*d %ld", iface, &r, &t) == 3) {
            if (strcmp(iface, "lo") != 0) {
                *rx += r;
                *tx += t;
            }
        }
    }
    fclose(f);
}

static void collect_metrics(metrics_t *m) {
    m->cpu = get_cpu();
    m->mem_mb = get_memory();
    m->disk_mb = rand() % 1000;  // Simplificado
    get_network(&m->net_rx, &m->net_tx);
}

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
    
    printf("[%s] Connected to %s:%d\n", producer_id, broker_host, broker_port);
    
    char reg[256];
    snprintf(reg, sizeof(reg), "REGISTER_PRODUCER|%s\n", producer_id);
    send(broker_socket, reg, strlen(reg), 0);
    
    return 0;
}

static int send_metric(const char *name, const char *value) {
    if (broker_socket < 0 && connect_to_broker() != 0) return -1;
    
    char topic[512];
    snprintf(topic, sizeof(topic), "%s/%s/%s", base_topic, producer_id, name);
    
    char msg[BUFFER_SIZE];
    snprintf(msg, sizeof(msg), "PRODUCE|%s||%s\n", topic, value);
    
    if (send(broker_socket, msg, strlen(msg), 0) <= 0) {
        close(broker_socket);
        broker_socket = -1;
        return -1;
    }
    
    return 0;
}

static void publish_metrics(const metrics_t *m) {
    char val[512];
    time_t now = time(NULL);
    
    snprintf(val, sizeof(val), "{\"cpu\":%.2f,\"ts\":%ld}", m->cpu, now);
    send_metric("cpu", val);
    
    snprintf(val, sizeof(val), "{\"mem\":%ld,\"ts\":%ld}", m->mem_mb, now);
    send_metric("memory", val);
    
    snprintf(val, sizeof(val), "{\"disk\":%ld,\"ts\":%ld}", m->disk_mb, now);
    send_metric("disk", val);
    
    snprintf(val, sizeof(val), "{\"rx\":%ld,\"tx\":%ld,\"ts\":%ld}", m->net_rx, m->net_tx, now);
    send_metric("network", val);
    
    printf("[%s] CPU:%.1f%% MEM:%ldMB NET:rx=%ld,tx=%ld\n", 
           producer_id, m->cpu, m->mem_mb, m->net_rx, m->net_tx);
}

static void signal_handler(int sig) {
    running = 0;
}

int main(int argc, char *argv[]) {
    // Argumentos: producer_id base_topic broker_host broker_port
    if (argc >= 2) strncpy(producer_id, argv[1], 63);
    if (argc >= 3) strncpy(base_topic, argv[2], 255);
    if (argc >= 4) strncpy(broker_host, argv[3], 127);
    if (argc >= 5) broker_port = atoi(argv[4]);
    
    printf("\n=== AKLight Producer: %s ===\n", producer_id);
    printf("Broker: %s:%d\n", broker_host, broker_port);
    printf("Topic: %s/%s/*\n\n", base_topic, producer_id);
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    if (connect_to_broker() != 0) {
        printf("Initial connection failed, will retry\n");
    }
    
    metrics_t metrics;
    
    while (running) {
        collect_metrics(&metrics);
        publish_metrics(&metrics);
        sleep(INTERVAL);
    }
    
    if (broker_socket >= 0) close(broker_socket);
    
    return 0;
}