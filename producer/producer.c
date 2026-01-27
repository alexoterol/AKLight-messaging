// AKLight - Producer con Fork para Recolección de Métricas

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

// CONSTANTES Y ESTRUCTURAS

#define MAX_BUFFER 4096
#define MAX_TOPIC 256
#define MAX_KEY 64
#define RECONNECT_DELAY 5

typedef struct {
    char type[32];
    double value;
    char unit[16];
    long timestamp;
} Metric;

static volatile sig_atomic_t running = 1;
static int broker_socket = -1;
static char producer_id[64] = "producer1";
static char base_topic[128] = "metrics/docker";
static char broker_host[256] = "localhost";
static int broker_port = 7000;
static int interval_seconds = 5;
static int use_key = 1;

static int cpu_pipe[2];
static int mem_pipe[2];

static pid_t cpu_collector_pid = -1;
static pid_t mem_collector_pid = -1;

// SIGNAL HANDLERS

void signal_handler(int sig) {
    (void)sig;
    running = 0;
}

void child_signal_handler(int sig) {
    (void)sig;
    running = 0;
}

void setup_signals(void) {
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, SIG_IGN);
}

// RECOLECCIÓN DE MÉTRICAS

double get_cpu_usage(void) {
    static long prev_usage = 0;
    static long prev_time = 0;
    double cpu_percent = 0.0;
    FILE *fp;
    
    const char *paths[] = {
        "/sys/fs/cgroup/cpu.stat",
        "/sys/fs/cgroup/system.slice/cpu.stat",
        NULL
    };
    
    for (int i = 0; paths[i]; i++) {
        fp = fopen(paths[i], "r");
        if (fp) {
            char line[256];
            long usage_usec = 0;
            while (fgets(line, sizeof(line), fp)) {
                if (sscanf(line, "usage_usec %ld", &usage_usec) == 1) {
                    break;
                }
            }
            fclose(fp);
            
            if (usage_usec > 0) {
                struct timespec ts;
                clock_gettime(CLOCK_MONOTONIC, &ts);
                long current_time = ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
                
                if (prev_time > 0 && current_time > prev_time) {
                    long delta_usage = usage_usec - prev_usage;
                    long delta_time = current_time - prev_time;
                    cpu_percent = (double)delta_usage / (double)delta_time * 100.0;
                    if (cpu_percent > 100.0) cpu_percent = 100.0;
                    if (cpu_percent < 0.0) cpu_percent = 0.0;
                }
                
                prev_usage = usage_usec;
                prev_time = current_time;
                return cpu_percent;
            }
        }
    }
    fp = fopen("/proc/stat", "r");
    if (fp) {
        static long prev_idle = 0, prev_total = 0;
        long user, nice, system, idle, iowait, irq, softirq, steal = 0;
        
        if (fscanf(fp, "cpu %ld %ld %ld %ld %ld %ld %ld %ld",
                   &user, &nice, &system, &idle, &iowait, &irq, &softirq, &steal) >= 7) {
            long total = user + nice + system + idle + iowait + irq + softirq + steal;
            long idle_total = idle + iowait;
            
            if (prev_total > 0 && total > prev_total) {
                long diff_total = total - prev_total;
                long diff_idle = idle_total - prev_idle;
                cpu_percent = 100.0 * (1.0 - (double)diff_idle / (double)diff_total);
                if (cpu_percent < 0.0) cpu_percent = 0.0;
                if (cpu_percent > 100.0) cpu_percent = 100.0;
            }
            
            prev_idle = idle_total;
            prev_total = total;
        }
        fclose(fp);
    }
    
    return cpu_percent;
}

long get_memory_usage_mb(void) {
    FILE *fp;
    long memory_bytes = 0;
    
    // cgroups v2 paths
    const char *paths[] = {
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/system.slice/memory.current",
        NULL
    };
    
    for (int i = 0; paths[i]; i++) {
        fp = fopen(paths[i], "r");
        if (fp) {
            if (fscanf(fp, "%ld", &memory_bytes) == 1 && memory_bytes > 0) {
                fclose(fp);
                return memory_bytes / (1024 * 1024);
            }
            fclose(fp);
        }
    }
    
    fp = fopen("/proc/meminfo", "r");
    if (fp) {
        char line[256];
        long mem_total = 0, mem_available = 0;
        while (fgets(line, sizeof(line), fp)) {
            if (sscanf(line, "MemTotal: %ld kB", &mem_total) == 1) continue;
            if (sscanf(line, "MemAvailable: %ld kB", &mem_available) == 1) break;
        }
        fclose(fp);
        if (mem_total > 0 && mem_available > 0) {
            return (mem_total - mem_available) / 1024;  // Return used memory in MB
        }
    }
    
    return 0;
}

double get_disk_usage_percent(void) {
    FILE *fp;
    double usage = 0.0;
    
    fp = popen("df / 2>/dev/null | awk 'NR==2 {print $5}' | tr -d '%'", "r");
    if (fp) {
        if (fscanf(fp, "%lf", &usage) != 1) usage = 0.0;
        pclose(fp);
    }
    
    return usage;
}

void get_network_stats(long *rx_bytes, long *tx_bytes) {
    FILE *fp;
    *rx_bytes = 0;
    *tx_bytes = 0;
    
    fp = fopen("/proc/net/dev", "r");
    if (fp) {
        char line[512];
        fgets(line, sizeof(line), fp);  // Skip header 1
        fgets(line, sizeof(line), fp);  // Skip header 2
        
        while (fgets(line, sizeof(line), fp)) {
            char iface[32];
            long rx, tx;
            if (sscanf(line, " %31[^:]: %ld %*d %*d %*d %*d %*d %*d %*d %ld",
                       iface, &rx, &tx) == 3) {
                // Skip loopback
                if (strcmp(iface, "lo") != 0) {
                    *rx_bytes += rx;
                    *tx_bytes += tx;
                }
            }
        }
        fclose(fp);
    }
}

// PROCESOS HIJOS PARA RECOLECCIÓN DE MÉTRICAS (FORK)

void cpu_collector_process(int write_fd) {
    // Re-configurar señales para el hijo
    signal(SIGINT, child_signal_handler);
    signal(SIGTERM, child_signal_handler);
    signal(SIGPIPE, SIG_IGN);
    
    close(cpu_pipe[0]);
    close(mem_pipe[0]);
    close(mem_pipe[1]);
    
    printf("[CPU Collector PID=%d] Iniciado\n", getpid());
    fflush(stdout);
    
    Metric metric;
    memset(&metric, 0, sizeof(metric));
    strncpy(metric.type, "cpu", sizeof(metric.type) - 1);
    strncpy(metric.unit, "percent", sizeof(metric.unit) - 1);
    
    get_cpu_usage();
    sleep(1);
    
    while (running) {
        metric.value = get_cpu_usage();
        metric.timestamp = time(NULL);
        
        ssize_t written = write(write_fd, &metric, sizeof(metric));
        if (written < 0) {
            if (errno == EPIPE) break;
            perror("[CPU Collector] write error");
        }
        
        sleep(interval_seconds);
    }
    
    close(write_fd);
    printf("[CPU Collector] Finalizando\n");
    fflush(stdout);
    _exit(0);
}

void mem_collector_process(int write_fd) {
    signal(SIGINT, child_signal_handler);
    signal(SIGTERM, child_signal_handler);
    signal(SIGPIPE, SIG_IGN);
    
    close(cpu_pipe[0]);
    close(cpu_pipe[1]);
    close(mem_pipe[0]);
    
    printf("[MEM Collector PID=%d] Iniciado\n", getpid());
    fflush(stdout);
    
    Metric metric;
    memset(&metric, 0, sizeof(metric));
    strncpy(metric.type, "memory", sizeof(metric.type) - 1);
    strncpy(metric.unit, "MB", sizeof(metric.unit) - 1);
    
    while (running) {
        metric.value = (double)get_memory_usage_mb();
        metric.timestamp = time(NULL);
        
        ssize_t written = write(write_fd, &metric, sizeof(metric));
        if (written < 0) {
            if (errno == EPIPE) break;
            perror("[MEM Collector] write error");
        }
        
        sleep(interval_seconds);
    }
    
    close(write_fd);
    printf("[MEM Collector] Finalizando\n");
    fflush(stdout);
    _exit(0);
}

// CONEXIÓN AL BROKER
int connect_to_broker(void) {
    struct hostent *he;
    struct sockaddr_in server_addr;
    
    printf("[Producer] Conectando a %s:%d...\n", broker_host, broker_port);
    fflush(stdout);
    
    he = gethostbyname(broker_host);
    if (!he) {
        fprintf(stderr, "[Producer] No se puede resolver host: %s\n", broker_host);
        return -1;
    }
    
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("[Producer] Error creando socket");
        return -1;
    }
    
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(broker_port);
    memcpy(&server_addr.sin_addr, he->h_addr, he->h_length);
    
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("[Producer] Error conectando al broker");
        close(sock);
        return -1;
    }
    
    printf("[Producer] ✅ Conectado a %s:%d\n", broker_host, broker_port);
    fflush(stdout);
    
    // Registrar como producer
    char msg[MAX_BUFFER];
    snprintf(msg, sizeof(msg), "REGISTER_PRODUCER|%s\n", producer_id);
    if (send(sock, msg, strlen(msg), 0) < 0) {
        perror("[Producer] Error enviando registro");
        close(sock);
        return -1;
    }
    
    char response[MAX_BUFFER];
    ssize_t n = recv(sock, response, sizeof(response) - 1, 0);
    if (n > 0) {
        response[n] = '\0';
        printf("[Producer] Broker response: %s", response);
        fflush(stdout);
    }
    
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    
    return sock;
}

// ENVÍO DE MÉTRICAS (sin esperar ACK bloqueante)

int send_metric(int sock, const char *metric_type, double value, const char *unit) {
    char topic[MAX_TOPIC];
    char payload[MAX_BUFFER];
    char message[MAX_BUFFER];
    char key[MAX_KEY] = "";
    
    snprintf(topic, sizeof(topic), "%s/%s/%s", base_topic, producer_id, metric_type);
    
    snprintf(payload, sizeof(payload),
             "{\"producer\":\"%s\",\"metric\":\"%s\",\"value\":%.2f,\"unit\":\"%s\",\"timestamp\":%ld}",
             producer_id, metric_type, value, unit, time(NULL));
    
    if (use_key) {
        snprintf(key, sizeof(key), "%s", producer_id);
    }
    
    snprintf(message, sizeof(message), "PRODUCE|%s|%s|%s\n", topic, key, payload);
    
    ssize_t sent = send(sock, message, strlen(message), MSG_NOSIGNAL);
    if (sent <= 0) {
        perror("[Producer] Error en send()");
        return -1;
    }
    
    printf("[Producer] 📤 %s = %.2f %s\n", metric_type, value, unit);
    fflush(stdout);
    
    // Leer ACK de forma no bloqueante
    char ack_buffer[512];
    ssize_t n = recv(sock, ack_buffer, sizeof(ack_buffer) - 1, MSG_DONTWAIT);
    if (n > 0) {
        ack_buffer[n] = '\0';
        if (strstr(ack_buffer, "ACK|")) {
            printf("[Producer] ✅ ACK recibido\n");
            fflush(stdout);
        }
    }
    
    return 0;
}

// PROCESO PADRE - BUCLE PRINCIPAL

void parent_main_loop(void) {
    close(cpu_pipe[1]);
    close(mem_pipe[1]);
    
    fcntl(cpu_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(mem_pipe[0], F_SETFL, O_NONBLOCK);
    
    Metric cpu_metric = {0};
    Metric mem_metric = {0};
    int has_cpu = 0, has_mem = 0;
    
    while (running) {
        if (broker_socket < 0) {
            broker_socket = connect_to_broker();
            if (broker_socket < 0) {
                fprintf(stderr, "[Producer] Reintentando en %d segundos...\n", RECONNECT_DELAY);
                sleep(RECONNECT_DELAY);
                continue;
            }
        }
        
        ssize_t n = read(cpu_pipe[0], &cpu_metric, sizeof(cpu_metric));
        if (n == sizeof(cpu_metric)) has_cpu = 1;
        
        n = read(mem_pipe[0], &mem_metric, sizeof(mem_metric));
        if (n == sizeof(mem_metric)) has_mem = 1;
        
        int need_reconnect = 0;
        
        // Enviar CPU
        if (has_cpu && broker_socket >= 0) {
            if (send_metric(broker_socket, cpu_metric.type, cpu_metric.value, cpu_metric.unit) < 0) {
                need_reconnect = 1;
            }
            has_cpu = 0;
        }
        
        // Enviar Memory
        if (has_mem && broker_socket >= 0 && !need_reconnect) {
            if (send_metric(broker_socket, mem_metric.type, mem_metric.value, mem_metric.unit) < 0) {
                need_reconnect = 1;
            }
            has_mem = 0;
        }
        
        // Enviar Disk
        if (broker_socket >= 0 && !need_reconnect) {
            double disk = get_disk_usage_percent();
            if (send_metric(broker_socket, "disk", disk, "percent") < 0) {
                need_reconnect = 1;
            }
        }
        
        // Enviar Network
        if (broker_socket >= 0 && !need_reconnect) {
            long rx, tx;
            get_network_stats(&rx, &tx);
            double net_rx_mb = (double)rx / (1024.0 * 1024.0);
            
            if (send_metric(broker_socket, "network_rx", net_rx_mb, "MB") < 0) {
                need_reconnect = 1;
            }
        }
        
        if (need_reconnect) {
            close(broker_socket);
            broker_socket = -1;
            fprintf(stderr, "[Producer] Conexión perdida, reconectando...\n");
        }
        
        sleep(interval_seconds);
    }
    
    close(cpu_pipe[0]);
    close(mem_pipe[0]);
    
    if (broker_socket >= 0) {
        close(broker_socket);
    }
}

// MAIN

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    
    char *env;
    
    if ((env = getenv("PRODUCER_ID")) != NULL) {
        strncpy(producer_id, env, sizeof(producer_id) - 1);
    }
    if ((env = getenv("BASE_TOPIC")) != NULL) {
        strncpy(base_topic, env, sizeof(base_topic) - 1);
    }
    if ((env = getenv("BROKER_HOST")) != NULL) {
        strncpy(broker_host, env, sizeof(broker_host) - 1);
    }
    if ((env = getenv("BROKER_PORT")) != NULL) {
        broker_port = atoi(env);
    }
    if ((env = getenv("INTERVAL")) != NULL) {
        interval_seconds = atoi(env);
    }
    if ((env = getenv("USE_KEY")) != NULL) {
        use_key = atoi(env);
    }
    
    if (argc >= 3) {
        strncpy(broker_host, argv[1], sizeof(broker_host) - 1);
        broker_port = atoi(argv[2]);
    }
    
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║          AKLight v2 - Producer con Fork                      ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    printf("║  Producer ID: %-45s ║\n", producer_id);
    printf("║  Base Topic: %-46s ║\n", base_topic);
    printf("║  Broker: %s:%-40d ║\n", broker_host, broker_port);
    printf("║  Intervalo: %d segundos                                       ║\n", interval_seconds);
    printf("╚══════════════════════════════════════════════════════════════╝\n\n");
    fflush(stdout);
    
    setup_signals();
    
    if (pipe(cpu_pipe) < 0 || pipe(mem_pipe) < 0) {
        perror("Error creando pipes");
        return 1;
    }
    
    printf("[Producer] Creando procesos hijos con fork()...\n");
    fflush(stdout);
    
    // Fork CPU collector
    cpu_collector_pid = fork();
    if (cpu_collector_pid < 0) {
        perror("Error en fork() para CPU collector");
        return 1;
    } else if (cpu_collector_pid == 0) {
        cpu_collector_process(cpu_pipe[1]);
        _exit(0);
    }
    
    // Fork MEM collector
    mem_collector_pid = fork();
    if (mem_collector_pid < 0) {
        perror("Error en fork() para MEM collector");
        kill(cpu_collector_pid, SIGTERM);
        return 1;
    } else if (mem_collector_pid == 0) {
        mem_collector_process(mem_pipe[1]);
        _exit(0);
    }
    
    printf("[Producer] ✅ Proceso padre PID=%d\n", getpid());
    printf("[Producer] ✅ CPU Collector PID=%d\n", cpu_collector_pid);
    printf("[Producer] ✅ MEM Collector PID=%d\n", mem_collector_pid);
    printf("[Producer] Total: 3 procesos (padre + 2 hijos)\n");
    printf("═══════════════════════════════════════════════════════════════\n\n");
    fflush(stdout);
    
    parent_main_loop();
    
    printf("[Producer] Terminando procesos hijos...\n");
    fflush(stdout);
    
    if (cpu_collector_pid > 0) {
        kill(cpu_collector_pid, SIGTERM);
        waitpid(cpu_collector_pid, NULL, WNOHANG);
    }
    if (mem_collector_pid > 0) {
        kill(mem_collector_pid, SIGTERM);
        waitpid(mem_collector_pid, NULL, WNOHANG);
    }
    
    printf("[Producer] 👋 Finalizado\n");
    return 0;
}
