// =============================================================================
// AKLight v2 - Consumer con Thresholds y Alertas (CORREGIDO)
// =============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

// =============================================================================
// CONSTANTES
// =============================================================================

#define MAX_BUFFER 8192
#define MAX_TOPIC 256
#define MAX_TOPICS 32
#define OFFSETS_DIR "/app/offsets"
#define RECONNECT_DELAY 5
#define ALERT_COOLDOWN 60

// =============================================================================
// ESTRUCTURAS
// =============================================================================

typedef enum {
    OP_GT, OP_LT, OP_GTE, OP_LTE, OP_EQ
} ThresholdOp;

typedef struct {
    char metric[32];
    double value;
    ThresholdOp op;
    time_t last_alert;
} Threshold;

typedef struct {
    char topic[MAX_TOPIC];
    long offset;
} OffsetEntry;

// =============================================================================
// VARIABLES GLOBALES
// =============================================================================

static volatile sig_atomic_t running = 1;
static int broker_socket = -1;
static char consumer_id[64] = "consumer1";
static char broker_host[256] = "localhost";
static int broker_port = 7000;
static int session_persistent = 1;
static char topics[MAX_TOPICS][MAX_TOPIC];
static int num_topics = 0;

static OffsetEntry offset_table[256];
static int offset_count = 0;
static pthread_mutex_t offset_mutex = PTHREAD_MUTEX_INITIALIZER;

static Threshold thresholds[16];
static int threshold_count = 0;

static char twilio_account_sid[128] = "";
static char twilio_auth_token[128] = "";
static char twilio_from_number[64] = "";
static char twilio_to_number[64] = "";
static int twilio_enabled = 0;

static long messages_received = 0;
static long alerts_triggered = 0;

// =============================================================================
// SIGNAL HANDLERS
// =============================================================================

void signal_handler(int sig) {
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
}

// =============================================================================
// GESTIÓN DE OFFSETS
// =============================================================================

char* get_offset_file_path(void) {
    static char path[512];
    snprintf(path, sizeof(path), "%s/%s.offsets", OFFSETS_DIR, consumer_id);
    return path;
}

void load_offsets(void) {
    if (!session_persistent) return;
    
    char *path = get_offset_file_path();
    FILE *fp = fopen(path, "r");
    if (!fp) {
        printf("[Consumer] No hay archivo de offsets previo\n");
        fflush(stdout);
        return;
    }
    
    pthread_mutex_lock(&offset_mutex);
    
    char line[MAX_BUFFER];
    while (fgets(line, sizeof(line), fp) && offset_count < 256) {
        char *sep = strchr(line, '|');
        if (sep) {
            *sep = '\0';
            strncpy(offset_table[offset_count].topic, line, MAX_TOPIC - 1);
            offset_table[offset_count].offset = atol(sep + 1);
            printf("[Consumer] Offset cargado: %s = %ld\n", 
                   offset_table[offset_count].topic, 
                   offset_table[offset_count].offset);
            offset_count++;
        }
    }
    
    pthread_mutex_unlock(&offset_mutex);
    fclose(fp);
    
    printf("[Consumer] Cargados %d offsets\n", offset_count);
    fflush(stdout);
}

void save_offsets(void) {
    if (!session_persistent) return;
    
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", OFFSETS_DIR);
    system(cmd);
    
    char *path = get_offset_file_path();
    FILE *fp = fopen(path, "w");
    if (!fp) {
        perror("[Consumer] Error guardando offsets");
        return;
    }
    
    pthread_mutex_lock(&offset_mutex);
    for (int i = 0; i < offset_count; i++) {
        fprintf(fp, "%s|%ld\n", offset_table[i].topic, offset_table[i].offset);
    }
    pthread_mutex_unlock(&offset_mutex);
    
    fclose(fp);
    printf("[Consumer] Guardados %d offsets\n", offset_count);
    fflush(stdout);
}

long get_offset(const char *topic) {
    pthread_mutex_lock(&offset_mutex);
    for (int i = 0; i < offset_count; i++) {
        if (strcmp(offset_table[i].topic, topic) == 0) {
            long off = offset_table[i].offset;
            pthread_mutex_unlock(&offset_mutex);
            return off;
        }
    }
    pthread_mutex_unlock(&offset_mutex);
    return 0;
}

void update_offset(const char *topic, long offset) {
    pthread_mutex_lock(&offset_mutex);
    
    for (int i = 0; i < offset_count; i++) {
        if (strcmp(offset_table[i].topic, topic) == 0) {
            if (offset > offset_table[i].offset) {
                offset_table[i].offset = offset;
            }
            pthread_mutex_unlock(&offset_mutex);
            return;
        }
    }
    
    if (offset_count < 256) {
        strncpy(offset_table[offset_count].topic, topic, MAX_TOPIC - 1);
        offset_table[offset_count].offset = offset;
        offset_count++;
    }
    
    pthread_mutex_unlock(&offset_mutex);
}

// =============================================================================
// THRESHOLDS
// =============================================================================

void init_thresholds(void) {
    char *env;
    
    if ((env = getenv("CPU_THRESHOLD")) != NULL) {
        thresholds[threshold_count].value = atof(env);
        strcpy(thresholds[threshold_count].metric, "cpu");
        thresholds[threshold_count].op = OP_GT;
        thresholds[threshold_count].last_alert = 0;
        threshold_count++;
        printf("[Consumer] Threshold CPU: > %.1f%%\n", thresholds[threshold_count-1].value);
    }
    
    if ((env = getenv("MEM_THRESHOLD")) != NULL) {
        thresholds[threshold_count].value = atof(env);
        strcpy(thresholds[threshold_count].metric, "memory");
        thresholds[threshold_count].op = OP_GT;
        thresholds[threshold_count].last_alert = 0;
        threshold_count++;
        printf("[Consumer] Threshold Memory: > %.1f MB\n", thresholds[threshold_count-1].value);
    }
    
    fflush(stdout);
}

int check_threshold(const char *metric, double value, Threshold **matched) {
    time_t now = time(NULL);
    
    for (int i = 0; i < threshold_count; i++) {
        if (strcmp(thresholds[i].metric, metric) != 0) continue;
        
        int triggered = 0;
        switch (thresholds[i].op) {
            case OP_GT:  triggered = (value > thresholds[i].value); break;
            case OP_LT:  triggered = (value < thresholds[i].value); break;
            case OP_GTE: triggered = (value >= thresholds[i].value); break;
            case OP_LTE: triggered = (value <= thresholds[i].value); break;
            case OP_EQ:  triggered = (value == thresholds[i].value); break;
        }
        
        if (triggered && (now - thresholds[i].last_alert >= ALERT_COOLDOWN)) {
            thresholds[i].last_alert = now;
            *matched = &thresholds[i];
            return 1;
        }
    }
    
    return 0;
}

// =============================================================================
// TWILIO
// =============================================================================

void init_twilio(void) {
    char *env;
    
    if ((env = getenv("TWILIO_ACCOUNT_SID")) != NULL) 
        strncpy(twilio_account_sid, env, sizeof(twilio_account_sid) - 1);
    if ((env = getenv("TWILIO_AUTH_TOKEN")) != NULL) 
        strncpy(twilio_auth_token, env, sizeof(twilio_auth_token) - 1);
    if ((env = getenv("TWILIO_FROM_NUMBER")) != NULL) 
        strncpy(twilio_from_number, env, sizeof(twilio_from_number) - 1);
    if ((env = getenv("TWILIO_TO_NUMBER")) != NULL) 
        strncpy(twilio_to_number, env, sizeof(twilio_to_number) - 1);
    
    if (strlen(twilio_account_sid) > 0 && strlen(twilio_auth_token) > 0) {
        twilio_enabled = 1;
        printf("[Consumer] Twilio WhatsApp HABILITADO\n");
    } else {
        printf("[Consumer] Twilio WhatsApp no configurado\n");
    }
    fflush(stdout);
}

void send_alert(const char *metric, double value, double threshold, const char *producer) {
    // Banner en consola (igual que antes)
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║  🚨 ALERTA: THRESHOLD EXCEDIDO                               ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    printf("║  Métrica: %-50s ║\n", metric);
    printf("║  Valor actual: %-44.2f ║\n", value);
    printf("║  Threshold: %-43.2f ║\n", threshold);
    printf("║  Producer: %-49s ║\n", producer);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");
    fflush(stdout);
    
    alerts_triggered++;
    
    if (twilio_enabled) {
        char mensaje_body[256];
        snprintf(mensaje_body, sizeof(mensaje_body),
                 "🚨 *AKLight Alert*\\n\\n"
                 "Métrica: *%s*\\n"
                 "Valor: *%.2f* (límite: %.2f)\\n"
                 "Producer: %s",
                 metric, value, threshold, producer);
        
        char comando[2048];
        snprintf(comando, sizeof(comando),
                 "curl -s -X POST 'https://api.twilio.com/2010-04-01/Accounts/%s/Messages.json' "
                 "-u '%s:%s' "
                 "--data-urlencode 'From=%s' "
                 "--data-urlencode 'To=%s' "
                 "--data-urlencode 'Body=%s' "
                 "> /dev/null 2>&1 &",
                 twilio_account_sid, twilio_account_sid, twilio_auth_token,
                 twilio_from_number, twilio_to_number, mensaje_body);
        
        system(comando);
        
        printf("[Consumer] 📱 Alerta WhatsApp enviada\n");
        fflush(stdout);
    }
}

// =============================================================================
// PROCESAMIENTO DE MENSAJES
// =============================================================================

void process_message(const char *topic, long offset, int partition, 
                    const char *key, const char *payload) {
    messages_received++;
    
    printf("[Consumer] 📨 MESSAGE #%ld\n", messages_received);
    printf("           Topic: %s\n", topic);
    printf("           Offset: %ld, Partition: %d, Key: %s\n", offset, partition, key[0] ? key : "(none)");
    printf("           Payload: %.100s%s\n", payload, strlen(payload) > 100 ? "..." : "");
    fflush(stdout);
    
    // Actualizar offset (offset + 1 para el próximo)
    update_offset(topic, offset + 1);
    
    // Guardar cada 5 mensajes
    if (session_persistent && messages_received % 5 == 0) {
        save_offsets();
    }
    
    // Parsear payload para thresholds
    char metric[32] = "";
    double value = 0;
    char producer[64] = "";
    
    // Extraer "metric":"xxx"
    char *p = strstr(payload, "\"metric\":\"");
    if (p) {
        p += 10;
        char *end = strchr(p, '"');
        if (end && (size_t)(end - p) < sizeof(metric)) {
            strncpy(metric, p, end - p);
            metric[end - p] = '\0';
        }
    }
    
    // Extraer "value":xxx
    p = strstr(payload, "\"value\":");
    if (p) {
        value = atof(p + 8);
    }
    
    // Extraer "producer":"xxx"
    p = strstr(payload, "\"producer\":\"");
    if (p) {
        p += 12;
        char *end = strchr(p, '"');
        if (end && (size_t)(end - p) < sizeof(producer)) {
            strncpy(producer, p, end - p);
            producer[end - p] = '\0';
        }
    }
    
    // Verificar thresholds
    if (metric[0]) {
        Threshold *matched = NULL;
        if (check_threshold(metric, value, &matched)) {
            send_alert(metric, value, matched->value, producer);
        }
    }
}

// =============================================================================
// CONEXIÓN AL BROKER
// =============================================================================

int connect_to_broker(void) {
    struct hostent *he;
    struct sockaddr_in server_addr;
    
    printf("[Consumer] Conectando a %s:%d...\n", broker_host, broker_port);
    fflush(stdout);
    
    he = gethostbyname(broker_host);
    if (!he) {
        fprintf(stderr, "[Consumer] No se puede resolver host: %s\n", broker_host);
        return -1;
    }
    
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("[Consumer] Error creando socket");
        return -1;
    }
    
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(broker_port);
    memcpy(&server_addr.sin_addr, he->h_addr, he->h_length);
    
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("[Consumer] Error conectando");
        close(sock);
        return -1;
    }
    
    printf("[Consumer] ✅ Conectado\n");
    fflush(stdout);
    
    // Registrar
    char msg[MAX_BUFFER];
    snprintf(msg, sizeof(msg), "REGISTER_CONSUMER|%s|%d\n", consumer_id, session_persistent);
    send(sock, msg, strlen(msg), 0);
    
    // Leer respuesta
    char response[MAX_BUFFER];
    ssize_t n = recv(sock, response, sizeof(response) - 1, 0);
    if (n > 0) {
        response[n] = '\0';
        printf("[Consumer] Broker: %s", response);
        fflush(stdout);
    }
    
    // Suscribirse a topics
    for (int i = 0; i < num_topics; i++) {
        snprintf(msg, sizeof(msg), "SUBSCRIBE|%s\n", topics[i]);
        send(sock, msg, strlen(msg), 0);
        printf("[Consumer] 🔔 Suscrito a: %s\n", topics[i]);
        usleep(100000);
    }
    fflush(stdout);
    
    return sock;
}

// =============================================================================
// PARSEAR LÍNEA DE MENSAJE
// =============================================================================

void parse_message_line(char *line) {
    if (strncmp(line, "MESSAGE|", 8) != 0) return;
    
    // MESSAGE|topic|offset|partition|key|payload
    char *parts[6] = {NULL};
    int part_count = 0;
    char *start = line + 8;  // Skip "MESSAGE|"
    
    for (char *pos = start; *pos && part_count < 5; pos++) {
        if (*pos == '|') {
            *pos = '\0';
            parts[part_count++] = start;
            start = pos + 1;
        }
    }
    // El último es el payload (puede contener |)
    if (part_count < 6) {
        parts[part_count++] = start;
    }
    
    if (part_count >= 5) {
        process_message(parts[0], atol(parts[1]), atoi(parts[2]),
                       parts[3], parts[4] ? parts[4] : "");
    }
}

// =============================================================================
// BUCLE PRINCIPAL
// =============================================================================

void main_loop(void) {
    char buffer[MAX_BUFFER];
    char line_buffer[MAX_BUFFER * 2];
    int line_pos = 0;
    
    while (running) {
        // Conectar si es necesario
        if (broker_socket < 0) {
            broker_socket = connect_to_broker();
            if (broker_socket < 0) {
                fprintf(stderr, "[Consumer] Reintentando en %d segundos...\n", RECONNECT_DELAY);
                sleep(RECONNECT_DELAY);
                continue;
            }
        }
        
        // Configurar socket no bloqueante temporalmente para recibir pushes
        struct timeval tv;
        tv.tv_sec = 3;  // 3 segundos de timeout
        tv.tv_usec = 0;
        setsockopt(broker_socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        
        // Recibir mensajes (push del broker)
        ssize_t n = recv(broker_socket, buffer, sizeof(buffer) - 1, 0);
        
        if (n > 0) {
            buffer[n] = '\0';
            
            // Procesar byte a byte
            for (ssize_t i = 0; i < n; i++) {
                if (buffer[i] == '\n') {
                    line_buffer[line_pos] = '\0';
                    
                    if (line_pos > 0) {
                        // Procesar línea
                        if (strncmp(line_buffer, "MESSAGE|", 8) == 0) {
                            parse_message_line(line_buffer);
                        } else if (strncmp(line_buffer, "SUBSCRIBED|", 11) == 0) {
                            printf("[Consumer] ✅ %s\n", line_buffer);
                            fflush(stdout);
                        } else if (strncmp(line_buffer, "FETCH_END|", 10) == 0) {
                            // Ignorar silenciosamente
                        } else if (strncmp(line_buffer, "PONG", 4) == 0) {
                            // Heartbeat OK
                        }
                    }
                    
                    line_pos = 0;
                } else if (line_pos < (int)sizeof(line_buffer) - 1) {
                    line_buffer[line_pos++] = buffer[i];
                }
            }
            
        } else if (n == 0) {
            printf("[Consumer] ⚠️ Broker cerró la conexión\n");
            fflush(stdout);
            close(broker_socket);
            broker_socket = -1;
            continue;
            
        } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT) {
            // Timeout - no hay mensajes push, intentar FETCH
            
            // Enviar FETCH para cada topic
            for (int i = 0; i < num_topics; i++) {
                long offset = get_offset(topics[i]);
                char fetch_msg[MAX_BUFFER];
                snprintf(fetch_msg, sizeof(fetch_msg), "FETCH|%s|%ld\n", topics[i], offset);
                
                if (send(broker_socket, fetch_msg, strlen(fetch_msg), MSG_NOSIGNAL) < 0) {
                    printf("[Consumer] ⚠️ Error en FETCH, reconectando...\n");
                    fflush(stdout);
                    close(broker_socket);
                    broker_socket = -1;
                    break;
                }
            }
            
            if (broker_socket < 0) continue;
            
            // Leer respuestas del FETCH
            tv.tv_sec = 2;
            setsockopt(broker_socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            
            n = recv(broker_socket, buffer, sizeof(buffer) - 1, 0);
            if (n > 0) {
                buffer[n] = '\0';
                
                for (ssize_t i = 0; i < n; i++) {
                    if (buffer[i] == '\n') {
                        line_buffer[line_pos] = '\0';
                        
                        if (line_pos > 0) {
                            if (strncmp(line_buffer, "MESSAGE|", 8) == 0) {
                                parse_message_line(line_buffer);
                            }
                        }
                        
                        line_pos = 0;
                    } else if (line_pos < (int)sizeof(line_buffer) - 1) {
                        line_buffer[line_pos++] = buffer[i];
                    }
                }
            }
            
            // Heartbeat PING
            if (broker_socket >= 0) {
                send(broker_socket, "PING\n", 5, MSG_NOSIGNAL);
            }
            
        } else {
            perror("[Consumer] Error en recv()");
            close(broker_socket);
            broker_socket = -1;
        }
    }
}

// =============================================================================
// MAIN
// =============================================================================

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    
    char *env;
    
    if ((env = getenv("CONSUMER_ID")) != NULL)
        strncpy(consumer_id, env, sizeof(consumer_id) - 1);
    if ((env = getenv("BROKER_HOST")) != NULL)
        strncpy(broker_host, env, sizeof(broker_host) - 1);
    if ((env = getenv("BROKER_PORT")) != NULL)
        broker_port = atoi(env);
    if ((env = getenv("SESSION_PERSISTENT")) != NULL)
        session_persistent = atoi(env);
    
    // Topics separados por coma
    if ((env = getenv("TOPICS")) != NULL) {
        char topics_copy[1024];
        strncpy(topics_copy, env, sizeof(topics_copy) - 1);
        
        char *saveptr;
        char *token = strtok_r(topics_copy, ",", &saveptr);
        while (token && num_topics < MAX_TOPICS) {
            strncpy(topics[num_topics], token, MAX_TOPIC - 1);
            num_topics++;
            token = strtok_r(NULL, ",", &saveptr);
        }
    }
    
    if (num_topics == 0) {
        strcpy(topics[0], "metrics/docker/#");
        num_topics = 1;
    }
    
    if (argc >= 3) {
        strncpy(broker_host, argv[1], sizeof(broker_host) - 1);
        broker_port = atoi(argv[2]);
    }
    
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║          AKLight v2 - Consumer                               ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    printf("║  Consumer ID: %-45s ║\n", consumer_id);
    printf("║  Broker: %s:%-40d ║\n", broker_host, broker_port);
    printf("║  Persistent: %-47s ║\n", session_persistent ? "Sí" : "No");
    printf("║  Topics:                                                     ║\n");
    for (int i = 0; i < num_topics; i++) {
        printf("║    - %-55s ║\n", topics[i]);
    }
    printf("╚══════════════════════════════════════════════════════════════╝\n\n");
    fflush(stdout);
    
    setup_signals();
    init_thresholds();
    init_twilio();
    
    if (session_persistent) {
        load_offsets();
    }
    
    printf("[Consumer] Iniciando...\n\n");
    fflush(stdout);
    
    main_loop();
    
    if (session_persistent) {
        save_offsets();
    }
    
    if (broker_socket >= 0) {
        close(broker_socket);
    }
    
    printf("\n╔══════════════════════════════════════════════════════════════╗\n");
    printf("║  Estadísticas Finales                                        ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    printf("║  Mensajes recibidos: %-39ld ║\n", messages_received);
    printf("║  Alertas disparadas: %-39ld ║\n", alerts_triggered);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("[Consumer] 👋 Finalizado\n");
    
    return 0;
}
