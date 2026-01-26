# AKLight v2 - Sistema de Mensajería Distribuida

Sistema de mensajería distribuida inspirado en Apache Kafka para monitoreo de contenedores Docker.

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AKLight v2 Architecture                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐     ┌─────────────┐                                       │
│  │  Producer1  │     │  Producer2  │     Recolección de métricas           │
│  │  (fork x3)  │     │  (fork x3)  │     - CPU (hijo 1)                    │
│  └──────┬──────┘     └──────┬──────┘     - Memory (hijo 2)                 │
│         │                   │            - Disk, Network (padre)            │
│         ▼                   ▼                                               │
│  ┌─────────────┐     ┌─────────────┐                                       │
│  │   Broker1   │◄───►│   Broker2   │     Clúster con sincronización        │
│  │  Port 7000  │     │  Port 7001  │                                       │
│  │ ┌─────────┐ │     │ ┌─────────┐ │                                       │
│  │ │  P0 P1  │ │     │ │  P0 P1  │ │     2 particiones por broker          │
│  │ └─────────┘ │     │ └─────────┘ │                                       │
│  └──────┬──────┘     └─────────────┘                                       │
│         │                                                                   │
│         ▼                                                                   │
│  ┌─────────────────────────────────┐                                       │
│  │          Consumer1              │     Suscripción: metrics/docker/#     │
│  │  - Wildcard matching (#, +)     │     Sesión persistente                │
│  │  - Thresholds + Alertas         │     WhatsApp via Twilio               │
│  │  - Sesión persistente           │                                       │
│  └─────────────────────────────────┘                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Componentes

### Broker (broker/broker.c)
- Servidor de mensajería con soporte para clúster
- 2 particiones por broker (hash-based y round-robin)
- Persistencia de mensajes en archivos de log
- Sincronización entre brokers del clúster

### Producer (producer/producer.c)
- **Implementación con fork()** (requisito obligatorio):
  - Proceso padre: Gestiona conexión y envío
  - Proceso hijo 1: Recolecta métricas de CPU
  - Proceso hijo 2: Recolecta métricas de memoria
  - Comunicación: Pipes unidireccionales (O_NONBLOCK)
- Recolecta 4 métricas: CPU, Memory, Disk, Network
- Topics de 4 niveles: `metrics/docker/{producer_id}/{metric}`

### Consumer (consumer/consumer.c)
- Suscripción con wildcards (`#` y `+`)
- Sesiones persistentes/no-persistentes
- Sistema de thresholds con alertas
- Integración Twilio para WhatsApp

### Stress (stress/stress.c)
- Modos: cpu, memory, both, spike
- Para probar thresholds y alertas

---

## TDAs Implementados

### 1. Message Queue (Cola de Mensajes)
```c
typedef struct message_node {
    Message message;
    struct message_node *next;
} MessageNode;

typedef struct {
    MessageNode *head;
    MessageNode *tail;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
} MessageQueue;
```
- **Estructura**: Lista enlazada simple con punteros head/tail
- **Complejidad**: O(1) para enqueue y dequeue
- **Sincronización**: Mutex + Condition Variable

### 2. Partition Hash Table (Tabla Hash de Particiones)
```c
typedef struct partition_entry {
    char key[MAX_KEY_SIZE];
    int partition_id;
    struct partition_entry *next;
} PartitionEntry;

typedef struct {
    PartitionEntry *buckets[HASH_TABLE_SIZE];
    int count;
    pthread_rwlock_t rwlock;
} PartitionTable;
```
- **Estructura**: Tabla hash con encadenamiento
- **Función hash**: DJB2
- **Complejidad**: O(1) promedio, O(n) peor caso
- **Sincronización**: Read-Write Lock

### 3. Ring Buffer (Buffer Circular)
```c
typedef struct {
    LogEntry entries[RING_BUFFER_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
} RingBuffer;
```
- **Estructura**: Array circular de tamaño fijo
- **Complejidad**: O(1) para todas las operaciones
- **Uso**: Logs recientes para debugging

### 4. Topic Tree (Árbol de Topics)
```c
typedef struct topic_node {
    char segment[MAX_SEGMENT];
    struct topic_node *children[MAX_CHILDREN];
    int child_count;
    int is_terminal;
    MessageQueue *queue;
} TopicNode;
```
- **Estructura**: Árbol N-ario
- **Soporte**: Wildcards (#, +)
- **Complejidad**: O(d) donde d = profundidad del topic

---

## Mecanismos de Sincronización

### 1. Mutex (pthread_mutex_t)
```c
// Ejemplo: Proteger lista de clientes
pthread_mutex_lock(&clients_mutex);
add_client(client);
pthread_mutex_unlock(&clients_mutex);
```
**Uso en el proyecto**:
- `clients_mutex`: Lista de clientes conectados
- `partition.mutex`: Operaciones de partición
- `offset_mutex`: Tabla de offsets del consumer
- `memory_mutex`: Bloques de memoria en stress test

### 2. Read-Write Lock (pthread_rwlock_t)
```c
// Lectura (múltiples lectores)
pthread_rwlock_rdlock(&partition_rwlock);
int partition = lookup_partition(key);
pthread_rwlock_unlock(&partition_rwlock);

// Escritura (exclusiva)
pthread_rwlock_wrlock(&partition_rwlock);
update_partition(key, new_partition);
pthread_rwlock_unlock(&partition_rwlock);
```
**Uso**: Tabla de particiones (lectura frecuente, escritura infrecuente)

### 3. Semáforo (sem_t)
```c
// Limitar conexiones concurrentes
sem_t connection_semaphore;
sem_init(&connection_semaphore, 0, MAX_CONNECTIONS);

// Al aceptar conexión
if (sem_trywait(&connection_semaphore) == 0) {
    accept_connection();
} else {
    reject_connection();
}

// Al cerrar conexión
sem_post(&connection_semaphore);
```
**Uso**: Control de conexiones máximas al broker

### 4. Condition Variable (pthread_cond_t)
```c
// Consumer esperando mensajes
pthread_mutex_lock(&queue->mutex);
while (queue->count == 0) {
    pthread_cond_wait(&queue->not_empty, &queue->mutex);
}
Message msg = dequeue(queue);
pthread_mutex_unlock(&queue->mutex);

// Producer notificando
pthread_mutex_lock(&queue->mutex);
enqueue(queue, message);
pthread_cond_signal(&queue->not_empty);
pthread_mutex_unlock(&queue->mutex);
```
**Uso**: Cola de mensajes (bloqueo eficiente sin busy-wait)

---

## Protocolo de Comunicación

### Formato de Mensajes
```
COMMAND|PARAM1|PARAM2|...\n
```

### Comandos
| Comando | Formato | Descripción |
|---------|---------|-------------|
| REGISTER_PRODUCER | `REGISTER_PRODUCER\|id` | Registrar producer |
| REGISTER_CONSUMER | `REGISTER_CONSUMER\|id\|persistent` | Registrar consumer |
| PRODUCE | `PRODUCE\|topic\|key\|payload` | Enviar mensaje |
| SUBSCRIBE | `SUBSCRIBE\|pattern` | Suscribirse a topic |
| FETCH | `FETCH\|pattern\|offset` | Solicitar mensajes |
| ACK | `ACK\|topic\|offset\|partition` | Confirmar recepción |
| MESSAGE | `MESSAGE\|topic\|offset\|partition\|key\|payload` | Mensaje del broker |

---

## Diagramas

### Diagrama de Secuencia - Flujo de Mensaje
```
Producer          Broker              Consumer
   │                 │                    │
   │ REGISTER_PRODUCER                    │
   │────────────────>│                    │
   │      OK         │                    │
   │<────────────────│                    │
   │                 │   REGISTER_CONSUMER│
   │                 │<───────────────────│
   │                 │         OK         │
   │                 │───────────────────>│
   │                 │                    │
   │ PRODUCE|topic|key|payload            │
   │────────────────>│                    │
   │    ACK          │                    │
   │<────────────────│                    │
   │                 │      FETCH         │
   │                 │<───────────────────│
   │                 │      MESSAGE       │
   │                 │───────────────────>│
   │                 │        ACK         │
   │                 │<───────────────────│
```

### Diagrama de Estados - Consumer
```
┌─────────────┐
│ DISCONNECTED│
└──────┬──────┘
       │ connect()
       ▼
┌─────────────┐
│ CONNECTING  │
└──────┬──────┘
       │ registered
       ▼
┌─────────────┐   subscribe()   ┌─────────────┐
│  CONNECTED  │────────────────>│ SUBSCRIBED  │
└─────────────┘                 └──────┬──────┘
       ▲                               │
       │ reconnect()                   │ message
       │                               ▼
┌─────────────┐                 ┌─────────────┐
│   ERROR     │<────────────────│ PROCESSING  │
└─────────────┘   error         └─────────────┘
```

### Diagrama de Clases - TDAs
```
┌────────────────────┐     ┌────────────────────┐
│    MessageQueue    │     │   PartitionTable   │
├────────────────────┤     ├────────────────────┤
│ - head: Node*      │     │ - buckets[256]     │
│ - tail: Node*      │     │ - count: int       │
│ - count: int       │     │ - rwlock           │
│ - mutex            │     ├────────────────────┤
│ - not_empty        │     │ + lookup(key)      │
├────────────────────┤     │ + insert(key, pid) │
│ + enqueue(msg)     │     │ + remove(key)      │
│ + dequeue(): msg   │     └────────────────────┘
│ + peek(): msg      │
└────────────────────┘     ┌────────────────────┐
                           │     RingBuffer     │
┌────────────────────┐     ├────────────────────┤
│     TopicTree      │     │ - entries[1024]    │
├────────────────────┤     │ - head, tail       │
│ - root: TopicNode* │     │ - mutex            │
├────────────────────┤     ├────────────────────┤
│ + insert(topic)    │     │ + push(entry)      │
│ + match(pattern)   │     │ + get_recent(n)    │
│ + find(topic)      │     └────────────────────┘
└────────────────────┘
```

---

## Despliegue

### Iniciar Sistema Completo
```bash
cd /home/claude/aklight_v2
docker-compose up --build -d
```

### Ver Logs
```bash
# Todos los servicios
docker-compose logs -f

# Solo consumer
docker-compose logs -f consumer1

# Solo brokers
docker-compose logs -f broker1 broker2
```

### Ejecutar Stress Test
```bash
docker-compose --profile stress up stress1
```

### Detener Todo
```bash
docker-compose down -v
```

---

## Pruebas

### Test 1: Flujo Básico
```bash
# Verificar que los mensajes fluyen
docker-compose logs -f consumer1
# Debe mostrar mensajes de metrics/docker/producer1/* y producer2/*
```

### Test 2: Topics N-Niveles
```bash
# Verificar topics de 4 niveles
docker-compose exec consumer1 cat /app/offsets/consumer1.offsets
# Debe mostrar: metrics/docker/producer1/cpu, etc.
```

### Test 3: Wildcard #
```bash
# Consumer suscrito a metrics/docker/# debe recibir:
# - metrics/docker/producer1/cpu
# - metrics/docker/producer1/memory
# - metrics/docker/producer2/disk
# etc.
```

### Test 4: Sesión Persistente
```bash
# Reiniciar consumer y verificar que continúa desde último offset
docker-compose restart consumer1
docker-compose logs -f consumer1
# No debe re-procesar mensajes antiguos
```

### Test 5: Particionamiento
```bash
# Verificar distribución en particiones
docker-compose exec broker1 ls -la /app/data/
# Debe mostrar partition_0.log y partition_1.log
```

### Test 6: Fork (3 Procesos)
```bash
# Verificar que producer tiene 3 procesos
docker-compose exec producer1 ps aux
# Debe mostrar: 1 proceso padre + 2 hijos (cpu_collector, mem_collector)
```

### Test 7: Thresholds
```bash
# Ejecutar stress para disparar alertas
docker-compose --profile stress up stress1
docker-compose logs -f consumer1
# Debe mostrar: *** THRESHOLD EXCEEDED ***
```

### Test 8: Stress Test
```bash
# Diferentes modos
docker-compose exec stress1 ./stress cpu 30
docker-compose exec stress1 ./stress memory 30 600
docker-compose exec stress1 ./stress spike 60
```

### Test 9: Reconexión
```bash
# Reiniciar broker y verificar reconexión automática
docker-compose restart broker1
docker-compose logs -f producer1
# Debe reconectar automáticamente
```

### Test 10: WhatsApp (opcional)
```bash
# Configurar variables de Twilio en docker-compose.yml
# Ejecutar stress y verificar alerta WhatsApp
```

---

## Configuración Twilio (WhatsApp)

1. Crear cuenta en [Twilio](https://www.twilio.com)
2. Activar WhatsApp Sandbox
3. Configurar variables en `docker-compose.yml`:
```yaml
environment:
  - TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxx
  - TWILIO_AUTH_TOKEN=your_auth_token
  - TWILIO_FROM_NUMBER=whatsapp:+14155238886
  - TWILIO_TO_NUMBER=whatsapp:+1234567890
```

---

## Estructura de Archivos

```
aklight_v2/
├── common/
│   └── aklight_common.h      # Header compartido con TDAs
├── broker/
│   ├── broker.c              # Broker con clúster
│   └── Dockerfile
├── producer/
│   ├── producer.c            # Producer con fork()
│   └── Dockerfile
├── consumer/
│   ├── consumer.c            # Consumer con thresholds
│   └── Dockerfile
├── stress/
│   ├── stress.c              # Generador de carga
│   └── Dockerfile
├── docker-compose.yml        # Configuración de despliegue
└── README.md                 # Esta documentación
```

---

## Rúbrica de Evaluación

| Requisito | Puntos | Estado |
|-----------|--------|--------|
| Arquitectura (Broker/Cluster/Producer/Consumer) | 10 | ✅ |
| 2+ Métricas por Producer | 5 | ✅ (4 métricas) |
| Fork para Recolección | 5 | ✅ |
| TDAs (4 estructuras) | 15 | ✅ |
| Sincronización (4 mecanismos) | 20 | ✅ |
| Topics N-Niveles | 5 | ✅ (4 niveles) |
| Sesiones Persistentes/No-Persistentes | 5 | ✅ |
| Thresholds + WhatsApp | 5 | ✅ |
| Programa Stress | 5 | ✅ |
| Documentación | 5 | ✅ |
| Diagramas | 10 | ✅ |
| Video Demo | 5 | ⏳ |
| Video Explicación | 5 | ⏳ |
| **TOTAL** | **100** | **90/100** |

---

## Autores

Proyecto académico - Sistema Operativos
