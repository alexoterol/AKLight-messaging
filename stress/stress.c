// =============================================================================
// AKLight v2 - Stress Test Program
// =============================================================================
// Programa para generar carga de CPU y memoria para probar thresholds
// 
// Modos:
//   cpu     - Carga intensiva de CPU (cálculos matemáticos)
//   memory  - Uso progresivo de memoria hasta el objetivo
//   both    - CPU + Memoria simultáneamente
//   spike   - Picos intermitentes (5s carga, 10s descanso)
//
// Uso: ./stress <mode> <duration_seconds> [intensity]
//   mode:      cpu, memory, both, spike
//   duration:  duración en segundos
//   intensity: para memory = MB objetivo (default 400)
//              para cpu = número de iteraciones por ciclo (default 1000000)
// =============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <math.h>
#include <pthread.h>

// =============================================================================
// CONSTANTES
// =============================================================================

#define DEFAULT_DURATION 60
#define DEFAULT_MEMORY_MB 400
#define DEFAULT_CPU_ITERATIONS 1000000
#define SPIKE_ON_DURATION 5
#define SPIKE_OFF_DURATION 10
#define ALLOCATION_CHUNK_MB 10

// =============================================================================
// VARIABLES GLOBALES
// =============================================================================

static volatile sig_atomic_t running = 1;
static pthread_mutex_t memory_mutex = PTHREAD_MUTEX_INITIALIZER;
static void **memory_blocks = NULL;
static int num_blocks = 0;
static int max_blocks = 0;

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
}

// =============================================================================
// FUNCIONES DE STRESS
// =============================================================================

// Carga de CPU: cálculos matemáticos intensivos
void cpu_stress(int iterations) {
    double result = 0.0;
    for (int i = 0; i < iterations && running; i++) {
        result += sin(i) * cos(i) * tan(i * 0.001);
        result = sqrt(fabs(result) + 1.0);
        result = log(result + 1.0) * exp(result * 0.0001);
    }
    // Usar result para evitar optimización
    if (result == 0.123456789) printf("!");
}

// Carga de memoria: asignar bloques progresivamente
int allocate_memory_mb(int target_mb) {
    pthread_mutex_lock(&memory_mutex);
    
    int current_mb = num_blocks * ALLOCATION_CHUNK_MB;
    
    while (current_mb < target_mb && running) {
        if (num_blocks >= max_blocks) {
            // Expandir array de bloques
            int new_max = max_blocks == 0 ? 100 : max_blocks * 2;
            void **new_blocks = realloc(memory_blocks, new_max * sizeof(void*));
            if (!new_blocks) {
                printf("[Stress] Error expandiendo array de bloques\n");
                break;
            }
            memory_blocks = new_blocks;
            max_blocks = new_max;
        }
        
        // Asignar bloque de ALLOCATION_CHUNK_MB
        size_t chunk_size = ALLOCATION_CHUNK_MB * 1024 * 1024;
        void *block = malloc(chunk_size);
        if (!block) {
            printf("[Stress] Error asignando memoria en bloque %d\n", num_blocks);
            break;
        }
        
        // Escribir datos para asegurar que la memoria se asigna realmente
        memset(block, 0xAA, chunk_size);
        
        memory_blocks[num_blocks++] = block;
        current_mb = num_blocks * ALLOCATION_CHUNK_MB;
        
        printf("[Stress] Memoria asignada: %d MB / %d MB objetivo\n", 
               current_mb, target_mb);
    }
    
    pthread_mutex_unlock(&memory_mutex);
    return current_mb;
}

// Liberar toda la memoria
void free_all_memory(void) {
    pthread_mutex_lock(&memory_mutex);
    
    for (int i = 0; i < num_blocks; i++) {
        free(memory_blocks[i]);
    }
    
    free(memory_blocks);
    memory_blocks = NULL;
    num_blocks = 0;
    max_blocks = 0;
    
    pthread_mutex_unlock(&memory_mutex);
    printf("[Stress] Toda la memoria liberada\n");
}

// =============================================================================
// MODOS DE STRESS
// =============================================================================

void run_cpu_stress(int duration, int iterations) {
    printf("[Stress] Modo CPU - Duración: %d segundos, Iteraciones: %d\n",
           duration, iterations);
    
    time_t start = time(NULL);
    long cycles = 0;
    
    while (running && (time(NULL) - start) < duration) {
        cpu_stress(iterations);
        cycles++;
        
        if (cycles % 100 == 0) {
            printf("[Stress] CPU ciclos completados: %ld (%.0f segundos)\n",
                   cycles, difftime(time(NULL), start));
        }
    }
    
    printf("[Stress] CPU completado - Total ciclos: %ld\n", cycles);
}

void run_memory_stress(int duration, int target_mb) {
    printf("[Stress] Modo Memory - Duración: %d segundos, Objetivo: %d MB\n",
           duration, target_mb);
    
    time_t start = time(NULL);
    
    // Asignar memoria progresivamente
    int current_mb = 0;
    while (running && (time(NULL) - start) < duration && current_mb < target_mb) {
        current_mb = allocate_memory_mb(current_mb + ALLOCATION_CHUNK_MB);
        sleep(1);  // Pausa entre asignaciones
    }
    
    printf("[Stress] Memoria objetivo alcanzada: %d MB\n", current_mb);
    
    // Mantener la memoria asignada hasta el final
    while (running && (time(NULL) - start) < duration) {
        // Acceder a la memoria periódicamente para mantenerla activa
        pthread_mutex_lock(&memory_mutex);
        for (int i = 0; i < num_blocks; i++) {
            volatile char *p = (volatile char*)memory_blocks[i];
            *p = *p + 1;  // Acceso para evitar swap
        }
        pthread_mutex_unlock(&memory_mutex);
        
        sleep(2);
        printf("[Stress] Manteniendo %d MB (%.0f segundos restantes)\n",
               current_mb, duration - difftime(time(NULL), start));
    }
    
    free_all_memory();
}

void run_both_stress(int duration, int target_mb, int cpu_iterations) {
    printf("[Stress] Modo Both - Duración: %d segundos\n", duration);
    printf("         Memory objetivo: %d MB\n", target_mb);
    printf("         CPU iteraciones: %d\n", cpu_iterations);
    
    time_t start = time(NULL);
    
    // Asignar memoria
    allocate_memory_mb(target_mb);
    
    long cycles = 0;
    while (running && (time(NULL) - start) < duration) {
        cpu_stress(cpu_iterations);
        cycles++;
        
        // Acceder a la memoria
        pthread_mutex_lock(&memory_mutex);
        for (int i = 0; i < num_blocks && i < 10; i++) {  // Solo algunos bloques
            volatile char *p = (volatile char*)memory_blocks[i];
            *p = *p + 1;
        }
        pthread_mutex_unlock(&memory_mutex);
        
        if (cycles % 50 == 0) {
            printf("[Stress] Both - CPU ciclos: %ld, Memoria: %d MB\n",
                   cycles, num_blocks * ALLOCATION_CHUNK_MB);
        }
    }
    
    free_all_memory();
    printf("[Stress] Both completado - CPU ciclos: %ld\n", cycles);
}

void run_spike_stress(int duration, int target_mb, int cpu_iterations) {
    printf("[Stress] Modo Spike - Duración: %d segundos\n", duration);
    printf("         On: %d segundos, Off: %d segundos\n",
           SPIKE_ON_DURATION, SPIKE_OFF_DURATION);
    
    time_t start = time(NULL);
    int spike_count = 0;
    
    while (running && (time(NULL) - start) < duration) {
        // SPIKE ON
        printf("[Stress] === SPIKE #%d ON ===\n", ++spike_count);
        
        // Asignar memoria
        allocate_memory_mb(target_mb);
        
        time_t spike_start = time(NULL);
        while (running && (time(NULL) - spike_start) < SPIKE_ON_DURATION) {
            cpu_stress(cpu_iterations);
        }
        
        // SPIKE OFF
        printf("[Stress] === SPIKE #%d OFF ===\n", spike_count);
        free_all_memory();
        
        time_t rest_start = time(NULL);
        while (running && (time(NULL) - rest_start) < SPIKE_OFF_DURATION) {
            sleep(1);
        }
    }
    
    free_all_memory();
    printf("[Stress] Spike completado - Total spikes: %d\n", spike_count);
}

// =============================================================================
// MAIN
// =============================================================================

void print_usage(const char *prog) {
    printf("Uso: %s <mode> <duration> [intensity]\n", prog);
    printf("\n");
    printf("Modos:\n");
    printf("  cpu     - Carga intensiva de CPU\n");
    printf("  memory  - Uso progresivo de memoria\n");
    printf("  both    - CPU + Memoria simultáneamente\n");
    printf("  spike   - Picos intermitentes\n");
    printf("\n");
    printf("Parámetros:\n");
    printf("  duration  - Duración en segundos\n");
    printf("  intensity - Para memory: MB objetivo (default %d)\n", DEFAULT_MEMORY_MB);
    printf("              Para cpu: iteraciones por ciclo (default %d)\n", DEFAULT_CPU_ITERATIONS);
    printf("\n");
    printf("Ejemplos:\n");
    printf("  %s cpu 60           # 60 segundos de carga CPU\n", prog);
    printf("  %s memory 120 512   # 2 minutos, 512 MB\n", prog);
    printf("  %s both 180 400     # 3 minutos, 400 MB + CPU\n", prog);
    printf("  %s spike 300 600    # 5 minutos de picos\n", prog);
}

int main(int argc, char *argv[]) {
    char mode[32] = "both";
    int duration = DEFAULT_DURATION;
    int intensity = 0;  // 0 = usar default según modo
    
    // Variables de entorno (para Docker)
    char *env;
    if ((env = getenv("STRESS_MODE")) != NULL) {
        strncpy(mode, env, sizeof(mode) - 1);
    }
    if ((env = getenv("STRESS_DURATION")) != NULL) {
        duration = atoi(env);
    }
    if ((env = getenv("STRESS_INTENSITY")) != NULL) {
        intensity = atoi(env);
    }
    
    // Argumentos de línea de comandos (sobrescriben env vars)
    if (argc >= 2) {
        if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
        strncpy(mode, argv[1], sizeof(mode) - 1);
    }
    if (argc >= 3) {
        duration = atoi(argv[2]);
    }
    if (argc >= 4) {
        intensity = atoi(argv[3]);
    }
    
    // Defaults según modo
    int target_mb = (intensity > 0) ? intensity : DEFAULT_MEMORY_MB;
    int cpu_iterations = (intensity > 0) ? intensity : DEFAULT_CPU_ITERATIONS;
    
    printf("=============================================================\n");
    printf("AKLight v2 - Stress Test Program\n");
    printf("=============================================================\n");
    printf("Modo: %s\n", mode);
    printf("Duración: %d segundos\n", duration);
    printf("=============================================================\n");
    
    setup_signals();
    
    if (strcmp(mode, "cpu") == 0) {
        run_cpu_stress(duration, cpu_iterations);
    } else if (strcmp(mode, "memory") == 0) {
        run_memory_stress(duration, target_mb);
    } else if (strcmp(mode, "both") == 0) {
        run_both_stress(duration, target_mb, cpu_iterations);
    } else if (strcmp(mode, "spike") == 0) {
        run_spike_stress(duration, target_mb, cpu_iterations);
    } else {
        fprintf(stderr, "Modo desconocido: %s\n", mode);
        print_usage(argv[0]);
        return 1;
    }
    
    printf("=============================================================\n");
    printf("[Stress] Finalizado\n");
    return 0;
}
