FROM gcc:latest

WORKDIR /app

COPY producer.c .

RUN gcc -o producer producer.c -pthread -O2 -Wall && \
    echo "Producer compiled successfully" && \
    ls -lh producer

# Usar stdbuf para deshabilitar buffering
CMD ["sh", "-c", "echo '=== PRODUCER STARTING ===' && sleep 3 && echo 'Connecting to broker1:7000...' && stdbuf -oL -eL ./producer producer1 metrics/docker broker1 7000 2>&1"]