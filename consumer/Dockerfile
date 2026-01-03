FROM gcc:latest

WORKDIR /app

COPY consumer.c .

RUN gcc -o consumer consumer.c -pthread -O2 -Wall && \
    echo "Consumer compiled successfully" && \
    ls -lh consumer

# Usar stdbuf para deshabilitar buffering
CMD ["sh", "-c", "echo '=== CONSUMER STARTING ===' && sleep 5 && echo 'Connecting to broker1:7000...' && stdbuf -oL -eL ./consumer consumer1 1 broker1 7000 'metrics/docker/producer1/#' 2>&1"]