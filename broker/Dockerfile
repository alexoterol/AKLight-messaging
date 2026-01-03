FROM gcc:latest

WORKDIR /app

COPY broker.c .

RUN gcc -o broker broker.c -pthread -O2 -Wall && \
    echo "Broker compiled successfully" && \
    ls -lh broker

# Usar stdbuf para deshabilitar buffering
CMD ["sh", "-c", "echo '=== BROKER STARTING ===' && echo 'ID: 1' && echo 'Port: 7000' && stdbuf -oL -eL ./broker 1 2>&1"]