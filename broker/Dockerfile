FROM gcc:latest

WORKDIR /app

COPY broker.c .

RUN gcc -o broker broker.c -pthread -O2

CMD ["./broker", "1"]