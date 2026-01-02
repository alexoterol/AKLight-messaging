FROM gcc:latest

WORKDIR /app

COPY producer.c .

RUN gcc -o producer producer.c -pthread -O2

CMD ["./producer", "producer1", "metrics/docker", "broker1", "7000"]