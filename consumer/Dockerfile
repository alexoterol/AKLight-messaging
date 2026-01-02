FROM gcc:latest

WORKDIR /app

COPY consumer.c .

RUN gcc -o consumer consumer.c -pthread -O2

CMD ["./consumer", "consumer1", "1", "broker1", "7000", "metrics/docker/#"]