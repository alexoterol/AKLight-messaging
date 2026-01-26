# AKLight v2 - Producer Dockerfile
FROM alpine:3.19

RUN apk add --no-cache \
    gcc \
    musl-dev \
    linux-headers \
    procps

WORKDIR /app

# Copiar código fuente
COPY common/aklight_common.h ./common/
COPY producer/producer.c ./

# Compilar
RUN gcc -O2 -Wall -Wextra -pthread -o producer producer.c -lm

ENV PRODUCER_ID=producer1
ENV BASE_TOPIC=metrics/docker
ENV BROKER_HOST=broker1
ENV BROKER_PORT=7000
ENV INTERVAL=5
ENV USE_KEY=1

CMD ["./producer"]
