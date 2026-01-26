# AKLight v2 - Broker Dockerfile
FROM alpine:3.19

RUN apk add --no-cache \
    gcc \
    musl-dev \
    linux-headers \
    netcat-openbsd

WORKDIR /app

# Copiar código fuente
COPY common/aklight_common.h ./common/
COPY broker/broker.c ./

# Compilar
RUN gcc -O2 -Wall -Wextra -pthread -o broker broker.c -lm

# Crear directorio de datos
RUN mkdir -p /app/data

EXPOSE 7000 7001

ENV BROKER_ID=1

CMD ["./broker"]
