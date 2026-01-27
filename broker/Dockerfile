# Broker Dockerfile
FROM alpine:3.19

RUN apk add --no-cache \
    gcc \
    musl-dev \
    linux-headers \
    netcat-openbsd

WORKDIR /app

COPY common/aklight_common.h ./common/
COPY broker/broker.c ./

RUN gcc -O2 -Wall -Wextra -pthread -o broker broker.c -lm

RUN mkdir -p /app/data

EXPOSE 7000 7001

ENV BROKER_ID=1

CMD ["./broker"]
