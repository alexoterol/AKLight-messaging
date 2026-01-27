# AKLight - Consumer Dockerfile
FROM alpine:3.19

RUN apk add --no-cache \
    gcc \
    musl-dev \
    linux-headers \
    curl

WORKDIR /app

COPY common/aklight_common.h ./common/
COPY consumer/consumer.c ./

RUN gcc -O2 -Wall -Wextra -pthread -o consumer consumer.c -lm

RUN mkdir -p /app/offsets

ENV CONSUMER_ID=consumer1
ENV SESSION_PERSISTENT=1
ENV BROKER_HOST=broker1
ENV BROKER_PORT=7000
ENV TOPICS=metrics/docker/#
ENV CPU_THRESHOLD=80.0
ENV MEM_THRESHOLD=500

CMD ["./consumer"]
