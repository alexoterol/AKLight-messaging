# AKLight v2 - Stress Test Dockerfile
FROM alpine:3.19

RUN apk add --no-cache \
    gcc \
    musl-dev \
    linux-headers

WORKDIR /app

# Copiar código fuente
COPY stress/stress.c ./

# Compilar
RUN gcc -O2 -Wall -Wextra -pthread -o stress stress.c -lm

ENV STRESS_MODE=both
ENV STRESS_DURATION=60
ENV STRESS_INTENSITY=200

CMD ["./stress"]
