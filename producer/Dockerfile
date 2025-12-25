FROM alpine:latest

RUN apk add --no-cache gcc musl-dev

WORKDIR /app

COPY producer.c .

RUN gcc -o producer producer.c -pthread -Wall -Wextra

CMD ["./producer", "producer1", "metrics/docker"]
