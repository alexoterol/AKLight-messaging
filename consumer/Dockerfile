FROM alpine:latest

RUN apk add --no-cache gcc musl-dev

WORKDIR /app

COPY consumer.c .

RUN gcc -o consumer consumer.c -pthread -Wall -Wextra

CMD ["./consumer", "consumer1", "1"]
