FROM alpine:latest

# Instalar dependencias
RUN apk add --no-cache gcc musl-dev make

# Directorio de trabajo
WORKDIR /app

# Copiar código fuente
COPY broker.c .

# Compilar
RUN gcc -o broker broker.c -pthread -Wall -Wextra && \
    mkdir -p data

# Puerto
EXPOSE 7000 7001

# Comando por defecto
CMD ["./broker", "1"]
