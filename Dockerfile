# Stage 1: Build the Go binary statically
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod files first to leverage Docker layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build a statically linked binary (critical for running in minimal images like alpine/scratch)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o benchmark main.go

# Stage 2: Create the minimal runtime image
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/benchmark .
COPY ./TimescaleDB_coding_assignment-RD_eng_setup/query_params.csv /app/query_params.csv

# (Optional but good practice) Add certificates in case we ever need to make HTTPS calls
RUN apk --no-cache add ca-certificates

# Ensure it's executable
RUN chmod +x ./benchmark

# The actual configuration (CONNECTION_STRING, flags) and the CSV file 
# will be passed at runtime via docker-compose.
ENTRYPOINT ["./benchmark"]