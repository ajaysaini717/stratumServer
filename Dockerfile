# ---- Build Stage ----
FROM golang:1.24.2-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git make gcc musl-dev linux-headers

# Copy source code
COPY . .

# Build the Go binary
RUN go build -o stratumServer .

# ---- Run Stage ----
FROM alpine:3.20

WORKDIR /app

# Install bash and Python3
RUN apk add --no-cache bash python3

# Copy built binary from builder
COPY --from=builder /app/stratumServer .

# Copy index.html (and any other frontend files in root)
COPY index.html /app/index.html

# Expose needed ports
EXPOSE 3334 8080 8083

# Start both processes using shell
CMD bash -c "./stratumServer & python3 -m http.server 8080"
