FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o server ./cmd/server/
RUN go build -o kvctl ./cmd/kvctl/

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/server .
COPY --from=builder /app/kvctl .
EXPOSE 7001 7002 7003 8001 8002 8003
CMD ["/app/server"]
