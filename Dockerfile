FROM golang:1.21.0-bullseye as builder
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o redis-migrator

FROM ubuntu:20.04
RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/redis-migrator .
ENTRYPOINT ["./redis-migrator"]