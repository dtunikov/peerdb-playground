FROM golang:1.26.1 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

RUN go install github.com/bufbuild/buf/cmd/buf@v1.68.2

COPY . .

RUN buf generate

RUN CGO_ENABLED=0 go build -o /bin/service ./cmd/peerdb-playground

FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY --from=builder /bin/service /bin/service
COPY --from=builder /app/migrations /app/migrations

ENTRYPOINT ["/bin/service"]
