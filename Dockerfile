FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

RUN go install github.com/bufbuild/buf/cmd/buf@latest

COPY . .

RUN buf generate

ARG TARGET=api
RUN CGO_ENABLED=0 go build -o /bin/service ./cmd/${TARGET}

FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY --from=builder /bin/service /bin/service
COPY --from=builder /app/migrations /app/migrations

ENTRYPOINT ["/bin/service"]
