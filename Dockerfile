FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGET=api
RUN CGO_ENABLED=0 go build -o /bin/service ./cmd/${TARGET}

FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY --from=builder /bin/service /bin/service
COPY --from=builder /app/migrations /app/migrations

ENTRYPOINT ["/bin/service"]
