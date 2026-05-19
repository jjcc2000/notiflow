FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY .  .

RUN CGO_ENABLED=0 GOOS=linux  go build -o email-consumer ./consumers/email


FROM scratch

COPY --from=builder /app/email-consumer /email-consumer

EXPOSE 9090

ENTRYPOINT [ "/email-consumer" ]

