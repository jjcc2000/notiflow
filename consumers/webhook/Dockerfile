FROM golang:1.26-alpine AS builder 

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux  go build -o webhook-consumer ./consumers/webhook


FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder  /app/webhook-consumer /webhook-consumer

EXPOSE  9091

ENTRYPOINT [ "/webhook-consumer" ]