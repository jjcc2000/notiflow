FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum  ./ 

RUN go mod download 

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o subscription ./services/subscription


FROM scratch


COPY --from=builder /app/subscription/ /subscription
EXPOSE 8083
ENTRYPOINT [ "/subscription" ]
