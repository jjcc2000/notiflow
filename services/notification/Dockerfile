FROM golang:1.26-alpine AS BUILDER

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o notification ./services/notification

FROM scratch
COPY --from=builder /app/notification /notification
EXPOSE 8081
ENTRYPOINT [ "/notification" ]


