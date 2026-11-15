FROM golang:1.26-alpine AS build
WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/ping-service ./main.go

FROM alpine:3.22 AS final
WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata curl && apk upgrade --no-cache

COPY --from=build /app/ping-service /app/ping-service

RUN addgroup -g 10001 app && adduser -D -u 10001 -G app app
USER 10001

ENTRYPOINT ["/app/ping-service"]
