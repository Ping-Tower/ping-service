FROM golang:1.26.1-alpine AS build
WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/ping-service ./main.go

FROM alpine:3.22 AS final
WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata curl

COPY --from=build /app/ping-service /app/ping-service

ENTRYPOINT ["/app/ping-service"]
