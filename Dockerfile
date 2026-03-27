FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /airflowpulse ./cmd/airflowpulse

FROM alpine:3.21
RUN apk --no-cache add ca-certificates
COPY --from=builder /airflowpulse /usr/local/bin/airflowpulse
EXPOSE 9190
ENTRYPOINT ["airflowpulse"]
CMD ["serve"]
