FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/writer ./cmd/writer

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/writer /writer
EXPOSE 8080
ENTRYPOINT ["/writer"]
