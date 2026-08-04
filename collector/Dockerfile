FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/collector ./cmd/collector

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/collector /collector
EXPOSE 4317 4318
ENTRYPOINT ["/collector"]
