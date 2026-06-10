FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/profit-worker ./cmd/profit-worker

FROM alpine:3.20
RUN adduser -D -H app
USER app
COPY --from=build /out/profit-worker /profit-worker
EXPOSE 8080
ENTRYPOINT ["/profit-worker"]
