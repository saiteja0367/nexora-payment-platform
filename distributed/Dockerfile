FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
ARG SERVICE=api
RUN CGO_ENABLED=0 go build -o /app ./cmd/${SERVICE}
FROM alpine:3.21
COPY --from=build /app /app
USER 65534
ENTRYPOINT ["/app"]
