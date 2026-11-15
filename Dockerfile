FROM golang:1.23-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /faultlab ./cmd/faultlab
FROM alpine:3.21
COPY --from=build /faultlab /faultlab
RUN mkdir /data && chown 65534:65534 /data
USER 65534:65534
EXPOSE 8080
ENTRYPOINT ["/faultlab", "-journal", "/data/journal.jsonl"]
