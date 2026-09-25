FROM golang:1.24 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /liveness .

# alpine rather than scratch: the reproduction script injects faults with
# `kubectl exec ... wget`, which needs a shell and a HTTP client in the image.
FROM alpine:3.20
COPY --from=build /liveness /liveness
EXPOSE 8080 8081
ENTRYPOINT ["/liveness", "-serve"]
