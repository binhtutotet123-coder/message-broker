# syntax=docker/dockerfile:1

# Build stage: compile the broker and the two mock services as static binaries.
ARG GO_VERSION=1.27
FROM golang:${GO_VERSION}-alpine AS build
# Official Go images set GOTOOLCHAIN=local; "auto" lets Go fetch the exact version in go.mod
# if the image is older.
ENV GOTOOLCHAIN=auto CGO_ENABLED=0
WORKDIR /src
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/broker . \
 && go build -trimpath -ldflags="-s -w" -o /out/mock-agent-b ./cmd/mock-agent-b \
 && go build -trimpath -ldflags="-s -w" -o /out/mock-registry ./cmd/mock-registry

# Runtime stage: small image; busybox wget is used by the compose health check.
FROM alpine:3.22
RUN adduser -D -H -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
EXPOSE 8080 8081 8082
ENTRYPOINT ["broker"]
