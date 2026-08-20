ARG VERSION=dev
ARG REVISION=unknown
ARG CREATED=unknown

FROM golang:1.25-alpine AS builder

WORKDIR /src

# Keep dependency downloads cached when only application source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/flowstitch ./cmd/flowstitch

FROM alpine:3.24

ARG VERSION
ARG REVISION
ARG CREATED

LABEL org.opencontainers.image.title="FlowStitch" \
    org.opencontainers.image.description="Durable event correlation for OpenSearch" \
    org.opencontainers.image.source="https://github.com/kraicdesign/flow-stitch" \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.revision="${REVISION}" \
    org.opencontainers.image.created="${CREATED}"

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 flowstitch \
    && adduser -S -D -H -u 10001 -G flowstitch flowstitch \
    && mkdir -p /var/lib/flowstitch \
    && chown flowstitch:flowstitch /var/lib/flowstitch

COPY --from=builder /out/flowstitch /usr/local/bin/flowstitch
COPY config/flowstitch.container.yaml /etc/flowstitch/flowstitch.yaml

USER flowstitch
# Prevent an unmounted run from silently storing durable state in the container layer.
VOLUME ["/var/lib/flowstitch"]
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=2m --retries=3 \
    CMD wget -q -O - http://127.0.0.1:8080/health/ready >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/flowstitch"]
CMD ["-config", "/etc/flowstitch/flowstitch.yaml"]
