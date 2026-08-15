# Build: static Go binary → distroless. No CGO.
FROM golang:1.25 AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /databricks-emulator ./cmd/databricks-emulator
RUN mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /databricks-emulator /usr/local/bin/databricks-emulator
COPY --from=build --chown=65532:65532 /data /data
ENV DATABRICKS_DATA_DIR=/data
VOLUME /data
EXPOSE 8447
HEALTHCHECK --interval=10s --timeout=3s --retries=5 CMD ["/usr/local/bin/databricks-emulator", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/databricks-emulator"]
