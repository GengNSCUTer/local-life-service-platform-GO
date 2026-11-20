FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /out/hmdp-api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath -o /out/hmdp-worker ./cmd/worker

FROM scratch AS api
COPY --from=build /out/hmdp-api /hmdp-api
USER 65532:65532
ENTRYPOINT ["/hmdp-api"]

FROM scratch AS worker
COPY --from=build /out/hmdp-worker /hmdp-worker
USER 65532:65532
ENTRYPOINT ["/hmdp-worker"]
