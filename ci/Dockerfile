FROM alpine:3.12.0 AS compressor

RUN apk add --no-cache upx
ADD fake-gcs-server /bin/fake-gcs-server
RUN upx --brute /bin/fake-gcs-server

FROM alpine:3.12.0

COPY --from=compressor /bin/fake-gcs-server /bin/fake-gcs-server
ENTRYPOINT ["/bin/fake-gcs-server", "-data", "/data"]
