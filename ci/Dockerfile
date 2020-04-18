FROM alpine:3.11.5 AS compressor

RUN apk add --no-cache upx
ADD fake-gcs-server /bin/fake-gcs-server
RUN upx --brute /bin/fake-gcs-server

FROM alpine:3.11.5

COPY --from=compressor /bin/fake-gcs-server /bin/fake-gcs-server
ENTRYPOINT ["/bin/fake-gcs-server", "-data", "/data"]
