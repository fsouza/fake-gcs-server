FROM alpine:3.13.2

ADD fake-gcs-server /bin/fake-gcs-server
ENTRYPOINT ["/bin/fake-gcs-server", "-data", "/data"]
