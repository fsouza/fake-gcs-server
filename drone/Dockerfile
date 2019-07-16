# This Dockerfile is designed to be used in a CI environment and requires a
# binary called "gcs-emulator" to be present in the current folder.

FROM alpine:3.10.1
RUN apk add --no-cache ca-certificates mailcap
ADD gcs-emulator /bin/gcs-emulator
ENTRYPOINT ["/bin/gcs-emulator"]
