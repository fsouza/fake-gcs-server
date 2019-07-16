FROM golang:1.12-stretch as builder
MAINTAINER Matteo Scandolo <teo.punto@gmail.com>

WORKDIR /go
ENV GO111MODULE=on
ADD . /go/src/github.com/fsouza/fake-gcs-server
RUN CGO_ENABLED=0 GOOS=linux go build -o /build/go-fake-storage /go/src/github.com/fsouza/fake-gcs-server/main.go

FROM alpine:3.5
MAINTAINER Matteo Scandolo <teo.punto@gmail.com>

COPY --from=builder /build/go-fake-storage /service/go-fake-storage

EXPOSE 4443

WORKDIR /service
ENTRYPOINT ["/service/go-fake-storage"]