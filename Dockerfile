# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

FROM golang:1.14.7 AS tester
WORKDIR /code
ADD go.mod go.sum ./
RUN go mod download
ADD . ./
RUN go test -race -vet all -mod readonly ./...

FROM golangci/golangci-lint AS linter
WORKDIR /code
ENV GOPROXY=off
COPY --from=tester /go/pkg /go/pkg
COPY --from=tester /code .
RUN golangci-lint run \
	&& rm -rf /root/.cache

FROM golang:1.14.7 AS builder
WORKDIR /code
ENV CGO_ENABLED=0 GOPROXY=off
COPY --from=tester /go/pkg /go/pkg
COPY --from=tester /code .
RUN go build -o fake-gcs-server

FROM alpine:3.12.0
COPY --from=builder /code/fake-gcs-server /bin/fake-gcs-server
RUN /bin/fake-gcs-server -h
EXPOSE 4443
ENTRYPOINT ["/bin/fake-gcs-server", "-data", "/data"]
