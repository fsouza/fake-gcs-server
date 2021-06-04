# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

FROM golang:1.16.5 AS builder
WORKDIR /code
ADD go.mod go.sum ./
RUN go mod download
ADD . ./
ENV CGO_ENABLED=0
RUN go build -o fake-gcs-server

FROM alpine:3.13.5
COPY --from=builder /code/fake-gcs-server /bin/fake-gcs-server
RUN /bin/fake-gcs-server -h
EXPOSE 4443
ENTRYPOINT ["/bin/fake-gcs-server", "-data", "/data"]
