# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

FROM golang:1.13beta1-alpine AS builder

ARG GOPROXY=https://proxy.golang.org

WORKDIR /code
ENV CGO_ENABLED=0
ADD go.mod go.sum ./
RUN go mod download
ADD . ./
RUN go build -o fake-gcs-server

FROM alpine:3.10.1
COPY --from=builder /code/fake-gcs-server /bin/fake-gcs-server
ENTRYPOINT ["/bin/fake-gcs-server"]
