# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

FROM golang:1.12-stretch as builder
MAINTAINER Matteo Scandolo <teo.punto@gmail.com>

WORKDIR /code
ENV GO111MODULE=on
ENV GOPROXY=https://proxy.golang.org
ADD go.mod go.sum ./
RUN go mod download
ADD . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /build/go-fake-storage main.go

FROM alpine:3.10.1
MAINTAINER Matteo Scandolo <teo.punto@gmail.com>

COPY --from=builder /build/go-fake-storage /service/go-fake-storage

EXPOSE 4443

WORKDIR /service
ENTRYPOINT ["/service/go-fake-storage"]