FROM golang:1.12.7-alpine AS build

ARG GOPROXY=https://proxy.golang.org
ARG CGO_ENABLED=0

WORKDIR /code
ADD go.mod go.sum ./
RUN go mod download
ADD . ./
RUN go build ./cmd/gcs-emulator

FROM alpine:3.10.1

# mailcap gives us the mime files
RUN  apk add --no-cache ca-certificates mailcap
COPY --from=build /code/gcs-emulator /bin
ENTRYPOINT ["/bin/gcs-emulator"]
