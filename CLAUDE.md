# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fake-gcs-server is a Google Cloud Storage API emulator that can be used as:
1. A Go library (`github.com/fsouza/fake-gcs-server/fakestorage`) for integration tests
2. A standalone binary/Docker container for testing GCS-dependent applications in any language

## Common Commands

### Build
```bash
go build
```

### Run Tests
```bash
# Run all tests with race detection
go test -race -vet all -mod readonly ./...

# Run a single test
go test -race -vet all -mod readonly ./fakestorage -run TestName

# Run tests in a specific package
go test -race -vet all -mod readonly ./internal/backend
```

### Lint
```bash
golangci-lint run
staticcheck ./...
```

### Run the Server
```bash
# Default HTTPS on port 4443
./fake-gcs-server

# HTTP mode
./fake-gcs-server -scheme http

# Both HTTP and HTTPS
./fake-gcs-server -scheme both

# With seed data
./fake-gcs-server -data /path/to/seed/data

# See all flags
./fake-gcs-server -help
```

## Architecture

### Package Structure

- **main.go**: CLI entry point, handles server startup with HTTP/HTTPS/both schemes and gRPC multiplexing
- **fakestorage/**: Public Go library package
  - `server.go`: Core server implementation, HTTP routing via gorilla/mux
  - `object.go`, `bucket.go`: GCS resource handlers
  - `upload.go`: Resumable and multipart upload handling
- **internal/backend/**: Storage abstraction layer
  - `storage.go`: `Storage` interface definition
  - `memory.go`: In-memory backend implementation
  - `fs.go`: Filesystem-based backend implementation
- **internal/grpc/**: gRPC server implementation sharing the same backend as HTTP
- **internal/notification/**: Pub/Sub event notifications
- **internal/config/**: CLI configuration parsing

### Key Design Patterns

1. **Backend Abstraction**: The `backend.Storage` interface allows swapping between memory and filesystem storage. Both HTTP and gRPC servers share the same backend instance.

2. **HTTP/gRPC Multiplexing**: The main server multiplexes HTTP and gRPC on the same port by checking the Content-Type header for `application/grpc`.

3. **Public Library API**: The `fakestorage` package is designed for use in Go tests - `Server.Client()` returns a pre-configured `*storage.Client` that talks to the fake server.

### API Endpoints

The server implements:
- JSON API at `/storage/v1/...`
- XML API for public object access
- Upload API at `/upload/storage/v1/...`
- Internal endpoints: `/_internal/healthcheck`, `/_internal/config`, `/_internal/reseed`
