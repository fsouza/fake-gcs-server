# fake-gcs-server

[![Build Status](https://travis-ci.com/fsouza/fake-gcs-server.svg?branch=master)](https://travis-ci.com/fsouza/fake-gcs-server)
[![GoDoc](https://img.shields.io/badge/api-Godoc-blue.svg?style=flat-square)](https://godoc.org/github.com/fsouza/fake-gcs-server/fakestorage)

fake-gcs-server is a project that provides an emulator and a library for
mocking Google's Cloud Storage API locally.

The library is available inside the package
[``github.com/fsouza/fake-gcs-server/fakestorage``](https://godoc.org/github.com/fsouza/fake-gcs-server/fakestorage)
and can be used from within test suites in Go package. The emulator is
available as a binary that can be built manually or fetched from Docker Hub
([``docker pull
fsouza/gcs-emulator``](https://hub.docker.com/r/fsouza/gcs-emulator)).
