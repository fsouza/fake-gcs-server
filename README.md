# fake-gcs-server

[![Build Status](https://travis-ci.com/fsouza/fake-gcs-server.svg?branch=master)](https://travis-ci.com/fsouza/fake-gcs-server)
[![GoDoc](https://img.shields.io/badge/api-Godoc-blue.svg?style=flat-square)](https://godoc.org/github.com/fsouza/fake-gcs-server/fakestorage)

fake-gcs-server is a library for mocking Google's Cloud Storage API locally.
It's designed to be used from within test suites in Go packages. If you want to
run a standalone server (like the datastore/pubsub emulators) for integration
tests and/or tests in other languages, check out
[teone/gc-fake-storage](https://github.com/teone/gc-fake-storage).
