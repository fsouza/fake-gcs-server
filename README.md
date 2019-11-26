# fake-gcs-server

[![Build Status](https://github.com/fsouza/fake-gcs-server/workflows/Build/badge.svg)](https://github.com/fsouza/fake-gcs-server/actions?query=branch:master+workflow:Build)
[![GoDoc](https://img.shields.io/badge/api-Godoc-blue.svg?style=flat-square)](https://pkg.go.dev/github.com/fsouza/fake-gcs-server/fakestorage?tab=doc)

fake-gcs-server provides an emulator for Google Cloud Storage API. It can be
used as a library in Go projects and/or as a standalone binary/Docker image.

The library is available inside the package
[``github.com/fsouza/fake-gcs-server/fakestorage``](https://pkg.go.dev/github.com/fsouza/fake-gcs-server/fakestorage?tab=doc)
and can be used from within test suites in Go package. The emulator is
available as a binary that can be built manually, downloaded from the [releases
page](https://github.com/fsouza/fake-gcs-server/releases) or pulled from Docker
Hub ([``docker pull
fsouza/fake-gcs-server``](https://hub.docker.com/r/fsouza/fake-gcs-server)).

## Using the emulator in Docker

If you want to run a standalone server (like the datastore/pubsub emulators)
for integration tests and/or tests in other languages you may want to run the
``fake-gcs-server`` inside a Docker container:

```shell
docker run -d --name fake-gcs-server -p 4443:4443 fsouza/fake-gcs-server
```

### Preload data

In case you want to preload some data in ``fake-gcs-server`` just mount a
folder in the container at ``/data``:

```shell
docker run -d --name fake-gcs-server -p 4443:4443 -v ${PWD}/examples/data:/data fsouza/fake-gcs-server
```

Where the content of ``${PWD}/examples/data`` is something like:

```
.
└── sample-bucket
    └── some_file.txt
```

To make sure everything works as expected you can execute these commands:

```shell
curl --insecure https://0.0.0.0:4443/storage/v1/b
{"kind":"storage#buckets","items":[{"kind":"storage#bucket","id":"sample-bucket","name":"sample-bucket"}],"prefixes":null}

curl --insecure https://0.0.0.0:4443/storage/v1/b/sample-bucket/o
{"kind":"storage#objects","items":[{"kind":"storage#object","name":"some_file.txt","id":"sample-bucket/some_file.txt","bucket":"sample-bucket","size":"33"}],"prefixes":[]}
```

This will result in one bucket called ``sample-bucket`` containing one object called ``some_file.txt``.

## Client library examples

For examples using the Python, Node.js and Go clients, check out the [``examples``](/examples/) directory.

### Building the image locally

You may use ``docker build`` to build the image locally instead of pulling it
from Docker Hub:

```shell
docker build -t fsouza/fake-gcs-server .
```
