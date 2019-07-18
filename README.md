# fake-gcs-server

[![Build Status](https://cloud.drone.io/api/badges/fsouza/fake-gcs-server/status.svg)](https://cloud.drone.io/fsouza/fake-gcs-server)
[![GoDoc](https://img.shields.io/badge/api-Godoc-blue.svg?style=flat-square)](https://godoc.org/github.com/fsouza/fake-gcs-server/fakestorage)

fake-gcs-server provides an emulator for Google Cloud Storage API. It can be
used as a library in Go projects and/or as a standalone binary/Docker image.

The library is available inside the package
[``github.com/fsouza/fake-gcs-server/fakestorage``](https://godoc.org/github.com/fsouza/fake-gcs-server/fakestorage)
and can be used from within test suites in Go package. The emulator is
available as a binary that can be built manually or fetched from Docker Hub
([``docker pull
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
{"kind":"storage#buckets","items":[{"kind":"storage#bucket","id":"sample_bucket","name":"sample_bucket"}],"prefixes":null}

curl --insecure https://0.0.0.0:4443/storage/v1/b/sample_bucket/o
{"kind":"storage#objects","items":[{"kind":"storage#object","name":"some_file.txt","id":"sample_bucket/some_file.txt","bucket":"sample_bucket","size":"33"}],"prefixes":[]}
```

This will result in two ``buckets`` containing one ``blob`` each.

## Example with the Python client library

> For more examples, check out the [``examples``](/examples/) directory.

```python
# virtualenv venv --no-site-packages
# source venv/bin/activate
# pip install -r pip_requirements.txt
# python python.py

from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
import requests
import urllib3

storage._http.Connection.API_BASE_URL = "https://127.0.0.1:4443" # override the BASE_URL in the client library with the mock server

my_http = requests.Session()
my_http.verify = False  # disable SSL validation
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # disable https warnings for https insecure certs

client = storage.Client(credentials=AnonymousCredentials(), project="test", _http=my_http)
for bucket in client.list_buckets():
    print(bucket.name)
```

### Building the image locally

You may use ``docker build`` to build the image locally instead of pulling it
from Docker Hub:

```shell
docker build -t fsouza/fake-gcs-server .
```
