# fake-gcs-server

[![Build Status](https://travis-ci.com/fsouza/fake-gcs-server.svg?branch=master)](https://travis-ci.com/fsouza/fake-gcs-server)
[![GoDoc](https://img.shields.io/badge/api-Godoc-blue.svg?style=flat-square)](https://godoc.org/github.com/fsouza/fake-gcs-server/fakestorage)

fake-gcs-server is a library for mocking Google's Cloud Storage API locally.
It's designed to be used from within test suites in Go packages. 

## Run in Docker
If you want to run a standalone server (like the datastore/pubsub emulators) 
for integration tests and/or tests in other languages you may want to run the
`fake-gcs-server` inside a `docker` container.

## How to use

```shell
docker run -d --name fake-gcs-server -p 4443:4443 fsouza/fake-gcs-server:1.7.2
```

## Preload data

In case you want to preload some data in `fake-gcs-server` just mount a folder in the container at `/data`:

```shell
docker run -d --name fake-gcs-server -p 4443:4443 -v $(PWD)/examples/data:/data fsouza/fake-gcs-server:1.7.2
```

Where the content of `/tmp/data` is:

```
- sample_bucket
  - some_file.txt
```

To make sure everything works as expected you can execut these commands:

```shell
curl --insecure https://0.0.0.0:4443/storage/v1/b
{"kind":"storage#buckets","items":[{"kind":"storage#bucket","id":"sample_bucket","name":"sample_bucket"}],"prefixes":null}

curl --insecure https://0.0.0.0:4443/storage/v1/b/sample_bucket/o
{"kind":"storage#objects","items":[{"kind":"storage#object","name":"some_file.txt","id":"sample_bucket/some_file.txt","bucket":"sample_bucket","size":"33"}],"prefixes":[]}
```

This will result in two `buckets` containing one `blob` each.

## Example with the `python` client library:

For a more complex example look into the `examples` directory

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
````

## Build the image from source

```
docker build -t fsouza/fake-gcs-server .
```
