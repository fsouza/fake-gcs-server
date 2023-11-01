# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# How to run this example
# 1 - Build the docker image by running the command "docker build -t fsouza/fake-gcs-server ."
# 2 - Start the docker container: "docker run -d --name fake-gcs-server -p 4443:4443 -v ${PWD}/examples/data:/data fsouza/fake-gcs-server -scheme http"
# 3 - Check if it's working by running: "curl http://0.0.0.0:4443/storage/v1/b"
# 4 - Create a python virtual enviroment (Ex: python -m .venv venv)
# 5 - Source the env (source .venv/bin/activate)
# 7 - Go to the following directory examples/python: (cd examples/python)
# 6 - Install requirements: "pip install -r requirements.txt" and "pip install -r requirements.in"
# 7 - Run this script

# For additional info on how to run this example or setup the docker container check the
# run script "ci/run-python-example.sh"

import tempfile
import os

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

# This endpoint assumes that you are using the default port 4443 from the container.
# If you are using a different port, please set the environment variable STORAGE_EMULATOR_HOST.
os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://localhost:4443")


client = storage.Client(
    credentials=AnonymousCredentials(),
    project="test",
    # Alternatively instead of using the global env STORAGE_EMULATOR_HOST. You can define it here.
    # This will set this client object to point to the local google cloud storage.
    # client_options={"api_endpoint": "http://localhost:4443"},
)

# List the Buckets
for bucket in client.list_buckets():
    print(f"Bucket: {bucket.name}\n")

    # List the Blobs in each Bucket
    for blob in bucket.list_blobs():
        print(f"Blob: {blob.name}")

        # Print the content of the Blob
        b = bucket.get_blob(blob.name)
        with tempfile.NamedTemporaryFile() as temp_file:
            s = b.download_to_filename(temp_file.name)
            temp_file.seek(0, 0)
            print(temp_file.read(), "\n")
