# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import tempfile

import requests
import urllib3
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

EXTERNAL_URL = os.getenv("EXTERNAL_URL", "https://127.0.0.1:4443")
PUBLIC_HOST = os.getenv("PUBLIC_HOST", "storage.gcs.127.0.0.1.nip.io:4443")

storage.blob._API_ACCESS_ENDPOINT = "https://" + PUBLIC_HOST
storage.blob._DOWNLOAD_URL_TEMPLATE = (
    "%s/download/storage/v1{path}?alt=media" % EXTERNAL_URL
)
storage.blob._BASE_UPLOAD_TEMPLATE = (
    "%s/upload/storage/v1{bucket_path}/o?uploadType=" % EXTERNAL_URL
)
storage.blob._MULTIPART_URL_TEMPLATE = storage.blob._BASE_UPLOAD_TEMPLATE + "multipart"
storage.blob._RESUMABLE_URL_TEMPLATE = storage.blob._BASE_UPLOAD_TEMPLATE + "resumable"

my_http = requests.Session()
my_http.verify = False  # disable SSL validation
urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)  # disable https warnings for https insecure certs

client = storage.Client(
    credentials=AnonymousCredentials(),
    project="test",
    _http=my_http,
    client_options=ClientOptions(api_endpoint=EXTERNAL_URL),
)

# List the Buckets
for bucket in client.list_buckets():
    print("Bucket: %s\n" % bucket.name)

    # List the Blobs in each Bucket
    for blob in bucket.list_blobs():
        print("Blob: %s" % blob.name)

        # Print the content of the Blob
        b = bucket.get_blob(blob.name)
        with tempfile.NamedTemporaryFile() as temp_file:
            s = b.download_to_filename(temp_file.name)
            temp_file.seek(0, 0)
            print(temp_file.read(), "\n")
