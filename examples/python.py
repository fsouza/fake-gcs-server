from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
import requests
import urllib3

BASE_URL = "https://127.0.0.1:4443"

storage._http.Connection.API_BASE_URL = BASE_URL # override the BASE_URL in the client library with the mock server
storage.blob._DOWNLOAD_URL_TEMPLATE = (u"%s/download/storage/v1{path}?alt=media" % BASE_URL)
storage.blob._BASE_UPLOAD_TEMPLATE = (u"%s/upload/storage/v1{bucket_path}/o?uploadType=" % BASE_URL)
storage.blob._MULTIPART_URL_TEMPLATE = storage.blob._BASE_UPLOAD_TEMPLATE + u"multipart"
storage.blob._RESUMABLE_URL_TEMPLATE = storage.blob._BASE_UPLOAD_TEMPLATE + u"resumable"

my_http = requests.Session()
my_http.verify = False  # disable SSL validation
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # disable https warnings for https insecure certs

client = storage.Client(credentials=AnonymousCredentials(), project="test", _http=my_http)

# List the Buckets
for bucket in client.list_buckets():
    print("Bucket: %s\n" % bucket.name)

    # List the Blobs in each Bucket
    for blob in bucket.list_blobs():
        print("Blob: %s" % blob.name)

        # Print the content of the Blob
        b = bucket.get_blob(blob.name)
        s = b.download_as_string()
        print(s, "\n")
