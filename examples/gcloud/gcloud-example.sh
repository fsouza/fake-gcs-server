export CLOUDSDK_CORE_PROJECT=fake-project
export GCS_API_ENDPOINT=http://localhost:4443/
export CLOUDSDK_API_ENDPOINT_OVERRIDES_STORAGE=http://localhost:4443/
export CLOUDSDK_AUTH_DISABLE_CREDENTIALS=True


# Create bucket
gcloud storage buckets create gs://fake-bucket

# List buckets
gcloud storage buckets list

# Copy file to bucket
gcloud storage cp examples/gcloud/image.png gs://fake-bucket/image.png

# List objects
gcloud storage objects list gs://fake-bucket

# Tar gzip and stream to bucket
tar -czf - examples/gcloud/image.png | gcloud storage cp - gs://fake-bucket/image.tar.gz

# List objects
gcloud storage objects list gs://fake-bucket