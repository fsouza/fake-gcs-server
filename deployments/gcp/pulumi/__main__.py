import pulumi

from pulumi_gcp import storage

# Create a GCP resource (Storage Bucket)
bucket_no_versioning = storage.Bucket(f"{pulumi.get_project()}-tests-no-versioning")
bucket_versioning = storage.Bucket(f"{pulumi.get_project()}-tests-versioning",versioning={"enabled": True})

# Export the DNS name of the bucket
pulumi.export("bucket_name_no_versioning", bucket_no_versioning.url)
pulumi.export("bucket_name_versioning", bucket_versioning.url)
