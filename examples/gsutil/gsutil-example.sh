#!/usr/bin/env bash

# Copyright 2023 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -euo pipefail

bucket_name=some-bucket
project_id=test-project
here=$(cd "$(dirname "${0}")" && pwd -P)

# create bucket
gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" mb -p "${project_id}" "gs://${bucket_name}"

# list objects in the bucket (should be empty)
gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" ls -p "${project_id}" "gs://${bucket_name}"

# upload a couple of fileds
gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" cp "${here}"/hello.txt "${here}"/image.png "gs://${bucket_name}/"

# list objects in the bucket (should include the files that were just uploaded)
gsutil -o "Credentials:gs_json_host=127.0.0.1" -o "Credentials:gs_json_port=4443" -o "Boto:https_validate_certificates=False" ls -p "${project_id}" "gs://${bucket_name}"
