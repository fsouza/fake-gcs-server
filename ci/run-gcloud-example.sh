# Copyright 2023 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e

./fake-gcs-server -backend memory -port 4443 &

./examples/gcloud/gcloud-example.sh

pkill fake-gcs-server
