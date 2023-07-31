# Copyright 2023 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e

./fake-gcs-server -backend memory -scheme both -data ${PWD}/examples/data &

apk add --update curl
curl --silent --fail --insecure https://0.0.0.0:4443/storage/v1/b
curl --silent --fail --insecure http://0.0.0.0:8000/storage/v1/b
