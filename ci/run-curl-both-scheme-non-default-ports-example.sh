# Copyright 2023 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e

./fake-gcs-server -backend memory -port 5553 -port-http 9000 -scheme both -data ${PWD}/examples/data &

apk add --update curl
curl --silent --fail --insecure https://0.0.0.0:5553/storage/v1/b
curl --silent --fail --insecure http://0.0.0.0:9000/storage/v1/b
