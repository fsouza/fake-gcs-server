# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e

./fake-gcs-server -backend memory -scheme http -port 8080 -data $PWD/examples/data &

(
	export STORAGE_EMULATOR_HOST=http://localhost:8080
	cd examples/node
	npm ci
	node index.js
)
