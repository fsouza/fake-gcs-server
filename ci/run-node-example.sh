# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e

./fake-gcs-server -backend memory -data $PWD/examples/data &

(
	cd examples/node
	npm ci
	node index.js
)
