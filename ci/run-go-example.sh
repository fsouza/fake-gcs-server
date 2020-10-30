# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e

export STORAGE_EMULATOR_HOST=http://localhost:8080
./fake-gcs-server -backend memory -data $PWD/examples/data -scheme http -port 8080 -public-host localhost:8080 &

(
	cd examples/go
	go run main.go
)
