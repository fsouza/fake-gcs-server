# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

./fake-gcs-server -backend memory -data $PWD/examples/data &

(
	cd examples/go
	go run main.go
)
