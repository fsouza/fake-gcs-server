# Copyright 2022 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e

./fake-gcs-server -backend memory -scheme http -port 8080 -external-url "http://localhost:8080" &

(
	cd examples/java
	./mvnw clean test -B
)
