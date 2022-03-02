set -e

./fake-gcs-server -backend memory -scheme http -port 8080 -external-url "http://localhost:8080" &

(
  apk add abseil-cpp-dev gtest-dev cmake make g++ nlohmann-json curl-dev git
	cd examples/cpp
	mkdir build
	cd build
	cmake ..
	make -j$(nproc)
	./cpp-example
)
