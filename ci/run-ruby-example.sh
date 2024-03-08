set -e

./fake-gcs-server -backend memory -scheme http -port 8080 -external-url "http://localhost:8080" &

(
	cd examples/ruby
	bundle
	bundle exec ruby main.rb
)
