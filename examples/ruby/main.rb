# How to run this example
# 1 - Build the docker image by running the command "docker build -t fsouza/fake-gcs-server ."
# 2 - Start the docker container: "docker run -d --name fake-gcs-server -p 8080:4443 -v ${PWD}/examples/data:/data fsouza/fake-gcs-server -scheme http"
# 3 - Check if it's working by running: "curl http://0.0.0.0:8080/storage/v1/b"
# 4 - Install requirements: "bundle install"
# 5 - Run this script: "bundle exec ruby main.rb"

require 'google/cloud/storage'

client = Google::Cloud::Storage.anonymous(endpoint: 'http://localhost:8080/')
client.buckets.each do |bucket|
  puts "Bucket: #{bucket.name}"
  bucket.files.each do |file|
    puts "File: #{file.name}"
  end
end
