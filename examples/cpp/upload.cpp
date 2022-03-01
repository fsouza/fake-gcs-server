#include "google/cloud/storage/client.h"
#include <iostream>

using namespace google::cloud::storage;

int main() {
  ClientOptions options(oauth2::CreateAnonymousCredentials());
  options.set_endpoint("localhost:4443");
  Client client(options, LimitedErrorCountRetryPolicy(2));
  client.CreateBucket("my-bucket", BucketMetadata());
  auto writer = client.WriteObject("my-bucket", "my-key");
  writer << "hello world";
  writer.Close();
  auto result = writer.metadata();
  if (!result.ok()) {
    std::cout << "Upload failed: " << result.status().message();
  } else {
    std::cout << "Upload succeeded";
  }
}
