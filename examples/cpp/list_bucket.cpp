#include "google/cloud/storage/client.h"
#include <iostream>

using namespace google::cloud::storage;

int main() {
  ClientOptions options(oauth2::CreateAnonymousCredentials());
  options.set_endpoint("localhost:4443");
  Client client(options, LimitedErrorCountRetryPolicy(2));
  auto objects = client.ListObjects("my-bucket");
  for (auto& object : objects) {
    if (object.ok())
      std::cout << object.value();
  }
}
