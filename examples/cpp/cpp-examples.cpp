#include <iostream>
#include "gtest/gtest.h"
#include "google/cloud/storage/client.h"

using namespace google::cloud::storage;

TEST(CppExamples, ResumableUploadTest) {
  ClientOptions options(oauth2::CreateAnonymousCredentials());
  options.set_endpoint("localhost:8080");
  Client client(options, LimitedErrorCountRetryPolicy(2));
  client.CreateBucket("my-bucket", BucketMetadata());
  auto writer = client.WriteObject("my-bucket", "my-key");
  writer << "hello world";
  writer.Close();
  auto result = writer.metadata();
  ASSERT_TRUE(result.ok());
  auto objects = client.ListObjects("my-bucket");
  ASSERT_EQ(1, std::distance(objects.begin(), objects.end()));
  ASSERT_TRUE(objects.begin()->ok());
  EXPECT_EQ("my-key", objects.begin()->value().name());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
