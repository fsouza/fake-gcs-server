async function run() {
  // [START storage_list_buckets]
  // Imports the Google Cloud client library
  const { Storage } = require("@google-cloud/storage");

  // Creates a client
  const storage = new Storage({
    apiEndpoint: "http://localhost:8080",
    projectId: "test",
  });

  // Lists all buckets in the current project
  const [buckets] = await storage.getBuckets();
  console.log("Buckets:");
  buckets.forEach((bucket) => {
    console.log(bucket.id);
  });
  // [END storage_list_buckets]

  const response = await storage.bucket('sample-bucket')
    .file('some_file.txt')
    .download();
  console.log("Contents:")
  console.log(response)
}


run().catch((err) => {
  console.error(err);
  process.exit(1);
});
