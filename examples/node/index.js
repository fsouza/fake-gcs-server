process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;
process.on("unhandledRejection", err => { throw err });

async function listBuckets() {
  // [START storage_list_buckets]
  // Imports the Google Cloud client library
  const {Storage} = require("@google-cloud/storage");

  // Creates a client
  const storage = new Storage({
    apiEndpoint: "127.0.0.1:4443",
    projectId: "test",
  });

  // Lists all buckets in the current project
  const [buckets] = await storage.getBuckets();
  console.log("Buckets:");
  buckets.forEach(bucket => {
    console.log(bucket.id);
  });
  // [END storage_list_buckets]
}

listBuckets().then(console.log);
