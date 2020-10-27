process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;
const globalLog = require("global-request-logger");
const chalk = require("chalk");
const DEBUG_HTTP = 0;
globalLog.initialize();

function httpDebug(msg, ...args) {
  if (DEBUG_HTTP) console.log(chalk.blue(msg), ...args);
}

function logHeaders(headers) {
  if (headers) {
    Object.entries(headers).forEach(([key, value]) =>
      typeof value.forEach === "function"
        ? value.forEach((v) =>
            httpDebug(
              "%s: %s",
              key,
              key.toLowerCase() === "authorization" ? "[...redacted...]" : v
            )
          )
        : httpDebug("%s: %s", key, value)
    );
  }
}
function logRequestAndResponse(request, response) {
  httpDebug("");
  httpDebug("%s %s", request.method, request.path);
  logHeaders(request.headers);
  if (request.body) httpDebug(request.body);
  if (response && response.httpVersion) {
    httpDebug("");
    httpDebug("HTTP/%s %s", response.httpVersion, response.statusCode);
    logHeaders(response.headers);
    if (response.body) httpDebug(response.body);
  }
}
globalLog.on("success", logRequestAndResponse);
globalLog.on("error", logRequestAndResponse);

async function listBuckets(storage) {
  const [buckets] = await storage.getBuckets();
  console.log("Buckets:");
  buckets.forEach((bucket) => {
    console.log(bucket.id);
  });
  console.log(`${buckets.length} buckets.`);
}

async function listFiles(bucket) {
  const [files] = await bucket.getFiles();
  console.log(`Files in ${bucket.name}: `);
  files.forEach((file) => {
    console.log(`- ${file.name}`);
  });
  console.log(`${files.length} files.`);
}

async function run() {
  // Imports the Google Cloud client library
  const { Storage } = require("@google-cloud/storage");

  // Creates a client
  const storage = new Storage({
    apiEndpoint: "127.0.0.1:4443",
  });

  // Lists all buckets in the current project
  await listBuckets(storage);

  // create a new bucket

  //NOTE is you use a free firebase account you can't create a new bucket
  // you can use the provided bucket by setting the name here,
  // remember to comment the bucket creation below and the bucket
  // deletion at the bottom of the script.
  const bucketName =
    "bucket_" + Date.now() + Math.round(Math.random() * 1000000);

  await storage.createBucket(bucketName);

  console.log("Created", bucketName);
  await listBuckets(storage);

  //list files in a bucket
  const bucket = storage.bucket(bucketName);
  await listFiles(bucket);

  //create a file in the bucket
  const fileName = "file_" + Date.now() + Math.round(Math.random() * 1000000);
  const writeFile = bucket.file(fileName);
  await writeFile.save("fake-gcs-server is awesome", { resumable: false });

  //read a file in the bucket
  const readFile = bucket.file(fileName);
  const contents = await readFile.download().then((data) => data[0]);
  console.log("Contents of ", fileName);
  console.log(contents.toString("utf-8"));

  //delete a file from the bucket
  await bucket.file(fileName).delete();
  await listFiles(bucket);

  //delete the bucket
  await bucket.delete();
  await listBuckets(storage);
  console.log(chalk.green("Demo is over, thanks for watching"));
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
