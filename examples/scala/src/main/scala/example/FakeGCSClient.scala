package example
import com.google.cloud.storage.{BlobId, BlobInfo, BucketInfo, StorageOptions}

import java.io.ByteArrayOutputStream

object FakeGCSClient extends App {

  val opts = StorageOptions.newBuilder().setHost("http://0.0.0.0:8080").build()
  val client = opts.getService

  println("Creating a bucket")
  val BUCKET = "test"
  client.create(BucketInfo.of(BUCKET))

  println("Uploading object")
  val blobId = BlobId.of(BUCKET, s"testing/hello.txt")
  val blobInfo = BlobInfo.newBuilder(blobId).build()
  client.create(blobInfo, "Hello World!".getBytes)

  println("Downloading object")
  val baos = new ByteArrayOutputStream()
  val blob = client.get(blobId)
  blob.downloadTo(baos)
  println(new String(baos.toByteArray))
}
