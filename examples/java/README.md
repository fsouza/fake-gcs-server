# Java example

## Common case

          Storage storageClient = StorageOptions.newBuilder()
            .setHost(fakeGcsExternalUrl)
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();

See [the example](/src/test/java/com/fsouza/fakegcsserver/java/examples/FakeGcsServerTest.java) for more details.

## Resumable upload operations and containerised fake-gcs-server

The main difficulty with the case when a fake-gcs-server is containerised (by [Testcontainers](https://www.testcontainers.org/) for example)
is that the container IP and port are assigned after the container is eventually started, so we can not provide
fake-gcs-server with `external-url` option beforehand(when a test container has not been started yet).

It's necessary to update the server configuration before making resumable upload calls against the server, so the fake server can respond
with correct `content` HTTP header.

The example will look like as follows:

* additional testcontainers dependencies at `pom.xml`:

```xml
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>testcontainers</artifactId>
  <version>${testcontainers.version}</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>junit-jupiter</artifactId>
  <version>${testcontainers.version}</version>
  <scope>test</scope>
</dependency>
```

  * spin up fake-gcs-server testcontainer right in the test `FakeGcsServerTest.java`:
```java
@Testcontainers
class FakeGcsServerTest {

    @Container
    static final GenericContainer<?> fakeGcs = new GenericContainer<>("fsouza/fake-gcs-server")
      .withExposedPorts(4443)
      .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
          "/bin/fake-gcs-server",
          "-scheme", "http"
      ));

    @BeforeAll
    static void setUpFakeGcs() throws Exception {
      String fakeGcsExternalUrl = "http://" + fakeGcs.getHost() + ":" + fakeGcs.getFirstMappedPort();

      updateExternalUrlWithContainerUrl(fakeGcsExternalUrl);

      storageClient = StorageOptions.newBuilder()
          .setHost(fakeGcsExternalUrl)
          .setProjectId("test-project")
          .setCredentials(NoCredentials.getInstance())
          .build()
          .getService();
    }

    private static void updateExternalUrlWithContainerUrl(String fakeGcsExternalUrl) throws Exception {
      String modifyExternalUrlRequestUri = fakeGcsExternalUrl + "/_internal/config";
      String updateExternalUrlJson = "{"
          + "\"externalUrl\": \"" + fakeGcsExternalUrl + "\""
          + "}";

      HttpRequest req = HttpRequest.newBuilder()
          .uri(URI.create(modifyExternalUrlRequestUri))
          .header("Content-Type", "application/json")
          .PUT(BodyPublishers.ofString(updateExternalUrlJson))
          .build();
      HttpResponse<Void> response = HttpClient.newBuilder().build()
          .send(req, BodyHandlers.discarding());

      if (response.statusCode() != 200) {
          throw new RuntimeException(
              "error updating fake-gcs-server with external url, response status code " + response.statusCode() + " != 200");
      }
    }

    @Test
    void shouldUploadFileByWriterChannel() throws IOException {

      storageClient.create(BucketInfo.newBuilder("sample-bucket2").build());

      WriteChannel channel = storageClient.writer(BlobInfo.newBuilder("sample-bucket2", "some_file2.txt").build());
      channel.write(ByteBuffer.wrap("line1\n".getBytes()));
      channel.write(ByteBuffer.wrap("line2\n".getBytes()));
      channel.close();

      Blob someFile2 = storageClient.get("sample-bucket2", "some_file2.txt");
      String fileContent = new String(someFile2.getContent());
      assertEquals("line1\nline2\n", fileContent);
    }
}
```
