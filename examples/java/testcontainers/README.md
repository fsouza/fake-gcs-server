# Java example with Testcontainers

## Resumable upload operations

The main difficulty with [Testcontainers](https://www.testcontainers.org/) and fake-gcs-server is that the container IP and port are
assigned after the container is eventually started, so we can not provide fake-gcs-server with external-url option beforehand(when a test
container has not been started yet).

It's necessary to update the server configuration before making resumable upload calls against the server, so the fake server can respond
with correct `content` HTTP header:

    @BeforeAll
    static void setUpFakeGcs() throws Exception {
        String fakeGcsExternalUrl = "http://" + fakeGcs.getContainerIpAddress() + ":" + fakeGcs.getFirstMappedPort();

        updateExternalUrlWithContainerUrl(fakeGcsExternalUrl);

        storageService = StorageOptions.newBuilder()
            .setHost(fakeGcsExternalUrl)
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();
    }

    private static void updateExternalUrlWithContainerUrl(String fakeGcsExternalUrl) throws Exception {
        String modifyExternalUrlRequestUri = fakeGcsExternalUrl + "/internal/config/url/external";
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

        storageService.create(BucketInfo.newBuilder("sample-bucket2").build());

        WriteChannel channel = storageService.writer(BlobInfo.newBuilder("sample-bucket2", "some_file2.txt").build());
        channel.write(ByteBuffer.wrap("line1\n".getBytes()));
        channel.write(ByteBuffer.wrap("line2\n".getBytes()));
        channel.close();

        Blob someFile2 = storageService.get("sample-bucket2", "some_file2.txt");
        String fileContent = new String(someFile2.getContent());
        assertEquals("line1\nline2\n", fileContent);
    }

See [the example](/src/test/java/com/fsouza/fakegcsserver/java/examples/FakeGcsServerTest.java).

