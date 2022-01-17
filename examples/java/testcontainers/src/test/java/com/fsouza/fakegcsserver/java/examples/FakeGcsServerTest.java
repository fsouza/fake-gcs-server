package com.fsouza.fakegcsserver.java.examples;

import com.google.cloud.NoCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class FakeGcsServerTest {

    @Container
    static final GenericContainer<?> fakeGcs = new FixedHostPortGenericContainer<>("sergseven/fake-gcs-server:v2")
        .withReuse(true)
        .withExposedPorts(4443)
        .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
            "/bin/fake-gcs-server",
            "-scheme", "http"
        ));

    static Storage storageService;

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
        String modifyExternalUrlRequestUri = fakeGcsExternalUrl + "/internal/config";
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
}
