package com.fsouza.fakegcsserver.java.examples;

import com.google.cloud.NoCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FakeGcsServerTest {
    
    static Storage storageClient;

    @BeforeAll
    static void setUpStorageClient() throws Exception {
        String fakeGcsExternalUrl = "http://0.0.0.0:8080";

        storageClient = StorageOptions.newBuilder()
            .setHost(fakeGcsExternalUrl)
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();
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
