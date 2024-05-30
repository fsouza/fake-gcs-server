### Usage with Testcontainers

See [Java example's
README](https://github.com/fsouza/fake-gcs-server/tree/main/examples/java#resumable-upload-operations-and-containerised-fake-gcs-server)
for context.

```ts
import { Storage } from "@google-cloud/storage";
import ky from "ky";
import { GenericContainer } from "testcontainers";

const PORT = 4443;

const CONTAINER = await new GenericContainer("fsouza/fake-gcs-server:1.49.0")
  .withEntrypoint(["/bin/fake-gcs-server", "-scheme", "http"])
  .withExposedPorts(PORT)
  .start();

const API_ENDPOINT = `http://${CONTAINER.getHost()}:${CONTAINER.getMappedPort(
  PORT
)}`;

await ky.put(`${API_ENDPOINT}/_internal/config`, {
  json: { externalUrl: API_ENDPOINT },
});

const STORAGE = new Storage({ apiEndpoint: API_ENDPOINT });

// ...
```
