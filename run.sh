#!/bin/sh

/bin/fake-gcs-server \
  -backend "${BACKEND:-filesystem}" \
  -cert-location "${CERT_LOCATION}" \
  -cors-headers "${CORS_HEADERS}" \
  -data "${DATA:-/data}" \
  -event.list "${EVENT_LIST:-finalize}" \
  -event.object-prefix "${EVENT_OBJECT_PREFIX}" \
  -event.pubsub-project-id "${EVENT_PUBSUB_PROJECT_ID}" \
  -event.pubsub-topic "${EVENT_PUBSUB_TOPIC}" \
  -external-url "${EXTERNAL_URL}" \
  -filesystem-root "${FILESYSTEM_ROOT:-/storage}" \
  -host "${HOST:-0.0.0.0}" \
  -location "${LOCATION:-US-CENTRAL1}" \
  -log-level "${LOG_LEVEL:-info}" \
  -port "${PORT:-4443}" \
  -private-key-location "${PRIVATE_KEY_LOCATION}" \
  -public-host "${PUBLIC_HOST:-storage.googleapis.com}" \
  -scheme "${SCHEME:-https}"
