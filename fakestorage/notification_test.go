package fakestorage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/fsouza/fake-gcs-server/internal/notification"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newNotificationServer(t *testing.T) *Server {
	t.Helper()
	srv, err := NewServerWithOptions(Options{NoListener: true})
	require.NoError(t, err)
	srv.CreateBucketWithOpts(CreateBucketOpts{Name: "test-bucket"})
	return srv
}

func postNotification(t *testing.T, client *http.Client, bucket string, cfg notification.NotificationConfig) (*http.Response, notification.NotificationConfig) {
	t.Helper()
	body, _ := json.Marshal(cfg)
	resp, err := client.Post(
		fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/notificationConfigs", bucket),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	var created notification.NotificationConfig
	if resp.StatusCode == http.StatusOK {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))
		resp.Body.Close()
	}
	return resp, created
}

func TestInsertNotification(t *testing.T) {
	srv := newNotificationServer(t)
	cfg := notification.NotificationConfig{Topic: "projects/p/topics/t", PayloadFormat: "JSON_API_V1"}

	resp, created := postNotification(t, srv.HTTPClient(), "test-bucket", cfg)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, created.ID)
	assert.Equal(t, cfg.Topic, created.Topic)
}

func TestInsertNotification_BucketNotFound(t *testing.T) {
	srv := newNotificationServer(t)
	cfg := notification.NotificationConfig{Topic: "projects/p/topics/t"}
	resp, _ := postNotification(t, srv.HTTPClient(), "no-such-bucket", cfg)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestInsertNotification_MissingTopic(t *testing.T) {
	srv := newNotificationServer(t)
	resp, _ := postNotification(t, srv.HTTPClient(), "test-bucket", notification.NotificationConfig{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetNotification(t *testing.T) {
	srv := newNotificationServer(t)
	cfg := notification.NotificationConfig{Topic: "projects/p/topics/t"}
	_, inserted := postNotification(t, srv.HTTPClient(), "test-bucket", cfg)

	resp, err := srv.HTTPClient().Get(fmt.Sprintf(
		"https://storage.googleapis.com/storage/v1/b/test-bucket/notificationConfigs/%s",
		inserted.ID,
	))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var got notification.NotificationConfig
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, inserted.ID, got.ID)
	assert.Equal(t, cfg.Topic, got.Topic)
}

func TestGetNotification_NotFound(t *testing.T) {
	srv := newNotificationServer(t)
	resp, err := srv.HTTPClient().Get(
		"https://storage.googleapis.com/storage/v1/b/test-bucket/notificationConfigs/9999",
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestListNotifications(t *testing.T) {
	srv := newNotificationServer(t)
	client := srv.HTTPClient()
	listURL := "https://storage.googleapis.com/storage/v1/b/test-bucket/notificationConfigs"
	cfg := notification.NotificationConfig{Topic: "projects/p/topics/t"}

	// empty list
	resp, err := client.Get(listURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listResp struct {
		Items []notification.NotificationConfig `json:"items"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&listResp))
	assert.Empty(t, listResp.Items)

	// insert two
	for i := 0; i < 2; i++ {
		r, _ := postNotification(t, client, "test-bucket", cfg)
		io.Copy(io.Discard, r.Body)
	}

	resp2, err := client.Get(listURL)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&listResp))
	assert.Len(t, listResp.Items, 2)
}

func TestDeleteNotification(t *testing.T) {
	srv := newNotificationServer(t)
	client := srv.HTTPClient()
	cfg := notification.NotificationConfig{Topic: "projects/p/topics/t"}
	_, inserted := postNotification(t, client, "test-bucket", cfg)

	deleteURL := fmt.Sprintf(
		"https://storage.googleapis.com/storage/v1/b/test-bucket/notificationConfigs/%s",
		inserted.ID,
	)

	req, _ := http.NewRequest(http.MethodDelete, deleteURL, nil)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// second delete → 404
	req2, _ := http.NewRequest(http.MethodDelete, deleteURL, nil)
	resp2, err := client.Do(req2)
	require.NoError(t, err)
	resp2.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}
