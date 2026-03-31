package notification

import (
	"sync"
	"testing"

	"github.com/fsouza/fake-gcs-server/internal/backend"
)

func TestNotificationRegistry_InsertGetDeleteList(t *testing.T) {
	r := NewNotificationRegistry(nil)
	cfg := NotificationConfig{Topic: "projects/my-project/topics/my-topic", PayloadFormat: "JSON_API_V1"}

	inserted := r.Insert("my-bucket", cfg)
	if inserted.ID == "" {
		t.Fatal("expected non-empty ID after Insert")
	}

	got, ok := r.Get("my-bucket", inserted.ID)
	if !ok {
		t.Fatal("Get: expected config to exist")
	}
	if got.Topic != cfg.Topic {
		t.Errorf("Get: topic mismatch: want %q got %q", cfg.Topic, got.Topic)
	}

	if _, ok := r.Get("other-bucket", inserted.ID); ok {
		t.Error("Get: should not find config in wrong bucket")
	}

	if n := len(r.List("my-bucket")); n != 1 {
		t.Errorf("List: expected 1 config, got %d", n)
	}
	if r.List("other-bucket") != nil {
		t.Error("List: expected nil for empty bucket")
	}

	if !r.Delete("my-bucket", inserted.ID) {
		t.Error("Delete: expected true for existing ID")
	}
	if r.Delete("my-bucket", inserted.ID) {
		t.Error("Delete: expected false for already-deleted ID")
	}
}

func TestNotificationRegistry_DefaultPayloadFormat(t *testing.T) {
	r := NewNotificationRegistry(nil)
	inserted := r.Insert("bucket", NotificationConfig{Topic: "projects/p/topics/t"})
	if inserted.PayloadFormat != "JSON_API_V1" {
		t.Errorf("expected default PayloadFormat 'JSON_API_V1', got %q", inserted.PayloadFormat)
	}
}

func TestNotificationRegistry_MatchesConfig(t *testing.T) {
	tests := []struct {
		name      string
		cfg       NotificationConfig
		objName   string
		eventType EventType
		want      bool
	}{
		{"no filter matches everything", NotificationConfig{}, "o", EventFinalize, true},
		{"matching prefix", NotificationConfig{ObjectNamePrefix: "uploads/"}, "uploads/f.txt", EventFinalize, true},
		{"non-matching prefix", NotificationConfig{ObjectNamePrefix: "uploads/"}, "other/f.txt", EventFinalize, false},
		{"matching event type", NotificationConfig{EventTypes: []EventType{EventFinalize}}, "o", EventFinalize, true},
		{"non-matching event type", NotificationConfig{EventTypes: []EventType{EventFinalize}}, "o", EventDelete, false},
		{
			"matching prefix and event type",
			NotificationConfig{ObjectNamePrefix: "uploads/", EventTypes: []EventType{EventDelete}},
			"uploads/gone.txt", EventDelete, true,
		},
		{
			"matching prefix but wrong event type",
			NotificationConfig{ObjectNamePrefix: "uploads/", EventTypes: []EventType{EventFinalize}},
			"uploads/gone.txt", EventDelete, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			o := backend.Object{ObjectAttrs: backend.ObjectAttrs{BucketName: "b", Name: tt.objName}, Content: []byte("x")}
			so := o.StreamingObject()
			if got := matchesNotificationConfig(tt.cfg, &so, tt.eventType); got != tt.want {
				t.Errorf("matchesNotificationConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplitTopic(t *testing.T) {
	tests := []struct {
		topic     string
		wantProj  string
		wantTopic string
		wantErr   bool
	}{
		{"projects/my-project/topics/my-topic", "my-project", "my-topic", false},
		{"bad-format", "", "", true},
		{"projects/p/notopics/t", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			p, topic, err := splitTopic(tt.topic)
			if (err != nil) != tt.wantErr {
				t.Fatalf("splitTopic(%q) error = %v, wantErr %v", tt.topic, err, tt.wantErr)
			}
			if p != tt.wantProj || topic != tt.wantTopic {
				t.Errorf("splitTopic(%q) = (%q, %q), want (%q, %q)", tt.topic, p, topic, tt.wantProj, tt.wantTopic)
			}
		})
	}
}

func TestNotificationRegistry_ConcurrentInsert(t *testing.T) {
	r := NewNotificationRegistry(nil)
	var wg sync.WaitGroup
	const n = 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Insert("bucket", NotificationConfig{Topic: "projects/p/topics/t"})
		}()
	}
	wg.Wait()
	if len(r.List("bucket")) != n {
		t.Errorf("expected %d configs, got %d", n, len(r.List("bucket")))
	}
}
