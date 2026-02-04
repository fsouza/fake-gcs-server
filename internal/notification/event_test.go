// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package notification

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	"cloud.google.com/go/pubsub/v2"
	"github.com/fsouza/fake-gcs-server/internal/backend"
)

type mockPublisher struct {
	lastMessage *pubsub.Message
}

func (m *mockPublisher) Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {
	m.lastMessage = msg
	return nil
}

func TestPubsubEventManager_Trigger(t *testing.T) {
	newMetadata := map[string]string{
		"1-key": "1.1-value",
		"2-key": "2-value",
		"3-key": "3-value",
	}
	tests := []struct {
		name      string
		notifyOn  EventNotificationOptions
		eventType string
		bucket    string
		prefix    string
		metadata  map[string]string
	}{
		{
			"None",
			EventNotificationOptions{},
			"",
			"",
			"",
			nil,
		},
		{
			"Finalize enabled, no bucket",
			EventNotificationOptions{
				Finalize: true,
			},
			string(EventFinalize),
			"",
			"",
			nil,
		},
		{
			"Finalize enabled, matching bucket",
			EventNotificationOptions{
				Finalize: true,
			},
			string(EventFinalize),
			"some-bucket",
			"",
			nil,
		},
		{
			"Finalize enabled, non-matching bucket",
			EventNotificationOptions{
				Finalize: true,
			},
			"",
			"some-unmatched-bucket",
			"",
			nil,
		},
		{
			"Finalize enabled, no prefix",
			EventNotificationOptions{
				Finalize: true,
			},
			string(EventFinalize),
			"",
			"",
			nil,
		},
		{
			"Finalize enabled, matching prefix",
			EventNotificationOptions{
				Finalize: true,
			},
			string(EventFinalize),
			"",
			"files/",
			nil,
		},
		{
			"Finalize enabled, non-matching prefix",
			EventNotificationOptions{
				Finalize: true,
			},
			"",
			"",
			"uploads/",
			nil,
		},
		{
			"Delete enabled",
			EventNotificationOptions{
				Delete: true,
			},
			string(EventDelete),
			"",
			"",
			nil,
		},
		{
			"Metadata update enabled",
			EventNotificationOptions{
				MetadataUpdate: true,
			},
			string(EventMetadata),
			"",
			"",
			newMetadata,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			content := []byte("something")
			bufferedObj := backend.Object{
				ObjectAttrs: backend.ObjectAttrs{
					BucketName: "some-bucket",
					Name:       "files/txt/text-01.txt",
					Size:       int64(len(content)),
				},
				Content: content,
			}
			obj := bufferedObj.StreamingObject()
			eventManager := PubsubEventManager{
				notifyOn:     test.notifyOn,
				bucket:       test.bucket,
				objectPrefix: test.prefix,
			}
			publisher := &mockPublisher{}
			eventManager.publisher = publisher
			eventManager.publishSynchronously = true

			eventManager.Trigger(&obj, EventFinalize, nil)
			eventManager.Trigger(&obj, EventDelete, nil)
			eventManager.Trigger(&obj, EventArchive, nil)
			if test.metadata != nil {
				obj.Metadata = test.metadata
			}
			eventManager.Trigger(&obj, EventMetadata, nil)

			receivedMessage := publisher.lastMessage
			switch test.eventType {
			case "":
				if receivedMessage != nil {
					t.Errorf("expecting no event but got %v", receivedMessage)
				}
			default:
				if receivedMessage == nil || receivedMessage.Attributes == nil {
					t.Errorf("expecting event %q but got %v", test.eventType, receivedMessage)
				} else {
					if test.eventType != receivedMessage.Attributes["eventType"] {
						t.Errorf("wrong event type\nwant %q\ngot %q", test.eventType, receivedMessage.Attributes["eventType"])
					}
					var receivedEvent gcsEvent
					if err := json.Unmarshal(receivedMessage.Data, &receivedEvent); err != nil {
						t.Errorf("invalid event payload: %v", err)
					}
					if obj.BucketName != receivedEvent.Bucket {
						t.Errorf("wrong bucket on object\nwant %q\ngot %q", obj.BucketName, receivedEvent.Bucket)
					}
					if obj.Name != receivedEvent.Name {
						t.Errorf("wrong object name\nwant %q\ngot %q", obj.Name, receivedEvent.Name)
					}
					if int64(len(bufferedObj.Content)) != receivedEvent.Size {
						t.Errorf("wrong object size\nwant %q\ngot %q", strconv.Itoa(len(bufferedObj.Content)), receivedEvent.Size)
					}
					if !reflect.DeepEqual(test.metadata, receivedEvent.MetaData) {
						t.Errorf("wrong object metadata\nwant %q\ngot %q", test.metadata, receivedEvent.MetaData)
					}
				}
			}
		})
	}
}

func TestMultiEventManager_Trigger(t *testing.T) {
	t.Parallel()
	content := []byte("something")
	newObject := func(bucket string) backend.StreamingObject {
		obj := backend.Object{
			ObjectAttrs: backend.ObjectAttrs{
				BucketName: bucket,
				Name:       "files/obj.txt",
				Size:       int64(len(content)),
			},
			Content: content,
		}
		return obj.StreamingObject()
	}

	tests := []struct {
		name            string
		managerA        *PubsubEventManager
		managerB        *PubsubEventManager
		triggerBucket   string
		expectAReceived bool
		expectBReceived bool
	}{
		{
			name: "both managers no bucket filter",
			managerA: &PubsubEventManager{
				notifyOn:             EventNotificationOptions{Finalize: true},
				publishSynchronously: true,
			},
			managerB: &PubsubEventManager{
				notifyOn:             EventNotificationOptions{Finalize: true},
				publishSynchronously: true,
			},
			triggerBucket:   "bucket-x",
			expectAReceived: true,
			expectBReceived: true,
		},
		{
			name: "only manager A matches bucket",
			managerA: &PubsubEventManager{
				notifyOn:             EventNotificationOptions{Finalize: true},
				bucket:               "bucket-x",
				publishSynchronously: true,
			},
			managerB: &PubsubEventManager{
				notifyOn:             EventNotificationOptions{Finalize: true},
				bucket:               "bucket-y",
				publishSynchronously: true,
			},
			triggerBucket:   "bucket-x",
			expectAReceived: true,
			expectBReceived: false,
		},
		{
			name: "only manager B matches bucket",
			managerA: &PubsubEventManager{
				notifyOn:             EventNotificationOptions{Finalize: true},
				bucket:               "bucket-x",
				publishSynchronously: true,
			},
			managerB: &PubsubEventManager{
				notifyOn:             EventNotificationOptions{Finalize: true},
				bucket:               "bucket-y",
				publishSynchronously: true,
			},
			triggerBucket:   "bucket-y",
			expectAReceived: false,
			expectBReceived: true,
		},
		{
			name: "neither manager matches bucket",
			managerA: &PubsubEventManager{
				notifyOn:             EventNotificationOptions{Finalize: true},
				bucket:               "bucket-x",
				publishSynchronously: true,
			},
			managerB: &PubsubEventManager{
				notifyOn:             EventNotificationOptions{Finalize: true},
				bucket:               "bucket-y",
				publishSynchronously: true,
			},
			triggerBucket:   "bucket-z",
			expectAReceived: false,
			expectBReceived: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pubA := &mockPublisher{}
			pubB := &mockPublisher{}
			test.managerA.publisher = pubA
			test.managerB.publisher = pubB

			multi := NewMultiEventManager([]EventManager{test.managerA, test.managerB})
			obj := newObject(test.triggerBucket)
			multi.Trigger(&obj, EventFinalize, nil)

			if test.expectAReceived && pubA.lastMessage == nil {
				t.Error("manager A: expected to receive event, got nil")
			}
			if !test.expectAReceived && pubA.lastMessage != nil {
				t.Errorf("manager A: expected no event, got %v", pubA.lastMessage)
			}
			if test.expectBReceived && pubB.lastMessage == nil {
				t.Error("manager B: expected to receive event, got nil")
			}
			if !test.expectBReceived && pubB.lastMessage != nil {
				t.Errorf("manager B: expected no event, got %v", pubB.lastMessage)
			}
		})
	}

	t.Run("empty managers slice", func(t *testing.T) {
		t.Parallel()
		multi := NewMultiEventManager(nil)
		obj := newObject("any-bucket")
		multi.Trigger(&obj, EventFinalize, nil) // must not panic
	})
}
