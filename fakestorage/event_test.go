// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)

func TestServerEventNotification(t *testing.T) {
	newMetadata := map[string]string{
		"1-key": "1.1-value",
		"2-key": "2-value",
		"3-key": "3-value",
	}
	tests := []struct {
		name      string
		notifyOn  EventNotificationOptions
		eventType string
		prefix    string
		metadata  map[string]string
	}{
		{
			"Finalize enabled, no prefix",
			EventNotificationOptions{
				Finalize: true,
			},
			string(EventFinalize),
			"",
			nil,
		},
		{
			"Finalize enabled, matching prefix",
			EventNotificationOptions{
				Finalize: true,
			},
			string(EventFinalize),
			"files/",
			nil,
		},
		{
			"Finalize enabled, non-matching prefix",
			EventNotificationOptions{
				Finalize: true,
			},
			"",
			"uploads/",
			nil,
		},
		{
			"Finalize disabled",
			EventNotificationOptions{
				Finalize: false,
			},
			"",
			"",
			nil,
		},
		{
			"Delete enabled",
			EventNotificationOptions{
				Delete: true,
			},
			string(EventDelete),
			"",
			nil,
		},
		{
			"Delete disabled",
			EventNotificationOptions{
				Delete: false,
			},
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
			newMetadata,
		},
		{
			"Metadata update disabled",
			EventNotificationOptions{
				MetadataUpdate: false,
			},
			"",
			"",
			nil,
		},
	}
	obj := Object{BucketName: "some-bucket", Name: "files/txt/text-01.txt", Content: []byte("something")}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			opts := Options{
				EventOptions: EventManagerOptions{
					ObjectPrefix: test.prefix,
					NotifyOn:     test.notifyOn,
				},
			}
			server, err := NewServerWithOptions(opts)
			if err != nil {
				t.Fatal(err)
			}
			publisher := &mockPublisher{}
			server.eventManager.publisher = publisher
			server.eventManager.publishSynchronously = true
			server.createObject(obj)

			if opts.EventOptions.NotifyOn.Delete {
				client := server.Client()
				objHandle := client.Bucket(obj.BucketName).Object(obj.Name)
				err := objHandle.Delete(context.TODO())
				if err != nil {
					t.Fatal(err)
				}
			}
			if opts.EventOptions.NotifyOn.MetadataUpdate {
				client := server.Client()
				objHandle := client.Bucket(obj.BucketName).Object(obj.Name)
				_, err := objHandle.Update(context.TODO(), storage.ObjectAttrsToUpdate{Metadata: newMetadata})
				if err != nil {
					t.Fatal(err)
				}
			}
			server.Stop()
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
						t.Errorf("wrong objectc name\nwant %q\ngot %q", obj.Name, receivedEvent.Name)
					}
					if strconv.Itoa(len(obj.Content)) != receivedEvent.Size {
						t.Errorf("wrong object size\nwant %q\ngot %q", strconv.Itoa(len(obj.Content)), receivedEvent.Size)
					}
					if !reflect.DeepEqual(test.metadata, receivedEvent.MetaData) {
						t.Errorf("wrong object metadata\nwant %q\ngot %q", test.metadata, receivedEvent.MetaData)
					}
				}
			}
		})
	}
}

type mockPublisher struct {
	lastMessage *pubsub.Message
}

func (m *mockPublisher) Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {
	m.lastMessage = msg
	return nil
}
