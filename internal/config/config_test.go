// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package config

import (
	"github.com/fsouza/fake-gcs-server/internal/notification"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		args           []string
		expectedConfig Config
		expectErr      bool
	}{
		{
			name: "all parameters",
			args: []string{
				"-backend", "memory",
				"-filesystem-root", "/tmp/something",
				"-public-host", "127.0.0.1.nip.io:8443",
				"-external-url", "https://myhost.example.com:8443",
				"-cors-headers", "X-Goog-Meta-Uploader",
				"-host", "127.0.0.1",
				"-port", "443",
				"-data", "/var/gcs",
				"-scheme", "http",
				"-event.pubsub-project-id", "test-project",
				"-event.pubsub-topic", "gcs-events",
				"-event.object-prefix", "uploads/",
				"-event.list", "finalize,delete,metadataUpdate,archive",
			},
			expectedConfig: Config{
				Seed:               "/var/gcs",
				backend:            "memory",
				fsRoot:             "/tmp/something",
				publicHost:         "127.0.0.1.nip.io:8443",
				externalURL:        "https://myhost.example.com:8443",
				allowedCORSHeaders: []string{"X-Goog-Meta-Uploader"},
				host:               "127.0.0.1",
				port:               443,
				scheme:             "http",
				event: EventConfig{
					pubsubProjectID: "test-project",
					pubsubTopic:     "gcs-events",
					prefix:          "uploads/",
					list:            []string{"finalize", "delete", "metadataUpdate", "archive"},
				},
			},
		},
		{
			name: "default parameters",
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "",
				allowedCORSHeaders: nil,
				host:               "0.0.0.0",
				port:               4443,
				scheme:             "https",
				event: EventConfig{
					list: []string{"finalize"},
				},
			},
		},
		{
			name:      "invalid port value type",
			args:      []string{"-port", "not-a-number"},
			expectErr: true,
		},
		{
			name:      "invalid port value",
			args:      []string{"-port", "65536"},
			expectErr: true,
		},
		{
			name:      "invalid backend",
			args:      []string{"-backend", "in-memory"},
			expectErr: true,
		},
		{
			name:      "filesystem backend with no root",
			args:      []string{"-backend", "filesystem", "-filesystem-root", ""},
			expectErr: true,
		},
		{
			name:      "missing event pubsub project ID",
			args:      []string{"-event.pubsub-topic", "gcs-events"},
			expectErr: true,
		},
		{
			name:      "missing event pubsub topic",
			args:      []string{"-event.pubsub-project-id", "test-project"},
			expectErr: true,
		},
		{
			name:      "invalid events",
			args:      []string{"-event.list", "invalid,stuff", "-event.pubsub-topic", "gcs-events", "-event.pubsub-project-id", "test-project"},
			expectErr: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := Load(test.args)
			if err != nil && !test.expectErr {
				t.Fatalf("unexpected non-nil error: %v", err)
			} else if err == nil && test.expectErr {
				t.Fatal("unexpected <nil> error")
			}
			if diff := cmp.Diff(cfg, test.expectedConfig, cmp.AllowUnexported(Config{}, EventConfig{})); !test.expectErr && diff != "" {
				t.Errorf("wrong config returned\nwant %#v\ngot  %#v\ndiff: %v", test.expectedConfig, cfg, diff)
			}
		})
	}
}

func TestToFakeGcsOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		config   Config
		expected fakestorage.Options
	}{
		{
			"filesystem",
			Config{
				backend:     "filesystem",
				fsRoot:      "/tmp/something",
				publicHost:  "127.0.0.1.nip.io:8443",
				externalURL: "https://myhost.example.com:8443",
				host:        "0.0.0.0",
				port:        443,
				event: EventConfig{
					pubsubProjectID: "test-project",
					pubsubTopic:     "gcs-events",
					prefix:          "uploads/",
					list:            []string{"finalize", "delete"},
				},
			},
			fakestorage.Options{
				StorageRoot: "/tmp/something",
				PublicHost:  "127.0.0.1.nip.io:8443",
				ExternalURL: "https://myhost.example.com:8443",
				Host:        "0.0.0.0",
				Port:        443,
				EventOptions: notification.EventManagerOptions{
					ProjectID:    "test-project",
					TopicName:    "gcs-events",
					ObjectPrefix: "uploads/",
					NotifyOn: notification.EventNotificationOptions{
						Finalize:       true,
						Delete:         true,
						MetadataUpdate: false,
					},
				},
			},
		},
		{
			"memory",
			Config{
				backend:     "memory",
				fsRoot:      "/tmp/something",
				publicHost:  "127.0.0.1.nip.io:8443",
				externalURL: "https://myhost.example.com:8443",
				host:        "0.0.0.0",
				port:        443,
			},
			fakestorage.Options{
				StorageRoot: "",
				PublicHost:  "127.0.0.1.nip.io:8443",
				ExternalURL: "https://myhost.example.com:8443",
				Host:        "0.0.0.0",
				Port:        443,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			opts := test.config.ToFakeGcsOptions()
			ignWriter := cmpopts.IgnoreFields(fakestorage.Options{}, "Writer")
			if diff := cmp.Diff(opts, test.expected, ignWriter); diff != "" {
				t.Errorf("wrong set of options returned\nwant %#v\ngot  %#v\ndiff: %v", test.expected, opts, diff)
			}
		})
	}
}
