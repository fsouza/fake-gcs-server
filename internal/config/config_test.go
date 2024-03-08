// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package config

import (
	"log/slog"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/fsouza/fake-gcs-server/internal/notification"
	"github.com/fsouza/slognil"
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
				"-port-http", "80",
				"-data", "/var/gcs",
				"-scheme", "both",
				"-event.pubsub-project-id", "test-project",
				"-event.pubsub-topic", "gcs-events",
				"-event.object-prefix", "uploads/",
				"-event.list", "finalize,delete,metadataUpdate,archive",
				"-location", "US-EAST1",
				"-log-level", "warn",
			},
			expectedConfig: Config{
				Seed:               "/var/gcs",
				backend:            "memory",
				fsRoot:             "/tmp/something",
				publicHost:         "127.0.0.1.nip.io:8443",
				externalURL:        "https://myhost.example.com:8443",
				allowedCORSHeaders: []string{"X-Goog-Meta-Uploader"},
				Host:               "127.0.0.1",
				Port:               443,
				PortHTTP:           80,
				Scheme:             "both",
				event: EventConfig{
					pubsubProjectID: "test-project",
					pubsubTopic:     "gcs-events",
					prefix:          "uploads/",
					list:            []string{"finalize", "delete", "metadataUpdate", "archive"},
				},
				bucketLocation: "US-EAST1",
				LogLevel:       slog.LevelWarn,
			},
		},
		{
			name: "default parameters",
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "https://0.0.0.0:4443",
				allowedCORSHeaders: nil,
				Host:               "0.0.0.0",
				Port:               4443,
				PortHTTP:           0,
				Scheme:             "https",
				event: EventConfig{
					list: []string{"finalize"},
				},
				bucketLocation: "US-CENTRAL1",
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "https scheme and default port",
			args: []string{
				"-scheme", "https",
			},
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "https://0.0.0.0:4443",
				allowedCORSHeaders: nil,
				Host:               "0.0.0.0",
				Port:               4443,
				PortHTTP:           0,
				Scheme:             "https",
				event: EventConfig{
					list: []string{"finalize"},
				},
				bucketLocation: "US-CENTRAL1",
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "http scheme and default port",
			args: []string{
				"-scheme", "http",
			},
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "http://0.0.0.0:4443",
				allowedCORSHeaders: nil,
				Host:               "0.0.0.0",
				Port:               4443,
				PortHTTP:           0,
				Scheme:             "http",
				event: EventConfig{
					list: []string{"finalize"},
				},
				bucketLocation: "US-CENTRAL1",
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "https scheme and non-default port",
			args: []string{
				"-port", "5553",
				"-scheme", "https",
			},
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "https://0.0.0.0:5553",
				allowedCORSHeaders: nil,
				Host:               "0.0.0.0",
				Port:               5553,
				PortHTTP:           0,
				Scheme:             "https",
				event: EventConfig{
					list: []string{"finalize"},
				},
				bucketLocation: "US-CENTRAL1",
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "http scheme and non-default port",
			args: []string{
				"-port", "9000",
				"-scheme", "http",
			},
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "http://0.0.0.0:9000",
				allowedCORSHeaders: nil,
				Host:               "0.0.0.0",
				Port:               9000,
				PortHTTP:           0,
				Scheme:             "http",
				event: EventConfig{
					list: []string{"finalize"},
				},
				bucketLocation: "US-CENTRAL1",
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "'both' scheme and default ports",
			args: []string{
				"-scheme", "both",
			},
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "https://0.0.0.0:4443",
				allowedCORSHeaders: nil,
				Host:               "0.0.0.0",
				Port:               4443,
				PortHTTP:           8000,
				Scheme:             "both",
				event: EventConfig{
					list: []string{"finalize"},
				},
				bucketLocation: "US-CENTRAL1",
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "'both' scheme with non-default https port and default http",
			args: []string{
				"-port", "5553",
				"-scheme", "both",
			},
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "https://0.0.0.0:5553",
				allowedCORSHeaders: nil,
				Host:               "0.0.0.0",
				Port:               5553,
				PortHTTP:           8000,
				Scheme:             "both",
				event: EventConfig{
					list: []string{"finalize"},
				},
				bucketLocation: "US-CENTRAL1",
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "'both' scheme with default https port and non-default http",
			args: []string{
				"-port-http", "9000",
				"-scheme", "both",
			},
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "https://0.0.0.0:4443",
				allowedCORSHeaders: nil,
				Host:               "0.0.0.0",
				Port:               4443,
				PortHTTP:           9000,
				Scheme:             "both",
				event: EventConfig{
					list: []string{"finalize"},
				},
				bucketLocation: "US-CENTRAL1",
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "'both' scheme with non-default ports",
			args: []string{
				"-port", "5553",
				"-port-http", "9000",
				"-scheme", "both",
			},
			expectedConfig: Config{
				Seed:               "",
				backend:            "filesystem",
				fsRoot:             "/storage",
				publicHost:         "storage.googleapis.com",
				externalURL:        "https://0.0.0.0:5553",
				allowedCORSHeaders: nil,
				Host:               "0.0.0.0",
				Port:               5553,
				PortHTTP:           9000,
				Scheme:             "both",
				event: EventConfig{
					list: []string{"finalize"},
				},
				bucketLocation: "US-CENTRAL1",
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name:      "invalid port value type",
			args:      []string{"-port", "not-a-number"},
			expectErr: true,
		},
		{
			name:      "invalid port-http value type",
			args:      []string{"-port-http", "not-a-number"},
			expectErr: true,
		},
		{
			name:      "invalid port value",
			args:      []string{"-port", "65536"},
			expectErr: true,
		},
		{
			name:      "invalid port-http value",
			args:      []string{"-port-http", "65536"},
			expectErr: true,
		},
		{
			name:      "invalid scheme value",
			args:      []string{"-scheme", "wrong-scheme-value"},
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
		{
			name:      "invalid log level",
			args:      []string{"-log-level", "non-existent-level"},
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
				Host:        "0.0.0.0",
				Port:        443,
				Scheme:      "https",
				event: EventConfig{
					pubsubProjectID: "test-project",
					pubsubTopic:     "gcs-events",
					bucket:          "my-bucket",
					prefix:          "uploads/",
					list:            []string{"finalize", "delete"},
				},
				bucketLocation: "US-EAST1",
			},
			fakestorage.Options{
				StorageRoot: "/tmp/something",
				PublicHost:  "127.0.0.1.nip.io:8443",
				ExternalURL: "https://myhost.example.com:8443",
				Host:        "0.0.0.0",
				Port:        443,
				Scheme:      "https",
				EventOptions: notification.EventManagerOptions{
					ProjectID:    "test-project",
					TopicName:    "gcs-events",
					Bucket:       "my-bucket",
					ObjectPrefix: "uploads/",
					NotifyOn: notification.EventNotificationOptions{
						Finalize:       true,
						Delete:         true,
						MetadataUpdate: false,
					},
				},
				BucketsLocation: "US-EAST1",
				NoListener:      true,
			},
		},
		{
			"memory",
			Config{
				backend:     "memory",
				fsRoot:      "/tmp/something",
				publicHost:  "127.0.0.1.nip.io:8443",
				externalURL: "https://myhost.example.com:8443",
				Host:        "0.0.0.0",
				Port:        443,
				Scheme:      "https",
			},
			fakestorage.Options{
				StorageRoot: "",
				PublicHost:  "127.0.0.1.nip.io:8443",
				ExternalURL: "https://myhost.example.com:8443",
				Host:        "0.0.0.0",
				Port:        443,
				Scheme:      "https",
				NoListener:  true,
			},
		},
		{
			"external-url with trailing slashes",
			Config{
				backend:     "memory",
				fsRoot:      "/tmp/something",
				publicHost:  "127.0.0.1.nip.io:8443",
				externalURL: "https://myhost.example.com:8443/",
				Host:        "0.0.0.0",
				Port:        443,
				Scheme:      "https",
			},
			fakestorage.Options{
				StorageRoot: "",
				PublicHost:  "127.0.0.1.nip.io:8443",
				ExternalURL: "https://myhost.example.com:8443",
				Host:        "0.0.0.0",
				Port:        443,
				Scheme:      "https",
				NoListener:  true,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			opts := test.config.ToFakeGcsOptions(slognil.NewLogger(), test.config.Scheme)
			ignWriter := cmpopts.IgnoreFields(fakestorage.Options{}, "Writer")
			if diff := cmp.Diff(opts, test.expected, ignWriter); diff != "" {
				t.Errorf("wrong set of options returned\nwant %#v\ngot  %#v\ndiff: %v", test.expected, opts, diff)
			}
		})
	}
}
