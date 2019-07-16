// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/go-cmp/cmp"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		args           []string
		expectedConfig config
		expectErr      bool
	}{
		{
			name: "all parameters",
			args: []string{
				"-backend", "filesystem",
				"-filesystem-root", "/tmp/something",
				"-public-host", "127.0.0.1.nip.io:8443",
				"-external-url", "https://myhost.example.com:8443",
				"-host", "0.0.0.0",
				"-port", "443",
			},
			expectedConfig: config{
				backend:     "filesystem",
				fsRoot:      "/tmp/something",
				publicHost:  "127.0.0.1.nip.io:8443",
				externalURL: "https://myhost.example.com:8443",
				host:        "0.0.0.0",
				port:        443,
			},
		},
		{
			name: "default parameters",
			expectedConfig: config{
				backend:     "memory",
				fsRoot:      "",
				publicHost:  "storage.googleapis.com",
				externalURL: "",
				host:        "127.0.0.1",
				port:        8443,
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
			args:      []string{"-backend", "filesystem"},
			expectErr: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := loadConfig(test.args)
			if err != nil && !test.expectErr {
				t.Fatalf("uexpected non-nil error: %v", err)
			} else if err == nil && test.expectErr {
				t.Fatal("unexpected <nil> error")
			}
			if diff := cmp.Diff(cfg, test.expectedConfig, cmp.AllowUnexported(config{})); !test.expectErr && diff != "" {
				t.Errorf("wrong config returned\nwant %#v\ngot  %#v\ndiff: %v", test.expectedConfig, cfg, diff)
			}
		})
	}
}

func TestToFakeGcsOptions(t *testing.T) {
	cfg := config{
		backend:     "filesystem",
		fsRoot:      "/tmp/something",
		publicHost:  "127.0.0.1.nip.io:8443",
		externalURL: "https://myhost.example.com:8443",
		host:        "0.0.0.0",
		port:        443,
	}
	expected := fakestorage.Options{
		StorageRoot: "/tmp/something",
		PublicHost:  "127.0.0.1.nip.io:8443",
		ExternalURL: "https://myhost.example.com:8443",
		Host:        "0.0.0.0",
		Port:        443,
	}
	options := cfg.toFakeGcsOptions()

	if diff := cmp.Diff(options, expected); diff != "" {
		t.Errorf("wrong options generated\nwant %#v\ngot  %#v\ndiff: %v", expected, options, diff)
	}
}
