// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"math"
	"os"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

const (
	filesystemBackend = "filesystem"
	memoryBackend     = "memory"
)

type config struct {
	publicHost  string
	externalURL string
	host        string
	port        uint
	backend     string
	fsRoot      string
}

func loadConfig(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("gcs-emulator", flag.ContinueOnError)
	fs.StringVar(&cfg.backend, "backend", memoryBackend, "storage backend (memory or filesystem)")
	fs.StringVar(&cfg.fsRoot, "filesystem-root", "", "filesystem root (required for the filesystem backend). folder will be created if it doesn't exist")
	fs.StringVar(&cfg.publicHost, "public-host", "storage.googleapis.com", "Optional URL for public host")
	fs.StringVar(&cfg.externalURL, "external-url", "", "optional external URL, returned in the Location header for uploads. Defaults to the address where the server is running")
	fs.StringVar(&cfg.host, "host", "127.0.0.1", "host to bind to")
	fs.UintVar(&cfg.port, "port", 8443, "port to bind to")
	err := fs.Parse(args)
	if err != nil {
		return cfg, err
	}
	return cfg, cfg.validate()
}

func (c *config) toFakeGcsOptions() fakestorage.Options {
	return fakestorage.Options{
		StorageRoot: c.fsRoot,
		Host:        c.host,
		Port:        uint16(c.port),
		PublicHost:  c.publicHost,
		ExternalURL: c.externalURL,
	}
}

func (c *config) validate() error {
	if c.backend != memoryBackend && c.backend != filesystemBackend {
		return fmt.Errorf(`invalid backend %q, must be either "memory" or "filesystem"`, c.backend)
	}
	if c.backend == filesystemBackend && c.fsRoot == "" {
		return fmt.Errorf("backend %q requires the filesystem-root to be defined", c.backend)
	}
	if c.port > math.MaxUint16 {
		return fmt.Errorf("port %d is too high, maximum value is %d", c.port, math.MaxUint16)
	}
	if c.backend == filesystemBackend {
		if err := os.MkdirAll(c.fsRoot, 0700); err != nil {
			return fmt.Errorf("could not initialize filesystem at %q: %v", c.fsRoot, err)
		}
	}
	return nil
}
