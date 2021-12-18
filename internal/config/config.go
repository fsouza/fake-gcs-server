// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package config provides utilities for managing fake-gcs-server's
// configuration using command line flags.
package config

import (
	"flag"
	"fmt"
	"github.com/fsouza/fake-gcs-server/internal/notification"
	"math"
	"strings"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/sirupsen/logrus"
)

const (
	filesystemBackend   = "filesystem"
	memoryBackend       = "memory"
	eventFinalize       = "finalize"
	eventDelete         = "delete"
	eventMetadataUpdate = "metadataUpdate"
	eventArchive        = "archive"
)

type Config struct {
	Seed               string
	publicHost         string
	externalURL        string
	allowedCORSHeaders []string
	scheme             string
	host               string
	port               uint
	backend            string
	fsRoot             string
	event              EventConfig
}

type EventConfig struct {
	pubsubProjectID string
	pubsubTopic     string
	prefix          string
	list            []string
}

// Load parses the given arguments list and return a config object (and/or an
// error in case of failures).
func Load(args []string) (Config, error) {
	var cfg Config
	var allowedCORSHeaders string
	var eventList string

	fs := flag.NewFlagSet("fake-gcs-server", flag.ContinueOnError)
	fs.StringVar(&cfg.backend, "backend", filesystemBackend, "storage backend (memory or filesystem)")
	fs.StringVar(&cfg.fsRoot, "filesystem-root", "/storage", "filesystem root (required for the filesystem backend). folder will be created if it doesn't exist")
	fs.StringVar(&cfg.publicHost, "public-host", "storage.googleapis.com", "Optional URL for public host")
	fs.StringVar(&cfg.externalURL, "external-url", "", "optional external URL, returned in the Location header for uploads. Defaults to the address where the server is running")
	fs.StringVar(&cfg.scheme, "scheme", "https", "using http or https")
	fs.StringVar(&cfg.host, "host", "0.0.0.0", "host to bind to")
	fs.StringVar(&cfg.Seed, "data", "", "where to load data from (provided that the directory exists)")
	fs.StringVar(&allowedCORSHeaders, "cors-headers", "", "comma separated list of headers to add to the CORS allowlist")
	fs.UintVar(&cfg.port, "port", 4443, "port to bind to")
	fs.StringVar(&cfg.event.pubsubProjectID, "event.pubsub-project-id", "", "project ID containing the pubsub topic")
	fs.StringVar(&cfg.event.pubsubTopic, "event.pubsub-topic", "", "pubsub topic name to publish events on")
	fs.StringVar(&cfg.event.prefix, "event.object-prefix", "", "if not empty, only objects having this prefix will generate trigger events")
	fs.StringVar(&eventList, "event.list", eventFinalize, "comma separated list of events to publish on cloud function URl. Options are: finalize, delete, and metadataUpdate")

	err := fs.Parse(args)
	if err != nil {
		return cfg, err
	}

	if allowedCORSHeaders != "" {
		cfg.allowedCORSHeaders = strings.Split(allowedCORSHeaders, ",")
	}
	if eventList != "" {
		cfg.event.list = strings.Split(eventList, ",")
	}

	return cfg, cfg.validate()
}

func (c *Config) validate() error {
	if c.backend != memoryBackend && c.backend != filesystemBackend {
		return fmt.Errorf(`invalid backend %q, must be either "memory" or "filesystem"`, c.backend)
	}
	if c.backend == filesystemBackend && c.fsRoot == "" {
		return fmt.Errorf("backend %q requires the filesystem-root to be defined", c.backend)
	}
	if c.scheme != "http" && c.scheme != "https" {
		return fmt.Errorf(`invalid scheme %s, must be either "http"" or "https"`, c.scheme)
	}
	if c.port > math.MaxUint16 {
		return fmt.Errorf("port %d is too high, maximum value is %d", c.port, math.MaxUint16)
	}

	return c.event.validate()
}

func (c *EventConfig) validate() error {
	switch c.pubsubProjectID {
	case "":
		if c.pubsubTopic != "" {
			return fmt.Errorf("missing event pubsub project ID")
		}
	default:
		if c.pubsubTopic == "" {
			return fmt.Errorf("missing event pubsub topic ID")
		}
		for i, event := range c.list {
			e := strings.TrimSpace(event)
			switch e {
			case eventFinalize, eventDelete, eventMetadataUpdate, eventArchive:
			default:
				return fmt.Errorf("%s is an invalid event", e)
			}
			c.list[i] = e
		}
		if len(c.list) == 0 {
			return fmt.Errorf("event list cannot be empty")
		}
	}

	return nil
}

func (c *Config) ToFakeGcsOptions() fakestorage.Options {
	storageRoot := c.fsRoot
	if c.backend == memoryBackend {
		storageRoot = ""
	}
	eventOptions := notification.EventManagerOptions{
		ProjectID:    c.event.pubsubProjectID,
		TopicName:    c.event.pubsubTopic,
		ObjectPrefix: c.event.prefix,
	}
	if c.event.pubsubProjectID != "" && c.event.pubsubTopic != "" {
		for _, event := range c.event.list {
			switch event {
			case eventFinalize:
				eventOptions.NotifyOn.Finalize = true
			case eventDelete:
				eventOptions.NotifyOn.Delete = true
			case eventMetadataUpdate:
				eventOptions.NotifyOn.MetadataUpdate = true
			case eventArchive:
				eventOptions.NotifyOn.Archive = true
			}
		}
	}

	return fakestorage.Options{
		StorageRoot:        storageRoot,
		Scheme:             c.scheme,
		Host:               c.host,
		Port:               uint16(c.port),
		PublicHost:         c.publicHost,
		ExternalURL:        c.externalURL,
		AllowedCORSHeaders: c.allowedCORSHeaders,
		Writer:             logrus.New().Writer(),
		EventOptions:       eventOptions,
	}
}
