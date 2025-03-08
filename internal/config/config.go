// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package config provides utilities for managing fake-gcs-server's
// configuration using command line flags.
package config

import (
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/fsouza/fake-gcs-server/internal/notification"
)

const (
	filesystemBackend   = "filesystem"
	memoryBackend       = "memory"
	eventFinalize       = "finalize"
	eventDelete         = "delete"
	eventMetadataUpdate = "metadataUpdate"
	eventArchive        = "archive"
	defaultHTTPSPort    = 4443
	defaultHTTPPort     = 8000
	schemeHTTPS         = "https"
	schemeHTTP          = "http"
	schemeBoth          = "both"
	flagPort            = "port"
	flagPortHTTP        = "port-http"
)

type Config struct {
	Scheme              string
	Seed                string
	Host                string
	Port                uint
	PortHTTP            uint
	CertificateLocation string
	PrivateKeyLocation  string

	publicHost         string
	externalURL        string
	allowedCORSHeaders []string
	backend            string
	fsRoot             string
	event              EventConfig
	bucketLocation     string
	LogLevel           slog.Level
}

type EventConfig struct {
	pubsubProjectID string
	pubsubTopic     string
	bucket          string
	prefix          string
	list            []string
}

// envVarOrDefault retrieves an environment variable value and converts it to type T,
// or returns the default value if the environment variable is not set or cannot be converted.
func envVarOrDefault[T string | uint](key string, defaultValue T, convert func(string) (T, error)) T {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if converted, err := convert(val); err == nil {
			return converted
		}
	}
	return defaultValue
}

// Load parses the given arguments list and return a config object (and/or an
// error in case of failures).
func Load(args []string) (Config, error) {
	var cfg Config
	var allowedCORSHeaders string
	var eventList string
	var logLevel string

	envVarOrDefaultPort := envVarOrDefault("FAKE_GCS_PORT", defaultHTTPSPort, func(s string) (uint, error) {
		val, err := strconv.ParseUint(s, 10, 32)
		return uint(val), err
	})
	envVarOrDefaultPortHTTP := envVarOrDefault("FAKE_GCS_PORT_HTTP", defaultHTTPPort, func(s string) (uint, error) {
		val, err := strconv.ParseUint(s, 10, 32)
		return uint(val), err
	})

	fs := flag.NewFlagSet("fake-gcs-server", flag.ContinueOnError)
	fs.StringVar(&cfg.backend, "backend", envVarOrDefault("FAKE_GCS_BACKEND", filesystemBackend, func(s string) (string, error) {
		return s, nil
	}), "storage backend (memory or filesystem)")
	fs.StringVar(&cfg.fsRoot, "filesystem-root", envVarOrDefault("FAKE_GCS_FILESYSTEM_ROOT", "/storage", func(s string) (string, error) {
		return s, nil
	}), "filesystem root (required for the filesystem backend). folder will be created if it doesn't exist")
	fs.StringVar(&cfg.publicHost, "public-host", envVarOrDefault("FAKE_GCS_PUBLIC_HOST", "storage.googleapis.com", func(s string) (string, error) {
		return s, nil
	}), "Optional URL for public host")
	fs.StringVar(&cfg.externalURL, "external-url", envVarOrDefault("FAKE_GCS_EXTERNAL_URL", "", func(s string) (string, error) {
		return s, nil
	}), "optional external URL, returned in the Location header for uploads. Defaults to the address where the server is running")
	fs.StringVar(&cfg.Scheme, "scheme", envVarOrDefault("FAKE_GCS_SCHEME", schemeHTTPS, func(s string) (string, error) {
		return s, nil
	}), "using 'http' or 'https' or 'both'")
	fs.StringVar(&cfg.Host, "host", envVarOrDefault("FAKE_GCS_HOST", "0.0.0.0", func(s string) (string, error) {
		return s, nil
	}), "host to bind to")
	fs.StringVar(&cfg.Seed, "data", envVarOrDefault("FAKE_GCS_DATA", "", func(s string) (string, error) {
		return s, nil
	}), "where to load data from (provided that the directory exists)")
	fs.StringVar(&allowedCORSHeaders, "cors-headers", envVarOrDefault("FAKE_GCS_CORS_HEADERS", "", func(s string) (string, error) {
		return s, nil
	}), "comma separated list of headers to add to the CORS allowlist")
	fs.UintVar(&cfg.Port, "port", envVarOrDefaultPort, "port to bind to")
	fs.UintVar(&cfg.PortHTTP, flagPortHTTP, envVarOrDefaultPortHTTP, "used only when scheme is 'both' as the port to bind http to")
	fs.StringVar(&cfg.event.pubsubProjectID, "event.pubsub-project-id", envVarOrDefault("FAKE_GCS_EVENT_PUBSUB_PROJECT_ID", "", func(s string) (string, error) {
		return s, nil
	}), "project ID containing the pubsub topic")
	fs.StringVar(&cfg.event.pubsubTopic, "event.pubsub-topic", envVarOrDefault("FAKE_GCS_EVENT_PUBSUB_TOPIC", "", func(s string) (string, error) {
		return s, nil
	}), "pubsub topic name to publish events on")
	fs.StringVar(&cfg.event.bucket, "event.bucket", envVarOrDefault("FAKE_GCS_EVENT_BUCKET", "", func(s string) (string, error) {
		return s, nil
	}), "if not empty, only objects in this bucket will generate trigger events")
	fs.StringVar(&cfg.event.prefix, "event.object-prefix", envVarOrDefault("FAKE_GCS_EVENT_OBJECT_PREFIX", "", func(s string) (string, error) {
		return s, nil
	}), "if not empty, only objects having this prefix will generate trigger events")
	fs.StringVar(&eventList, "event.list", envVarOrDefault("FAKE_GCS_EVENT_LIST", eventFinalize, func(s string) (string, error) {
		return s, nil
	}), "comma separated list of events to publish on cloud function URl. Options are: finalize, delete, and metadataUpdate")
	fs.StringVar(&cfg.bucketLocation, "location", envVarOrDefault("FAKE_GCS_LOCATION", "US-CENTRAL1", func(s string) (string, error) {
		return s, nil
	}), "location for buckets")
	fs.StringVar(&cfg.CertificateLocation, "cert-location", envVarOrDefault("FAKE_GCS_CERT_LOCATION", "", func(s string) (string, error) {
		return s, nil
	}), "location for server certificate")
	fs.StringVar(&cfg.PrivateKeyLocation, "private-key-location", envVarOrDefault("FAKE_GCS_PRIVATE_KEY_LOCATION", "", func(s string) (string, error) {
		return s, nil
	}), "location for private key")
	fs.StringVar(&logLevel, "log-level", envVarOrDefault("FAKE_GCS_LOG_LEVEL", "info", func(s string) (string, error) {
		return s, nil
	}), "level for logging. Options same as for logrus: trace, debug, info, warn, error, fatal, and panic")

	err := fs.Parse(args)
	if err != nil {
		return cfg, err
	}

	// Create a map to store the flags and their values
	setFlags := make(map[string]interface{})

	// Check if a flag was used using Visit
	fs.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = f.Value
	})

	// setting default values, if not provided, for port - mind that the default port is 4443 regardless of the scheme
	if _, ok := setFlags[flagPort]; !ok {
		cfg.Port = envVarOrDefaultPort
	}

	if _, ok := setFlags[flagPortHTTP]; !ok {
		if cfg.Scheme == schemeBoth {
			cfg.PortHTTP = envVarOrDefaultPortHTTP
		} else {
			cfg.PortHTTP = 0
		}
	}

	levels := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"warning": slog.LevelWarn,
		"warn":    slog.LevelWarn,
		"error":   slog.LevelError,
	}
	if level, ok := levels[logLevel]; ok {
		cfg.LogLevel = level
	} else {
		return cfg, fmt.Errorf("invalid log level %q", logLevel)
	}

	if allowedCORSHeaders != "" {
		cfg.allowedCORSHeaders = strings.Split(allowedCORSHeaders, ",")
	}
	if eventList != "" {
		cfg.event.list = strings.Split(eventList, ",")
	}

	if cfg.externalURL == "" {
		if cfg.Scheme != "both" {
			cfg.externalURL = fmt.Sprintf("%s://%s:%d", cfg.Scheme, cfg.Host, cfg.Port)
		} else {
			// for scheme 'both' taking externalURL as HTTPs by default
			cfg.externalURL = fmt.Sprintf("%s://%s:%d", schemeHTTPS, cfg.Host, cfg.Port)
		}
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
	if c.Scheme != schemeHTTP && c.Scheme != schemeHTTPS && c.Scheme != schemeBoth {
		return fmt.Errorf(`invalid scheme %s, must be either "%s", "%s" or "%s"`, c.Scheme, schemeHTTP, schemeHTTPS, schemeBoth)
	}
	if c.Port > math.MaxUint16 {
		return fmt.Errorf("port %d is too high, maximum value is %d", c.Port, math.MaxUint16)
	}
	if c.PortHTTP > math.MaxUint16 {
		return fmt.Errorf("port-http %d is too high, maximum value is %d", c.PortHTTP, math.MaxUint16)
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

func (c *Config) ToFakeGcsOptions(logger *slog.Logger, scheme string) fakestorage.Options {
	storageRoot := c.fsRoot
	if c.backend == memoryBackend {
		storageRoot = ""
	}
	eventOptions := notification.EventManagerOptions{
		ProjectID:    c.event.pubsubProjectID,
		TopicName:    c.event.pubsubTopic,
		Bucket:       c.event.bucket,
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
	port := c.Port
	if c.Scheme == schemeBoth && scheme == schemeHTTP {
		port = c.PortHTTP // this cli flag, for port http, is relevant only when scheme is both
	}
	opts := fakestorage.Options{
		StorageRoot:         storageRoot,
		Seed:                c.Seed,
		Scheme:              scheme,
		Host:                c.Host,
		Port:                uint16(port),
		PublicHost:          c.publicHost,
		ExternalURL:         strings.TrimRight(c.externalURL, "/"),
		AllowedCORSHeaders:  c.allowedCORSHeaders,
		Writer:              &slogWriter{logger: logger, level: slog.LevelInfo},
		EventOptions:        eventOptions,
		BucketsLocation:     c.bucketLocation,
		CertificateLocation: c.CertificateLocation,
		PrivateKeyLocation:  c.PrivateKeyLocation,
		NoListener:          true,
	}
	return opts
}
