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
	events             eventConfigFlag
	parsedEvents       []notification.EventManagerOptions
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

// eventConfigFlag is a flag.Value that accumulates repeated -event.config
// values. Each invocation of Set appends the raw string; validation and
// conversion into EventManagerOptions happens in Config.validate.
type eventConfigFlag []string

func (f *eventConfigFlag) String() string {
	if f == nil {
		return ""
	}
	return strings.Join(*f, ", ")
}

func (f *eventConfigFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

// parseEventConfig parses a single -event.config value into an
// EventManagerOptions.  The expected format is:
//
// bucket=<name>;project=<project>;topic=<topic>[;events=<e1>,<e2>][;prefix=<prefix>]
//
// bucket, project, and topic are required. events defaults to finalize if not
// specified. prefix is optional.
func parseEventConfig(raw string) (notification.EventManagerOptions, error) {
	var opts notification.EventManagerOptions
	var hasBucket, hasProject, hasTopic, hasEvents bool

	for _, field := range strings.Split(raw, ";") {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 {
			return opts, fmt.Errorf("invalid event config field %q: expected key=value format", field)
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		switch key {
		case "bucket":
			opts.Bucket = val
			hasBucket = true
		case "project":
			opts.ProjectID = val
			hasProject = true
		case "topic":
			opts.TopicName = val
			hasTopic = true
		case "events":
			notifyOn, err := eventListToNotifyOn(strings.Split(val, ","))
			if err != nil {
				return opts, err
			}
			opts.NotifyOn = notifyOn
			hasEvents = true
		case "prefix":
			opts.ObjectPrefix = val
		default:
			return opts, fmt.Errorf("unknown event config key %q", key)
		}
	}

	if !hasBucket {
		return opts, fmt.Errorf("event config missing required field \"bucket\"")
	}
	if !hasProject {
		return opts, fmt.Errorf("event config missing required field \"project\"")
	}
	if !hasTopic {
		return opts, fmt.Errorf("event config missing required field \"topic\"")
	}
	if !hasEvents {
		opts.NotifyOn = notification.EventNotificationOptions{Finalize: true}
	}
	return opts, nil
}

// eventListToNotifyOn validates a list of event name strings and maps them to an
// EventNotificationOptions struct.
func eventListToNotifyOn(events []string) (notification.EventNotificationOptions, error) {
	var notifyOn notification.EventNotificationOptions
	if len(events) == 0 {
		return notifyOn, fmt.Errorf("events list must not be empty")
	}
	for _, e := range events {
		switch strings.TrimSpace(e) {
		case eventFinalize:
			notifyOn.Finalize = true
		case eventDelete:
			notifyOn.Delete = true
		case eventMetadataUpdate:
			notifyOn.MetadataUpdate = true
		case eventArchive:
			notifyOn.Archive = true
		default:
			return notifyOn, fmt.Errorf("%q is an invalid event", strings.TrimSpace(e))
		}
	}
	return notifyOn, nil
}

// Load parses the given arguments list and return a config object (and/or an
// error in case of failures).
func Load(args []string) (Config, error) {
	var cfg Config
	var allowedCORSHeaders string
	var eventList string
	var logLevel string

	fs := flag.NewFlagSet("fake-gcs-server", flag.ContinueOnError)
	fs.StringVar(&cfg.backend, "backend", filesystemBackend, "storage backend (memory or filesystem)")
	fs.StringVar(&cfg.fsRoot, "filesystem-root", "/storage", "filesystem root (required for the filesystem backend). folder will be created if it doesn't exist")
	fs.StringVar(&cfg.publicHost, "public-host", "storage.googleapis.com", "Optional URL for public host")
	fs.StringVar(&cfg.externalURL, "external-url", "", "optional external URL, returned in the Location header for uploads. Defaults to the address where the server is running")
	fs.StringVar(&cfg.Scheme, "scheme", schemeHTTPS, "using 'http' or 'https' or 'both'")
	fs.StringVar(&cfg.Host, "host", "0.0.0.0", "host to bind to")
	fs.StringVar(&cfg.Seed, "data", "", "where to load data from (provided that the directory exists)")
	fs.StringVar(&allowedCORSHeaders, "cors-headers", "", "comma separated list of headers to add to the CORS allowlist")
	fs.UintVar(&cfg.Port, "port", defaultHTTPSPort, "port to bind to")
	fs.UintVar(&cfg.PortHTTP, flagPortHTTP, 0, "used only when scheme is 'both' as the port to bind http to (default 8000)")
	fs.StringVar(&cfg.event.pubsubProjectID, "event.pubsub-project-id", "", "project ID containing the pubsub topic")
	fs.StringVar(&cfg.event.pubsubTopic, "event.pubsub-topic", "", "pubsub topic name to publish events on")
	fs.StringVar(&cfg.event.bucket, "event.bucket", "", "if not empty, only objects in this bucket will generate trigger events")
	fs.StringVar(&cfg.event.prefix, "event.object-prefix", "", "if not empty, only objects having this prefix will generate trigger events")
	fs.StringVar(&eventList, "event.list", eventFinalize, "comma separated list of events to publish on cloud function URl. Options are: finalize, delete, and metadataUpdate")
	fs.Var(&cfg.events, "event.config", "notification configuration in the format: bucket=<bucket>;project=<project>;topic=<topic>[;events=<event1>,<event2>][;prefix=<prefix>]. Can be specified multiple times for multiple configurations. events defaults to finalize. Supported events: finalize, delete, metadataUpdate, archive")
	fs.StringVar(&cfg.bucketLocation, "location", "US-CENTRAL1", "location for buckets")
	fs.StringVar(&cfg.CertificateLocation, "cert-location", "", "location for server certificate")
	fs.StringVar(&cfg.PrivateKeyLocation, "private-key-location", "", "location for private key")
	fs.StringVar(&logLevel, "log-level", "info", "level for logging. Options same as for logrus: trace, debug, info, warn, error, fatal, and panic")

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
		cfg.Port = defaultHTTPSPort
	}

	if _, ok := setFlags[flagPortHTTP]; !ok && cfg.Scheme == schemeBoth {
		cfg.PortHTTP = defaultHTTPPort
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

	if err := c.event.validate(); err != nil {
		return err
	}

	for _, raw := range c.events {
		opts, err := parseEventConfig(raw)
		if err != nil {
			return err
		}
		c.parsedEvents = append(c.parsedEvents, opts)
	}

	return nil
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
		EventConfigs:        c.parsedEvents,
		BucketsLocation:     c.bucketLocation,
		CertificateLocation: c.CertificateLocation,
		PrivateKeyLocation:  c.PrivateKeyLocation,
		NoListener:          true,
	}
	return opts
}
