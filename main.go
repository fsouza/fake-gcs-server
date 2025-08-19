// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/fsouza/fake-gcs-server/internal/config"
	"github.com/fsouza/fake-gcs-server/internal/grpc"
)

func createListener(logger *slog.Logger, cfg *config.Config, scheme string) (net.Listener, *fakestorage.Options) {
	opts := cfg.ToFakeGcsOptions(logger, scheme)

	addr := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	if opts.Scheme == "https" {
		var tlsConfig *tls.Config
		if opts.CertificateLocation != "" && opts.PrivateKeyLocation != "" {
			cert, err := tls.LoadX509KeyPair(opts.CertificateLocation, opts.PrivateKeyLocation)
			if err != nil {
				log.Fatal(err)
			}
			tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
		} else {
			// hack to use the certificate from Go's test server
			server := httptest.NewUnstartedServer(nil)
			server.StartTLS()
			server.Close()
			tlsConfig = server.TLS
		}
		tlsConfig.NextProtos = []string{"h2"}
		listener = tls.NewListener(listener, tlsConfig)
	}

	return listener, &opts
}

func startServer(logger *slog.Logger, cfg *config.Config) {
	type listenerAndOpts struct {
		listener net.Listener
		opts     *fakestorage.Options
	}

	var listenersAndOpts []listenerAndOpts

	if cfg.Scheme != "both" {
		listener, opts := createListener(logger, cfg, cfg.Scheme)
		listenersAndOpts = []listenerAndOpts{{listener, opts}}
	} else {
		httpListener, httpOpts := createListener(logger, cfg, "http")
		httpsListener, httpsOpts := createListener(logger, cfg, "https")
		listenersAndOpts = []listenerAndOpts{
			{httpListener, httpOpts},
			{httpsListener, httpsOpts},
		}
	}

	addMimeTypes()

	httpServer, err := fakestorage.NewServerWithOptions(*listenersAndOpts[0].opts)
	if err != nil {
		logger.With("Error", err).Error("couldn't start the server")
		os.Exit(1)
	}

	grpcServer := grpc.NewServerWithBackend(httpServer.Backend())

	for _, listenerAndOpts := range listenersAndOpts {
		go func(listener net.Listener) {
			http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.ProtoMajor == 2 && strings.HasPrefix(
					r.Header.Get("Content-Type"), "application/grpc") {
					grpcServer.ServeHTTP(w, r)
				} else {
					httpServer.HTTPHandler().ServeHTTP(w, r)
				}
			}))
		}(listenerAndOpts.listener)

		logger.Info(fmt.Sprintf("server started at %s://%s:%d",
			listenerAndOpts.opts.Scheme, listenerAndOpts.opts.Host, listenerAndOpts.opts.Port))
	}
}

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err == flag.ErrHelp {
		return
	}
	if err != nil {
		log.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	startServer(logger, &cfg)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}

func addMimeTypes() {
	mapping := map[string]string{
		".yaml": "application/x-yaml",
		".yml":  "application/x-yaml",
	}
	for ext, typ := range mapping {
		mime.AddExtensionType(ext, typ)
	}
}
