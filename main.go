// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
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
	"github.com/sirupsen/logrus"
)

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err == flag.ErrHelp {
		return
	}
	if err != nil {
		log.Fatal(err)
	}

	logger := logrus.New()
	logger.SetLevel(cfg.LogLevel)

	opts := cfg.ToFakeGcsOptions()

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.Scheme == "https" {
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
		listener = tls.NewListener(listener, tlsConfig)
	}

	addMimeTypes()

	httpServer, err := fakestorage.NewServerWithOptions(opts)
	if err != nil {
		logger.WithError(err).Fatal("couldn't start the server")
	}

	grpcServer := grpc.NewServerWithBackend(httpServer.Backend())
	go func() {
		http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor == 2 && strings.HasPrefix(
				r.Header.Get("Content-Type"), "application/grpc") {
				grpcServer.ServeHTTP(w, r)
			} else {
				httpServer.HTTPHandler().ServeHTTP(w, r)
			}
		}))
	}()

	logger.Infof("server started at %s://%s:%d", cfg.Scheme, cfg.Host, cfg.Port)

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
