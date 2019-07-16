// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func main() {
	cfg, err := loadConfig(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	server, err := fakestorage.NewServerWithOptions(cfg.toFakeGcsOptions())
	if err != nil {
		log.Fatalf("could not start the server: %v", err)
	}
	defer server.Stop()
	log.Printf("server running at %v", server.URL())

	signalListener := make(chan os.Signal, 1)
	signal.Notify(signalListener, os.Interrupt, syscall.SIGTERM)
	<-signalListener
}
