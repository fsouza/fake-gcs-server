// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/fsouza/fake-gcs-server/internal/config"
	"github.com/sirupsen/logrus"
)

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	logger := logrus.New()

	opts := cfg.ToFakeGcsOptions()
	opts.InitialObjects = generateObjectsFromFiles(logger, cfg.Seed)

	server, err := fakestorage.NewServerWithOptions(opts)
	if err != nil {
		logger.WithError(err).Fatal("couldn't start the server")
	}
	logger.Infof("server started at %s", server.URL())

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}

func generateObjectsFromFiles(logger *logrus.Logger, folder string) []fakestorage.Object {
	var objects []fakestorage.Object
	if files, err := ioutil.ReadDir(folder); err == nil {
		for _, f := range files {
			bucketName := f.Name()
			localBucketPath := filepath.Join(folder, bucketName)

			files, err := ioutil.ReadDir(localBucketPath)
			if err != nil {
				logger.WithError(err).Warnf("couldn't read files from %q, skipping (make sure it's a directory)", localBucketPath)
				continue
			}

			for _, f := range files {
				objectKey := f.Name()
				localObjectPath := filepath.Join(localBucketPath, objectKey)
				content, err := ioutil.ReadFile(localObjectPath)
				if err != nil {
					logger.WithError(err).Warnf("couldn't read file %q, skipping", localObjectPath)
					continue
				}

				object := fakestorage.Object{
					BucketName: bucketName,
					Name:       objectKey,
					Content:    content,
				}
				objects = append(objects, object)
			}
		}
	}
	if len(objects) == 0 {
		logger.Infof("couldn't load any objects from %q, starting empty", folder)
	}
	return objects
}
