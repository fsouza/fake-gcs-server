// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/fsouza/fake-gcs-server/internal/config"
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

	var emptyBuckets []string
	opts := cfg.ToFakeGcsOptions()
	opts.InitialObjects, emptyBuckets = generateObjectsFromFiles(logger, cfg.Seed)

	server, err := fakestorage.NewServerWithOptions(opts)
	if err != nil {
		logger.WithError(err).Fatal("couldn't start the server")
	}
	logger.Infof("server started at %s", server.URL())
	for _, bucketName := range emptyBuckets {
		server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}

func generateObjectsFromFiles(logger *logrus.Logger, folder string) ([]fakestorage.Object, []string) {
	var objects []fakestorage.Object
	var emptyBuckets []string
	if files, err := ioutil.ReadDir(folder); err == nil {
		for _, f := range files {
			if !f.IsDir() {
				continue
			}
			bucketName := f.Name()
			localBucketPath := filepath.Join(folder, bucketName)

			bucketObjects, err := objectsFromBucket(localBucketPath, bucketName)
			if err != nil {
				logger.WithError(err).Warnf("couldn't read files from %q, skipping (make sure it's a directory)", localBucketPath)
				continue
			}

			if len(bucketObjects) < 1 {
				emptyBuckets = append(emptyBuckets, bucketName)
			}
			objects = append(objects, bucketObjects...)
		}
	}
	if len(objects) == 0 && len(emptyBuckets) == 0 {
		logger.Infof("couldn't load any objects or buckets from %q, starting empty", folder)
	}
	return objects, emptyBuckets
}

func objectsFromBucket(localBucketPath, bucketName string) ([]fakestorage.Object, error) {
	var objects []fakestorage.Object
	err := filepath.Walk(localBucketPath, func(path string, info os.FileInfo, _ error) error {
		if info.Mode().IsRegular() {
			objectKey := strings.TrimLeft(strings.Replace(path, localBucketPath, "", 1), "/")
			fileContent, err := ioutil.ReadFile(path)
			if err != nil {
				return fmt.Errorf("could not read file %q: %w", path, err)
			}
			objects = append(objects, fakestorage.Object{
				BucketName: bucketName,
				Name:       objectKey,
				Content:    fileContent,
			})
		}
		return nil
	})
	return objects, err
}
