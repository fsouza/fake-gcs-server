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
	"path/filepath"
	"strings"
	"syscall"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/fsouza/fake-gcs-server/internal/checksum"
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
	var emptyBuckets []string
	if cfg.Seed != "" {
		opts.InitialObjects, emptyBuckets = generateObjectsFromFiles(logger, cfg.Seed)
	}

	httpServer, err := fakestorage.NewServerWithOptions(opts)
	if err != nil {
		logger.WithError(err).Fatal("couldn't start the server")
	}
	for _, bucketName := range emptyBuckets {
		httpServer.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})
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

func generateObjectsFromFiles(logger *logrus.Logger, folder string) ([]fakestorage.Object, []string) {
	var objects []fakestorage.Object
	var emptyBuckets []string
	if files, err := os.ReadDir(folder); err == nil {
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
			// Rel() should never return error since path always descend from localBucketPath
			relPath, _ := filepath.Rel(localBucketPath, path)
			objectKey := filepath.ToSlash(relPath)
			fileContent, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("could not read file %q: %w", path, err)
			}
			objects = append(objects, fakestorage.Object{
				ObjectAttrs: fakestorage.ObjectAttrs{
					ACL: []storage.ACLRule{
						{
							Entity: "projectOwner-test-project",
							Role:   "OWNER",
						},
					},
					BucketName:  bucketName,
					Name:        objectKey,
					ContentType: mime.TypeByExtension(filepath.Ext(path)),
					Crc32c:      checksum.EncodedCrc32cChecksum(fileContent),
					Md5Hash:     checksum.EncodedMd5Hash(fileContent),
				},
				Content: fileContent,
			})
		}
		return nil
	})
	return objects, err
}
