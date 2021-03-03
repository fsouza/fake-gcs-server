// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This example requires the server to be running with the flag -public-host
// defined as localhost:8080.
//
// Check the file ci/run-go-example.sh for a fully functional server + client
// script.
package main

import (
	"context"
	"fmt"
	"io/ioutil"
    "io"
	"log"
    "bytes"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func main() {
	client, err := storage.NewClient(context.TODO(), option.WithEndpoint("http://localhost:8080/storage/v1/"), option.WithoutAuthentication())
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	const (
		bucketName = "sample-bucket"
		fileKey    = "some_file.txt"
	)
	buckets, err := list(client, bucketName)
	if err != nil {
		log.Fatalf("failed to list: %v", err)
	}
	fmt.Printf("buckets: %+v\n", buckets)

	data, err := downloadFile(client, bucketName, fileKey)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("contents of %s/%s: %s\n", bucketName, fileKey, data)

    payloadBytes := bytes.NewBuffer([]byte("foobar"))
    err = uploadFile(client, bucketName, "uploadTestKey", payloadBytes)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("uploaded %s to %s/uploadTestKey\n", string(payloadBytes.Bytes()), bucketName)
}

func list(client *storage.Client, bucketName string) ([]string, error) {
	var objects []string
	it := client.Bucket(bucketName).Objects(context.Background(), &storage.Query{})
	for {
		oattrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		objects = append(objects, oattrs.Name)
	}
	return objects, nil
}

func downloadFile(client *storage.Client, bucketName, fileKey string) ([]byte, error) {
	reader, err := client.Bucket(bucketName).Object(fileKey).NewReader(context.TODO())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

func uploadFile(client *storage.Client, bucketName, key string, reader io.Reader) error {
    wc := client.Bucket(bucketName).Object(key).NewWriter(context.Background())
	if _, err := io.Copy(wc, reader); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}
	return nil
}
