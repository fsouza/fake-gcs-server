package main

import (
	"cloud.google.com/go/storage"
	"context"
	"crypto/tls"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"log"
	"net/http"
)

func main() {

	ctx := context.Background()

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	httpClient := &http.Client{Transport: transCfg}

	// [START setup]
	client, err := storage.NewClient(ctx, option.WithEndpoint("https://0.0.0.0:4443/storage/v1/"), option.WithHTTPClient(httpClient))
	if err != nil {
		log.Fatal(err)
	}

	buckets, err := list(client, "test")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("buckets: %+v\n", buckets)
}

func list(client *storage.Client, projectID string) ([]string, error) {
	ctx := context.Background()
	// [START list_buckets]
	var buckets []string
	it := client.Buckets(ctx, projectID)
	for {
		battrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, battrs.Name)
	}
	// [END list_buckets]
	return buckets, nil
}
