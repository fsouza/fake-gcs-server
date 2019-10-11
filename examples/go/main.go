package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func main() {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	httpClient := &http.Client{Transport: transCfg}
	client, err := storage.NewClient(context.TODO(), option.WithEndpoint("https://0.0.0.0:4443/storage/v1/"), option.WithHTTPClient(httpClient))
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
	var buckets []string
	it := client.Buckets(context.TODO(), projectID)
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
	return buckets, nil
}
