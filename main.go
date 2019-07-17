// Copyright 2019 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func generateObjectsFromFiles() []fakestorage.Object {
	objects := []fakestorage.Object{}

	// if a /data volume is mounted in the container
	if _, err := os.Stat("/data"); !os.IsNotExist(err) {
		// list the content
		files, err := ioutil.ReadDir("/data")

		if err != nil {
			panic(err)
		}

		for _, f := range files {
			fn := f.Name()
			fi, err := os.Stat("/data/" + fn)

			if err != nil {
				panic(err)
			}

			mode := fi.Mode();
			if mode.IsDir() {
				// if it's a directory, look for the files it's containing

				files, err := ioutil.ReadDir("/data/" + fn)

				if err != nil {
					panic(err)
				}

				// for each file create an object
				for _, f := range files {
					fmt.Printf("Creating object with name %s in bucket %s\n", f.Name(), fn)
					content, err := ioutil.ReadFile("/data/" + fn + "/" + f.Name())

					if err != nil {
						panic(err)
					}

					object := fakestorage.Object{
						BucketName: fn,
						Name:       f.Name(), //filename
						Content:    content,
					}
					objects = append(objects, object)
				}
			}
		}
	}
	return objects
}

func main() {

	// initialObjects := []fakestorage.Object{
	// 	{
	// 		objectName: "object",
	// 		Name:       "object-precreate-object",
	// 		Content:    []byte("This object just forces the object to exist when the server starts up."),
	// 	},
	// }

	loadedObjects := generateObjectsFromFiles()

	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: loadedObjects,
		Host:           "0.0.0.0",
		Port:           4443,
		StorageRoot:    "/storage",
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Server started at %s\n", server.URL())
	select {}
}
