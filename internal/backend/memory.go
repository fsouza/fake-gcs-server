// Copyright 2018 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"errors"
	"fmt"
	"sync"
)

// StorageMemory is an implementation of the backend storage that stores data in memory
type StorageMemory struct {
	buckets map[string]bucketInMemory
	mtx     sync.RWMutex
}

type bucketInMemory struct {
	Bucket
	objects []Object
}

func newBucketInMemory(name string, versioningEnabled bool) bucketInMemory {
	return bucketInMemory{Bucket{name, versioningEnabled}, []Object{}}
}

func (bm *bucketInMemory) addObject(object Object) {
	bm.objects = append(bm.objects, object)
}

// NewStorageMemory creates an instance of StorageMemory
func NewStorageMemory(objects []Object) Storage {
	s := &StorageMemory{
		buckets: make(map[string]bucketInMemory),
	}
	for _, o := range objects {
		s.CreateBucket(o.BucketName, false)
		bucket := s.buckets[o.BucketName]
		bucket.addObject(o)
		s.buckets[o.BucketName] = bucket
	}
	return s
}

// CreateBucket creates a bucket
func (s *StorageMemory) CreateBucket(name string, versioningEnabled bool) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	bucket, err := s.getBucketInMemory(name)
	if err == nil {
		if bucket.VersioningEnabled != versioningEnabled {
			return fmt.Errorf("a bucket named %s already exists, but with different properties", name)
		}
		return nil
	}
	s.buckets[name] = newBucketInMemory(name, versioningEnabled)
	return nil
}

// ListBuckets lists buckets
func (s *StorageMemory) ListBuckets() ([]Bucket, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	buckets := []Bucket{}
	for bucket := range s.buckets {
		buckets = append(buckets, Bucket{s.buckets[bucket].Name, s.buckets[bucket].VersioningEnabled})
	}
	return buckets, nil
}

// GetBucket checks if a bucket exists
func (s *StorageMemory) GetBucket(name string) (Bucket, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	bucketInMemory, err := s.getBucketInMemory(name)
	return Bucket{bucketInMemory.Name, bucketInMemory.VersioningEnabled}, err
}

func (s *StorageMemory) getBucketInMemory(name string) (bucketInMemory, error) {
	if bucketInMemory, found := s.buckets[name]; found {
		return bucketInMemory, nil
	}
	return bucketInMemory{}, fmt.Errorf("no bucket named %s", name)
}

// CreateObject stores an object
func (s *StorageMemory) CreateObject(obj Object) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	bucketInMemory, err := s.getBucketInMemory(obj.BucketName)
	if err != nil {
		bucketInMemory = newBucketInMemory(obj.BucketName, false)
	}
	index := s.findObject(obj)
	if index < 0 {
		bucketInMemory.addObject(obj)
	} else {
		bucketInMemory.objects[index] = obj
	}
	s.buckets[obj.BucketName] = bucketInMemory
	return nil
}

// findObject looks for an object in its bucket and return the index where it
// was found, or -1 if the object doesn't exist.
//
// It doesn't lock the mutex, callers must lock the mutex before calling this
// method.
func (s *StorageMemory) findObject(obj Object) int {
	for i, o := range s.buckets[obj.BucketName].objects {
		if obj.ID() == o.ID() {
			return i
		}
	}
	return -1
}

// ListObjects lists the objects in a given bucket with a given prefix and delimeter
func (s *StorageMemory) ListObjects(bucketName string) ([]Object, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	bucketInMemory, err := s.getBucketInMemory(bucketName)
	if err != nil {
		return []Object{}, err
	}
	return bucketInMemory.objects, nil
}

// GetObject get an object by bucket and name
func (s *StorageMemory) GetObject(bucketName, objectName string) (Object, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	obj := Object{BucketName: bucketName, Name: objectName}
	index := s.findObject(obj)
	if index < 0 {
		return obj, errors.New("object not found")
	}
	return s.buckets[bucketName].objects[index], nil
}

// GetObjectWithGeneration retrieves an specific version of the object
func (s *StorageMemory) GetObjectWithGeneration(bucketName, objectName string, generation int64) (Object, error) {
	return s.GetObject(bucketName, objectName)
}

// DeleteObject deletes an object by bucket and name
func (s *StorageMemory) DeleteObject(bucketName, objectName string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	bucketInMemory, err := s.getBucketInMemory(bucketName)
	if err != nil {
		return err
	}
	obj := Object{BucketName: bucketName, Name: objectName}
	index := s.findObject(obj)
	if index < 0 {
		return fmt.Errorf("no such object in bucket %s: %s", bucketName, objectName)
	}
	objects := bucketInMemory.objects
	objects[index] = objects[len(objects)-1]
	bucketInMemory.objects = objects[:len(objects)-1]
	s.buckets[obj.BucketName] = bucketInMemory
	return nil
}
