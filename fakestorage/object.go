// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

// Object represents the object that is stored within the fake server.
type Object struct {
	BucketName string `json:"-"`
	Name       string `json:"name"`
	Content    []byte `json:"-"`
}

func (o *Object) id() string {
	return o.BucketName + "/" + o.Name
}

type objectList []Object

func (o objectList) Len() int {
	return len(o)
}

func (o objectList) Less(i int, j int) bool {
	return o[i].Name < o[j].Name
}

func (o *objectList) Swap(i int, j int) {
	d := *o
	d[i], d[j] = d[j], d[i]
}

// CreateObject stores the given object internally.
//
// If the bucket within the object doesn't exist, it also creates it. If the
// object already exists, it overrides the object.
func (s *Server) CreateObject(obj Object) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.createObject(obj)
}

func (s *Server) createObject(obj Object) {
	index := s.findObject(obj)
	if index < 0 {
		s.buckets[obj.BucketName] = append(s.buckets[obj.BucketName], obj)
	} else {
		s.buckets[obj.BucketName][index] = obj
	}
}

// ListObjects returns a sorted list of objects that match the given criteria,
// or an error if the bucket doesn't exist.
func (s *Server) ListObjects(bucketName, prefix, delimiter string) ([]Object, []string, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	objects, ok := s.buckets[bucketName]
	if !ok {
		return nil, nil, errors.New("bucket not found")
	}
	olist := objectList(objects)
	sort.Sort(&olist)
	var (
		respObjects  []Object
		respPrefixes []string
	)
	prefixes := make(map[string]bool)
	for _, obj := range olist {
		if strings.HasPrefix(obj.Name, prefix) {
			objName := strings.Replace(obj.Name, prefix, "", 1)
			delimPos := strings.Index(objName, delimiter)
			if delimiter != "" && delimPos > -1 {
				prefixes[obj.Name[:len(prefix)+delimPos+1]] = true
			} else {
				respObjects = append(respObjects, obj)
			}
		}
	}
	for p := range prefixes {
		respPrefixes = append(respPrefixes, p)
	}
	sort.Strings(respPrefixes)
	return respObjects, respPrefixes, nil
}

// GetObject returns the object with the given name in the given bucket, or an
// error if the object doesn't exist.
func (s *Server) GetObject(bucketName, objectName string) (Object, error) {
	obj := Object{BucketName: bucketName, Name: objectName}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	index := s.findObject(obj)
	if index < 0 {
		return obj, errors.New("object not found")
	}
	return s.buckets[bucketName][index], nil
}

// findObject looks for an object in its bucket and return the index where it
// was found, or -1 if the object doesn't exist.
//
// It doesn't lock the mutex, callers must lock the mutex before calling this
// method.
func (s *Server) findObject(obj Object) int {
	for i, o := range s.buckets[obj.BucketName] {
		if obj.id() == o.id() {
			return i
		}
	}
	return -1
}

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request) {
	bucketName := mux.Vars(r)["bucketName"]
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	objs, prefixes, err := s.ListObjects(bucketName, prefix, delimiter)
	encoder := json.NewEncoder(w)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		errResp := newErrorResponse(http.StatusNotFound, "Not Found", nil)
		encoder.Encode(errResp)
		return
	}
	encoder.Encode(newListObjectsResponse(objs, prefixes))
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	encoder := json.NewEncoder(w)
	obj, err := s.GetObject(vars["bucketName"], vars["objectName"])
	if err != nil {
		errResp := newErrorResponse(http.StatusNotFound, "Not Found", nil)
		w.WriteHeader(http.StatusNotFound)
		encoder.Encode(errResp)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	encoder.Encode(newObjectResponse(obj))
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	vars := mux.Vars(r)
	obj := Object{BucketName: vars["bucketName"], Name: vars["objectName"]}
	index := s.findObject(obj)
	if index < 0 {
		errResp := newErrorResponse(http.StatusNotFound, "Not Found", nil)
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(errResp)
		return
	}
	bucket := s.buckets[obj.BucketName]
	bucket[index] = bucket[len(bucket)-1]
	s.buckets[obj.BucketName] = bucket[:len(bucket)-1]
	w.WriteHeader(http.StatusOK)
}

func (s *Server) rewriteObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	obj, err := s.GetObject(vars["sourceBucket"], vars["sourceObject"])
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	dstBucket := vars["destinationBucket"]
	newObject := Object{
		BucketName: dstBucket,
		Name:       vars["destinationObject"],
		Content:    append([]byte(nil), obj.Content...),
	}
	s.CreateObject(newObject)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newObjectRewriteResponse(newObject))
}

func (s *Server) downloadObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	obj, err := s.GetObject(vars["bucketName"], vars["objectName"])
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	status := http.StatusOK
	start, end, content := s.handleRange(obj, r)
	if len(content) != len(obj.Content) {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.Content)))
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(content)))
	w.WriteHeader(status)
	w.Write(content)
}

func (s *Server) handleRange(obj Object, r *http.Request) (start, end int, content []byte) {
	if reqRange := r.Header.Get("Range"); reqRange != "" {
		parts := strings.SplitN(reqRange, "=", 2)
		if len(parts) == 2 && parts[0] == "bytes" {
			rangeParts := strings.SplitN(parts[1], "-", 2)
			if len(rangeParts) == 2 {
				start, _ = strconv.Atoi(rangeParts[0])
				end, _ = strconv.Atoi(rangeParts[1])
				if end < 1 {
					end = len(obj.Content)
				}
				return start, end, obj.Content[start:end]
			}
		}
	}
	return 0, 0, obj.Content
}
