// Copyright 2018 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsouza/fake-gcs-server/internal/checksum"
	"github.com/pkg/xattr"
)

// storageFS is an implementation of the backend storage that stores data on disk
//
// The layout is the following:
//
// - rootDir
//
//	|- bucket1
//	\- bucket2
//	  |- object1
//	  \- object2
//
// Bucket and object names are url path escaped, so there's no special meaning of forward slashes.
type storageFS struct {
	rootDir string
	mtx     sync.RWMutex
	mh      metadataHandler
}

// NewStorageFS creates an instance of the filesystem-backed storage backend.
func NewStorageFS(objects []StreamingObject, rootDir string) (Storage, error) {
	if !strings.HasSuffix(rootDir, "/") {
		rootDir += "/"
	}
	err := os.MkdirAll(rootDir, 0o700)
	if err != nil {
		return nil, err
	}

	var mh metadataHandler = metadataFile{}
	// Use xattr for metadata if rootDir supports it.
	if xattr.XATTR_SUPPORTED {
		xattrHandler := metadataXattr{}
		var xerr *xattr.Error
		_, err = xattrHandler.read(rootDir)
		if err == nil || (errors.As(err, &xerr) && xerr.Err == xattr.ENOATTR) {
			mh = xattrHandler
		}
	}

	s := &storageFS{rootDir: rootDir, mh: mh}
	for _, o := range objects {
		obj, err := s.CreateObject(o, NoConditions{})
		if err != nil {
			return nil, err
		}
		obj.Close()
	}
	return s, nil
}

// CreateBucket creates a bucket in the fs backend. A bucket is a folder in the
// root directory.
func (s *storageFS) CreateBucket(name string, versioningEnabled bool) error {
	if versioningEnabled {
		return errors.New("not implemented: fs storage type does not support versioning yet")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.createBucket(name)
}

func (s *storageFS) createBucket(name string) error {
	return os.MkdirAll(filepath.Join(s.rootDir, url.PathEscape(name)), 0o700)
}

// ListBuckets returns a list of buckets from the list of directories in the
// root directory.
func (s *storageFS) ListBuckets() ([]Bucket, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	infos, err := os.ReadDir(s.rootDir)
	if err != nil {
		return nil, err
	}
	buckets := []Bucket{}
	for _, info := range infos {
		if info.IsDir() {
			unescaped, err := url.PathUnescape(info.Name())
			if err != nil {
				return nil, fmt.Errorf("failed to unescape object name %s: %w", info.Name(), err)
			}
			buckets = append(buckets, Bucket{Name: unescaped})
		}
	}
	return buckets, nil
}

func timespecToTime(ts syscall.Timespec) time.Time {
	return time.Unix(int64(ts.Sec), int64(ts.Nsec))
}

// GetBucket returns information about the given bucket, or an error if it
// doesn't exist.
func (s *storageFS) GetBucket(name string) (Bucket, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	dirInfo, err := os.Stat(filepath.Join(s.rootDir, url.PathEscape(name)))
	if err != nil {
		return Bucket{}, err
	}
	return Bucket{Name: name, VersioningEnabled: false, TimeCreated: timespecToTime(createTimeFromFileInfo(dirInfo))}, err
}

// DeleteBucket removes the bucket from the backend.
func (s *storageFS) DeleteBucket(name string) error {
	objs, err := s.ListObjects(name, "", false)
	if err != nil {
		return BucketNotFound
	}
	if len(objs) > 0 {
		return BucketNotEmpty
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	return os.RemoveAll(filepath.Join(s.rootDir, url.PathEscape(name)))
}

// CreateObject stores an object as a regular file on disk. The backing content
// for the object may be in the same file that's being updated, so a temporary
// file is first created and then moved into place. This also makes it so any
// object content readers currently open continue reading from the original
// file instead of the newly created file.
//
// The crc32c checksum and md5 hash of the object content is calculated when
// reading the object content. Any checksum or hash in the passed-in object
// metadata is overwritten.
func (s *storageFS) CreateObject(obj StreamingObject, conditions Conditions) (StreamingObject, error) {
	if obj.Generation > 0 {
		return StreamingObject{}, errors.New("not implemented: fs storage type does not support objects generation yet")
	}

	// Note: this was a quick fix for issue #701. Now that we have a way to
	// persist object attributes, we should implement versioning in the
	// filesystem backend and handle generations outside of the backends.
	obj.Generation = time.Now().UnixNano() / 1000

	s.mtx.Lock()
	defer s.mtx.Unlock()
	err := s.createBucket(obj.BucketName)
	if err != nil {
		return StreamingObject{}, err
	}

	var activeGeneration int64
	existingObj, err := s.getObject(obj.BucketName, obj.Name)
	if err != nil {
		activeGeneration = 0
	} else {
		activeGeneration = existingObj.Generation
	}

	if !conditions.ConditionsMet(activeGeneration) {
		return StreamingObject{}, PreConditionFailed
	}

	path := filepath.Join(s.rootDir, url.PathEscape(obj.BucketName), url.PathEscape(obj.Name))

	tempFile, err := os.CreateTemp(filepath.Dir(path), "fake-gcs-object")
	if err != nil {
		return StreamingObject{}, err
	}
	tempFile.Close()

	tempFile, err = compatOpenForWritingAllowingRename(tempFile.Name())
	if err != nil {
		return StreamingObject{}, err
	}
	defer tempFile.Close()

	// The file is renamed below, which causes this to be a no-op. If the
	// function returns before the rename, though, the temp file will be
	// removed.
	defer os.Remove(tempFile.Name())

	err = os.Chmod(tempFile.Name(), 0o600)
	if err != nil {
		return StreamingObject{}, err
	}

	hasher := checksum.NewStreamingHasher()
	objectContent := io.TeeReader(obj.Content, hasher)

	if _, err = io.Copy(tempFile, objectContent); err != nil {
		return StreamingObject{}, err
	}

	obj.Crc32c = hasher.EncodedCrc32cChecksum()
	obj.Md5Hash = hasher.EncodedMd5Hash()
	obj.Etag = fmt.Sprintf("%q", obj.Md5Hash)

	// TODO: Handle if metadata is not present more gracefully?
	encoded, err := json.Marshal(obj.ObjectAttrs)
	if err != nil {
		return StreamingObject{}, err
	}

	if err = s.mh.write(tempFile.Name(), encoded); err != nil {
		return StreamingObject{}, err
	}

	tempFile.Close()
	err = compatRename(tempFile.Name(), path)
	if err != nil {
		return StreamingObject{}, err
	}

	if err = s.mh.rename(tempFile.Name(), path); err != nil {
		return StreamingObject{}, err
	}

	err = openObjectAndSetSize(&obj, path)

	return obj, err
}

// ListObjects lists the objects in a given bucket with a given prefix and
// delimeter.
func (s *storageFS) ListObjects(bucketName string, prefix string, versions bool) ([]ObjectAttrs, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	infos, err := os.ReadDir(filepath.Join(s.rootDir, url.PathEscape(bucketName)))
	if err != nil {
		return nil, err
	}
	objects := []ObjectAttrs{}
	for _, info := range infos {
		if s.mh.isSpecialFile(info.Name()) {
			continue
		}
		unescaped, err := url.PathUnescape(info.Name())
		if err != nil {
			return nil, fmt.Errorf("failed to unescape object name %s: %w", info.Name(), err)
		}
		if prefix != "" && !strings.HasPrefix(unescaped, prefix) {
			continue
		}
		object, err := s.getObject(bucketName, unescaped)
		if err != nil {
			return nil, err
		}
		object.Close()
		objects = append(objects, object.ObjectAttrs)
	}
	return objects, nil
}

// GetObject get an object by bucket and name.
func (s *storageFS) GetObject(bucketName, objectName string) (StreamingObject, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.getObject(bucketName, objectName)
}

// GetObjectWithGeneration retrieves an specific version of the object. Not
// implemented for this backend.
func (s *storageFS) GetObjectWithGeneration(bucketName, objectName string, generation int64) (StreamingObject, error) {
	obj, err := s.GetObject(bucketName, objectName)
	if err != nil {
		return obj, err
	}
	if obj.Generation != generation {
		return obj, fmt.Errorf("generation mismatch, object generation is %v, requested generation is %v (note: filesystem backend does not support versioning)", obj.Generation, generation)
	}
	return obj, nil
}

func (s *storageFS) getObject(bucketName, objectName string) (StreamingObject, error) {
	path := filepath.Join(s.rootDir, url.PathEscape(bucketName), url.PathEscape(objectName))

	encoded, err := s.mh.read(path)
	if err != nil {
		return StreamingObject{}, err
	}

	var obj StreamingObject
	if err = json.Unmarshal(encoded, &obj.ObjectAttrs); err != nil {
		return StreamingObject{}, err
	}

	obj.Name = filepath.ToSlash(objectName)
	obj.BucketName = bucketName

	err = openObjectAndSetSize(&obj, path)

	return obj, err
}

func openObjectAndSetSize(obj *StreamingObject, path string) error {
	// file is expected to be closed by the caller by calling obj.Close()
	file, err := compatOpenAllowingRename(path)
	if err != nil {
		return err
	}

	info, err := file.Stat()
	if err != nil {
		return err
	}

	obj.Content = file
	obj.Size = info.Size()

	return nil
}

// DeleteObject deletes an object by bucket and name.
func (s *storageFS) DeleteObject(bucketName, objectName string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if objectName == "" {
		return errors.New("can't delete object with empty name")
	}
	path := filepath.Join(s.rootDir, url.PathEscape(bucketName), url.PathEscape(objectName))
	if err := s.mh.remove(path); err != nil {
		return err
	}
	return os.Remove(path)
}

// PatchObject patches the given object metadata.
func (s *storageFS) PatchObject(bucketName, objectName string, metadata map[string]string) (StreamingObject, error) {
	obj, err := s.GetObject(bucketName, objectName)
	if err != nil {
		return StreamingObject{}, err
	}
	defer obj.Close()
	if obj.Metadata == nil {
		obj.Metadata = map[string]string{}
	}
	for k, v := range metadata {
		obj.Metadata[k] = v
	}
	obj.Generation = 0                         // reset generation id
	return s.CreateObject(obj, NoConditions{}) // recreate object
}

// UpdateObject replaces the given object metadata.
func (s *storageFS) UpdateObject(bucketName, objectName string, metadata map[string]string) (StreamingObject, error) {
	obj, err := s.GetObject(bucketName, objectName)
	if err != nil {
		return StreamingObject{}, err
	}
	defer obj.Close()
	obj.Metadata = map[string]string{}
	for k, v := range metadata {
		obj.Metadata[k] = v
	}
	obj.Generation = 0                         // reset generation id
	return s.CreateObject(obj, NoConditions{}) // recreate object
}

type concatenatedContent struct {
	io.Reader
}

func (c concatenatedContent) Close() error {
	return errors.New("not implemented")
}

func (c concatenatedContent) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("not implemented")
}

func concatObjectReaders(objects []StreamingObject) io.ReadSeekCloser {
	readers := make([]io.Reader, len(objects))
	for i := range objects {
		readers[i] = objects[i].Content
	}
	return concatenatedContent{io.MultiReader(readers...)}
}

func (s *storageFS) ComposeObject(bucketName string, objectNames []string, destinationName string, metadata map[string]string, contentType string) (StreamingObject, error) {
	var sourceObjects []StreamingObject
	for _, n := range objectNames {
		obj, err := s.GetObject(bucketName, n)
		if err != nil {
			return StreamingObject{}, err
		}
		defer obj.Close()
		sourceObjects = append(sourceObjects, obj)
	}

	dest := StreamingObject{
		ObjectAttrs: ObjectAttrs{
			BucketName:  bucketName,
			Name:        destinationName,
			ContentType: contentType,
			Created:     time.Now().String(),
		},
	}

	dest.Content = concatObjectReaders(sourceObjects)
	dest.Metadata = metadata

	result, err := s.CreateObject(dest, NoConditions{})
	if err != nil {
		return result, err
	}

	return result, nil
}
