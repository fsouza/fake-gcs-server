// Copyright 2017 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/internal/backend"
)

const timestampFormat = "2006-01-02T15:04:05.999999Z07:00"

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(timestampFormat)
}

type listResponse struct {
	Kind          string   `json:"kind"`
	Items         []any    `json:"items,omitempty"`
	Prefixes      []string `json:"prefixes,omitempty"`
	NextPageToken string   `json:"nextPageToken,omitempty"`
}

func newListBucketsResponse(buckets []backend.Bucket, location string, externalURL string, nextPageToken string) listResponse {
	resp := listResponse{
		Kind:          "storage#buckets",
		Items:         make([]any, len(buckets)),
		NextPageToken: nextPageToken,
	}
	for i, bucket := range buckets {
		resp.Items[i] = newBucketResponse(bucket, location, externalURL)
	}
	return resp
}

type bucketResponse struct {
	Kind                  string            `json:"kind"`
	ID                    string            `json:"id"`
	DefaultEventBasedHold bool              `json:"defaultEventBasedHold"`
	Name                  string            `json:"name"`
	Versioning            *bucketVersioning `json:"versioning,omitempty"`
	TimeCreated           string            `json:"timeCreated,omitempty"`
	Updated               string            `json:"updated,omitempty"`
	Location              string            `json:"location,omitempty"`
	StorageClass          string            `json:"storageClass,omitempty"`
	ProjectNumber         string            `json:"projectNumber"`
	Metageneration        string            `json:"metageneration"`
	Etag                  string            `json:"etag"`
	LocationType          string            `json:"locationType"`
	SelfLink              string            `json:"selfLink,omitempty"`
}

type bucketVersioning struct {
	Enabled bool `json:"enabled"`
}

func newBucketResponse(bucket backend.Bucket, location string, externalURL string) bucketResponse {
	return bucketResponse{
		Kind:                  "storage#bucket",
		ID:                    bucket.Name,
		Name:                  bucket.Name,
		DefaultEventBasedHold: bucket.DefaultEventBasedHold,
		Versioning:            &bucketVersioning{bucket.VersioningEnabled},
		TimeCreated:           formatTime(bucket.TimeCreated),
		Updated:               formatTime(bucket.TimeCreated), // not tracking update times yet, reporting `updated` = `timeCreated`
		Location:              location,
		StorageClass:          "STANDARD",
		ProjectNumber:         "0",
		Metageneration:        "1",
		Etag:                  "RVRhZw==",
		LocationType:          "region",
		SelfLink:              fmt.Sprintf("%s/storage/v1/b/%s", externalURL, url.PathEscape(bucket.Name)),
	}
}

type bucketStorageLayoutResponse struct {
	Kind                  string                                    `json:"kind"`
	Bucket                string                                    `json:"bucket"`
	Location              string                                    `json:"location,omitempty"`
	LocationType          string                                    `json:"locationType,omitempty"`
	HierarchicalNamespace *bucketStorageLayoutHierarchicalNamespace `json:"hierarchicalNamespace,omitempty"`
}

type bucketStorageLayoutHierarchicalNamespace struct {
	Enabled bool `json:"enabled"`
}

func newBucketStorageLayoutResponse(bucketName string, location string) bucketStorageLayoutResponse {
	return bucketStorageLayoutResponse{
		Kind:                  "storage#storageLayout",
		Bucket:                bucketName,
		Location:              location,
		LocationType:          "region",
		HierarchicalNamespace: &bucketStorageLayoutHierarchicalNamespace{Enabled: false},
	}
}

func newListObjectsResponse(response ListResponse, externalURL string) listResponse {
	resp := listResponse{
		Kind:          "storage#objects",
		Items:         make([]any, len(response.Objects)),
		Prefixes:      response.Prefixes,
		NextPageToken: response.NextPageToken,
	}
	for i, obj := range response.Objects {
		resp.Items[i] = newObjectResponse(obj, externalURL)
	}
	return resp
}

// objectAccessControl is copied from the Google SDK to avoid direct
// dependency.
type objectAccessControl struct {
	Bucket      string       `json:"bucket,omitempty"`
	Domain      string       `json:"domain,omitempty"`
	Email       string       `json:"email,omitempty"`
	Entity      string       `json:"entity,omitempty"`
	EntityID    string       `json:"entityId,omitempty"`
	Etag        string       `json:"etag,omitempty"`
	Generation  int64        `json:"generation,omitempty,string"`
	ID          string       `json:"id,omitempty"`
	Kind        string       `json:"kind,omitempty"`
	Object      string       `json:"object,omitempty"`
	ProjectTeam *projectTeam `json:"projectTeam,omitempty"`
	Role        string       `json:"role,omitempty"`
	SelfLink    string       `json:"selfLink,omitempty"`
}

type objectRetention struct {
	Mode            string `json:"mode,omitempty"`
	RetainUntilTime string `json:"retainUntilTime,omitempty"`
}

type objectResponse struct {
	Kind                    string                 `json:"kind"`
	Name                    string                 `json:"name"`
	ID                      string                 `json:"id"`
	Bucket                  string                 `json:"bucket"`
	Size                    int64                  `json:"size,string"`
	ContentType             string                 `json:"contentType,omitempty"`
	ContentEncoding         string                 `json:"contentEncoding,omitempty"`
	ContentDisposition      string                 `json:"contentDisposition,omitempty"`
	ContentLanguage         string                 `json:"contentLanguage,omitempty"`
	CacheControl            string                 `json:"cacheControl,omitempty"`
	Crc32c                  string                 `json:"crc32c,omitempty"`
	ACL                     []*objectAccessControl `json:"acl,omitempty"`
	Md5Hash                 string                 `json:"md5Hash,omitempty"`
	Etag                    string                 `json:"etag,omitempty"`
	StorageClass            string                 `json:"storageClass"`
	TimeCreated             string                 `json:"timeCreated,omitempty"`
	TimeDeleted             string                 `json:"timeDeleted,omitempty"`
	TimeStorageClassUpdated string                 `json:"timeStorageClassUpdated,omitempty"`
	Updated                 string                 `json:"updated,omitempty"`
	Generation              int64                  `json:"generation,string"`
	CustomTime              string                 `json:"customTime,omitempty"`
	Metadata                map[string]string      `json:"metadata,omitempty"`
	SelfLink                string                 `json:"selfLink,omitempty"`
	MediaLink               string                 `json:"mediaLink,omitempty"`
	Metageneration          string                 `json:"metageneration,omitempty"`
	Retention               *objectRetention       `json:"retention,omitempty"`
}

func newProjectedObjectResponse(obj ObjectAttrs, externalURL string, projection storage.Projection) objectResponse {
	objResponse := newObjectResponse(obj, externalURL)
	if projection == storage.ProjectionNoACL {
		objResponse.ACL = nil
	}
	return objResponse
}

func newObjectResponse(obj ObjectAttrs, externalURL string) objectResponse {
	acl := getAccessControlsListFromObject(obj, externalURL)
	storageClass := obj.StorageClass
	if storageClass == "" {
		storageClass = "STANDARD"
	}

	var retention *objectRetention
	if obj.Retention != nil {
		retention = &objectRetention{
			Mode:            obj.Retention.Mode,
			RetainUntilTime: formatTime(obj.Retention.RetainUntil),
		}
	}

	return objectResponse{
		Kind:                    "storage#object",
		ID:                      obj.id(),
		Bucket:                  obj.BucketName,
		Name:                    obj.Name,
		Size:                    obj.Size,
		ContentType:             obj.ContentType,
		ContentEncoding:         obj.ContentEncoding,
		ContentDisposition:      obj.ContentDisposition,
		ContentLanguage:         obj.ContentLanguage,
		CacheControl:            obj.CacheControl,
		Crc32c:                  obj.Crc32c,
		Md5Hash:                 obj.Md5Hash,
		Etag:                    obj.Etag,
		ACL:                     acl,
		StorageClass:            storageClass,
		Metadata:                obj.Metadata,
		TimeCreated:             formatTime(obj.Created),
		TimeDeleted:             formatTime(obj.Deleted),
		TimeStorageClassUpdated: formatTime(obj.Updated),
		Updated:                 formatTime(obj.Updated),
		CustomTime:              formatTime(obj.CustomTime),
		Generation:              obj.Generation,
		SelfLink:                fmt.Sprintf("%s/storage/v1/b/%s/o/%s", externalURL, url.PathEscape(obj.BucketName), url.PathEscape(obj.Name)),
		MediaLink:               fmt.Sprintf("%s/download/storage/v1/b/%s/o/%s?alt=media", externalURL, url.PathEscape(obj.BucketName), url.PathEscape(obj.Name)),
		Metageneration:          "1",
		Retention:               retention,
	}
}

type aclListResponse struct {
	Kind  string                 `json:"kind"`
	Items []*objectAccessControl `json:"items"`
}

func newACLListResponse(obj ObjectAttrs, externalURL string) aclListResponse {
	return aclListResponse{
		Kind:  "storage#objectAccessControls",
		Items: getAccessControlsListFromObject(obj, externalURL),
	}
}

func getAccessControlsListFromObject(obj ObjectAttrs, externalURL string) []*objectAccessControl {
	aclItems := make([]*objectAccessControl, len(obj.ACL))
	for idx, aclRule := range obj.ACL {
		aclItems[idx] = newObjectAccessControl(obj, aclRule, externalURL)
	}
	return aclItems
}

func newObjectAccessControl(obj ObjectAttrs, rule storage.ACLRule, externalURL string) *objectAccessControl {
	entity := string(rule.Entity)
	oac := &objectAccessControl{
		Kind:        "storage#objectAccessControl",
		ID:          fmt.Sprintf("%s/%s/%d/%s", obj.BucketName, obj.Name, obj.Generation, entity),
		SelfLink:    fmt.Sprintf("%s/storage/v1/b/%s/o/%s/acl/%s", externalURL, url.PathEscape(obj.BucketName), url.PathEscape(obj.Name), url.PathEscape(entity)),
		Bucket:      obj.BucketName,
		Object:      obj.Name,
		Generation:  obj.Generation,
		Entity:      entity,
		EntityID:    rule.EntityID,
		Email:       rule.Email,
		Domain:      rule.Domain,
		ProjectTeam: (*projectTeam)(rule.ProjectTeam),
		Role:        string(rule.Role),
		Etag:        "RVRhZw==",
	}
	// A rule set through the API carries only the entity and the role; GCS
	// fills in what the entity names.
	switch {
	case oac.Email == "" && (strings.HasPrefix(entity, "user-") || strings.HasPrefix(entity, "group-")) && strings.Contains(entity, "@"):
		_, oac.Email, _ = strings.Cut(entity, "-")
	case oac.Domain == "" && strings.HasPrefix(entity, "domain-"):
		oac.Domain = strings.TrimPrefix(entity, "domain-")
	case oac.ProjectTeam == nil && strings.HasPrefix(entity, "project-"):
		if team, number, ok := strings.Cut(strings.TrimPrefix(entity, "project-"), "-"); ok {
			oac.ProjectTeam = &projectTeam{ProjectNumber: number, Team: team}
		}
	}
	return oac
}

type bucketAccessControl struct {
	Bucket   string `json:"bucket,omitempty"`
	Entity   string `json:"entity,omitempty"`
	Role     string `json:"role,omitempty"`
	Etag     string `json:"etag,omitempty"`
	ID       string `json:"id,omitempty"`
	Kind     string `json:"kind,omitempty"`
	SelfLink string `json:"selfLink,omitempty"`
}

type bucketACLListResponse struct {
	Kind  string                 `json:"kind"`
	Items []*bucketAccessControl `json:"items"`
}

func newBucketAccessControl(bucketName string, rule storage.ACLRule, externalURL string) *bucketAccessControl {
	return &bucketAccessControl{
		Bucket:   bucketName,
		Entity:   string(rule.Entity),
		Role:     string(rule.Role),
		Etag:     "RVRhZw==",
		ID:       bucketName + "/" + string(rule.Entity),
		Kind:     "storage#bucketAccessControl",
		SelfLink: fmt.Sprintf("%s/storage/v1/b/%s/acl/%s", externalURL, url.PathEscape(bucketName), url.PathEscape(string(rule.Entity))),
	}
}

func newBucketACLListResponse(bucket backend.Bucket, externalURL string) bucketACLListResponse {
	items := make([]*bucketAccessControl, len(bucket.ACL))
	for idx, rule := range bucket.ACL {
		items[idx] = newBucketAccessControl(bucket.Name, rule, externalURL)
	}
	return bucketACLListResponse{
		Kind:  "storage#bucketAccessControls",
		Items: items,
	}
}

type rewriteResponse struct {
	Kind                string         `json:"kind"`
	TotalBytesRewritten int64          `json:"totalBytesRewritten,string"`
	ObjectSize          int64          `json:"objectSize,string"`
	Done                bool           `json:"done"`
	RewriteToken        string         `json:"rewriteToken"`
	Resource            objectResponse `json:"resource"`
}

func newObjectRewriteResponse(obj ObjectAttrs, externalURL string) rewriteResponse {
	return rewriteResponse{
		Kind:                "storage#rewriteResponse",
		TotalBytesRewritten: obj.Size,
		ObjectSize:          obj.Size,
		Done:                true,
		RewriteToken:        "",
		Resource:            newObjectResponse(obj, externalURL),
	}
}

type errorResponse struct {
	Error httpError `json:"error"`
}

type httpError struct {
	Code    int        `json:"code"`
	Message string     `json:"message"`
	Errors  []apiError `json:"errors"`
}

type apiError struct {
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

func newErrorResponse(code int, message string, errs []apiError) errorResponse {
	return errorResponse{
		Error: httpError{
			Code:    code,
			Message: message,
			Errors:  errs,
		},
	}
}
