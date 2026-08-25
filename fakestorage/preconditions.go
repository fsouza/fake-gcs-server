// Copyright 2026 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakestorage

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/fsouza/fake-gcs-server/internal/backend"
)

// objectMetageneration is the metageneration every object reports; see
// newObjectResponse. Nothing here revises an object's metadata without
// minting a new one, so the value never moves and a condition naming any
// other value cannot hold.
const objectMetageneration = 1

// preconditions holds the ifGenerationMatch family a request may name. GCS
// evaluates them against the object the request addresses and answers 412
// when one does not hold, generation zero standing for an object that is not
// there -- which is how a caller asks to create one and no more.
type preconditions struct {
	ifGenerationMatch        *int64
	ifGenerationNotMatch     *int64
	ifMetagenerationMatch    *int64
	ifMetagenerationNotMatch *int64
}

// empty reports whether the request named no condition at all, letting a
// caller skip the lookup the check would otherwise need.
func (p preconditions) empty() bool {
	return p == preconditions{}
}

// ConditionsMet reports whether every condition holds for an object of the
// given generation, zero meaning none. It satisfies backend.Conditions, so
// the paths that create an object can hand it down and have it answered where
// the object is published rather than a moment beforehand.
func (p preconditions) ConditionsMet(activeGeneration int64) bool {
	if p.ifGenerationMatch != nil && *p.ifGenerationMatch != activeGeneration {
		return false
	}
	if p.ifGenerationNotMatch != nil && *p.ifGenerationNotMatch == activeGeneration {
		return false
	}
	if activeGeneration == 0 {
		// An object that is not there has no metadata to have a generation of.
		return true
	}
	if p.ifMetagenerationMatch != nil && *p.ifMetagenerationMatch != objectMetageneration {
		return false
	}
	if p.ifMetagenerationNotMatch != nil && *p.ifMetagenerationNotMatch == objectMetageneration {
		return false
	}
	return true
}

// parsePreconditions reads the conditions a request names. The prefix picks
// which family: empty for the ifGenerationMatch naming the object being
// written, "Source" for the ifSourceGenerationMatch a copy or a rewrite names
// for the object it reads.
func parsePreconditions(query url.Values, prefix string) (preconditions, error) {
	var result preconditions
	for _, condition := range []struct {
		name string
		dest **int64
	}{
		{"if" + prefix + "GenerationMatch", &result.ifGenerationMatch},
		{"if" + prefix + "GenerationNotMatch", &result.ifGenerationNotMatch},
		{"if" + prefix + "MetagenerationMatch", &result.ifMetagenerationMatch},
		{"if" + prefix + "MetagenerationNotMatch", &result.ifMetagenerationNotMatch},
	} {
		value := query.Get(condition.name)
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return preconditions{}, fmt.Errorf("invalid %s: %w", condition.name, err)
		}
		*condition.dest = &parsed
	}
	return result, nil
}

// checkPreconditions answers the conditions a request names against the live
// object at bucketName/objectName, one that is not there counting as
// generation zero. It returns nil when they all hold and the response to send
// when one does not.
func (s *Server) checkPreconditions(r *http.Request, bucketName, objectName string) *jsonResponse {
	conditions, err := parsePreconditions(r.URL.Query(), "")
	if err != nil {
		return &jsonResponse{status: http.StatusBadRequest, errorMessage: err.Error()}
	}
	if conditions.empty() {
		return nil
	}
	var generation int64
	if obj, err := s.backend.GetObject(bucketName, objectName); err == nil {
		generation = obj.Generation
		obj.Close()
	}
	if !conditions.ConditionsMet(generation) {
		response := errToJsonResponse(backend.PreConditionFailed)
		return &response
	}
	return nil
}
