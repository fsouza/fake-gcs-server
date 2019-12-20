package backend

import "time"

// Bucket represents the bucket that is stored within the fake server.
type Bucket struct {
	Name              string
	VersioningEnabled bool
	TimeCreated       time.Time
}
