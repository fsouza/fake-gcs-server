package backend

// Object represents the object that is stored within the fake server.
type Object struct {
	BucketName string
	Name       string
	Content    []byte
}

func (o *Object) ID() string {
	return o.BucketName + "/" + o.Name
}
