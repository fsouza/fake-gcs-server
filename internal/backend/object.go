package backend

// Object represents the object that is stored within the fake server.
type Object struct {
	BucketName string `json:"-"`
	Name       string `json:"name"`
	Content    []byte `json:"-"`
	// Crc32c checksum of Content. calculated by server when it's upload methods are used.
	Crc32c string `json:"crc32c,omitempty"`
}

func (o *Object) ID() string {
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
