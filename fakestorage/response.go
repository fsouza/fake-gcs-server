package fakestorage

import "sort"

type listResponse struct {
	Kind  string        `json:"kind"`
	Items []interface{} `json:"items"`
}

func newListBucketsResponse(bucketNames []string) listResponse {
	resp := listResponse{
		Kind:  "storage#buckets",
		Items: make([]interface{}, len(bucketNames)),
	}
	sort.Strings(bucketNames)
	for i, name := range bucketNames {
		resp.Items[i] = newBucketResponse(name)
	}
	return resp
}

type bucketResponse struct {
	Kind string `json:"kind"`
	ID   string `json:"ID"`
	Name string `json:"Name"`
}

func newBucketResponse(bucketName string) bucketResponse {
	return bucketResponse{
		Kind: "storage#bucket",
		ID:   bucketName,
		Name: bucketName,
	}
}

func newListObjectsResponse(objs []Object, server *Server) listResponse {
	resp := listResponse{
		Kind:  "storage#objects",
		Items: make([]interface{}, len(objs)),
	}
	for i, obj := range objs {
		resp.Items[i] = newObjectResponse(obj, server)
	}
	return resp
}

type objectResponse struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	ID     string `json:"id"`
	Bucket string `json:"bucket"`
	Size   int64  `json:"size,string"`
}

func newObjectResponse(obj Object, server *Server) objectResponse {
	return objectResponse{
		Kind:   "storage#object",
		ID:     obj.id(),
		Bucket: obj.BucketName,
		Name:   obj.Name,
		Size:   int64(len(obj.Content)),
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
