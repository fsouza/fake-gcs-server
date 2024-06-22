package fakestorage

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

type objectPart struct {
	Content []byte
}

type multipartUpload struct {
	ObjectAttrs
	parts map[int]objectPart
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

func (s *Server) initiateMultipartUpload(r *http.Request) xmlResponse {
	vars := unescapeMuxVars(mux.Vars(r))
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]
	uploadID, err := generateUploadID()
	if err != nil {
		return xmlResponse{errorMessage: err.Error()}
	}

	s.mpus.Store(uploadID, &multipartUpload{
		ObjectAttrs: ObjectAttrs{
			BucketName: bucketName,
			Name:       objectName,
		},
		parts: make(map[int]objectPart),
	})
	respBody := initiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      objectName,
		UploadID: uploadID,
	}
	return xmlResponse{
		status: http.StatusOK,
		data:   respBody,
	}
}

func (s *Server) uploadObjectPart(r *http.Request) xmlResponse {
	vars := unescapeMuxVars(mux.Vars(r))
	uploadID := vars["uploadId"]
	partNumber, err := strconv.Atoi(vars["partNumber"])
	if err != nil {
		return xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: fmt.Sprintf("bad partNumber: %v", partNumber),
		}
	}

	// Save the part upload to the server.
	val, ok := s.mpus.Load(uploadID)
	if !ok {
		return xmlResponse{
			status:       http.StatusNotFound,
			errorMessage: "upload id not found",
		}
	}
	mpu := val.(*multipartUpload)
	partBody, err := io.ReadAll(r.Body)
	if err != nil {
		return xmlResponse{
			status:       http.StatusInternalServerError,
			errorMessage: fmt.Sprintf("failed to read request body: %s", err),
		}
	}
	part := objectPart{
		Content: partBody,
	}
	mpu.parts[partNumber] = part

	return xmlResponse{
		status: http.StatusOK,
	}
}

func (s *Server) completeMultipartUpload(r *http.Request) xmlResponse {
	return xmlResponse{
		status: 501,
	}
}

func (s *Server) abortMultipartUpload(r *http.Request) xmlResponse {
	vars := unescapeMuxVars(mux.Vars(r))
	uploadID := vars["uploadId"]

	_, ok := s.mpus.LoadAndDelete(uploadID)
	if !ok {
		return xmlResponse{
			status: http.StatusNotFound,
		}
	}
	return xmlResponse{
		status: http.StatusNoContent,
	}
}

type listUpload struct {
	XMLName  xml.Name `xml:"Upload"`
	UploadID string   `xml:"UploadId"`
}

type listMultipartUploadsResult struct {
	XMLName xml.Name     `xml:"ListMultipartUploadsResult"`
	Bucket  string       `xml:"Bucket"`
	Uploads []listUpload `xml:"Upload"`
}

func (s *Server) listMultipartUploads(r *http.Request) xmlResponse {
	vars := unescapeMuxVars(mux.Vars(r))
	bucketName := vars["bucketName"]

	uploads := []listUpload{}
	s.mpus.Range(func(key, _ any) bool {
		uploads = append(uploads, listUpload{
			UploadID: key.(string),
		})
		return true
	})

	result := listMultipartUploadsResult{
		Bucket:  bucketName,
		Uploads: uploads,
	}
	return xmlResponse{
		status: 200,
		data:   result,
	}
}

func (s *Server) listObjectParts(r *http.Request) xmlResponse {
	return xmlResponse{
		status: 501,
	}
}
