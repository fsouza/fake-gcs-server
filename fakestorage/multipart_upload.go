package fakestorage

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

type objectPart struct {
	PartNumber   int
	LastModified time.Time
	Etag         string
	Size         int64
	Content      []byte
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

	metadata := make(map[string]string)
	for key, value := range r.Header {
		if !strings.HasPrefix(key, "X-Goog-Meta-") {
			continue
		}
		metakey := strings.TrimPrefix(key, "X-Goog-Meta-")
		if len(r.Header[key]) != 1 {
			return xmlResponse{
				status:       http.StatusBadRequest,
				errorMessage: fmt.Sprintf("unexpected number of metadata values for key: %s", metakey),
			}
		}
		metadata[metakey] = value[0]
	}

	s.mpus.Store(uploadID, &multipartUpload{
		ObjectAttrs: ObjectAttrs{
			BucketName: bucketName,
			Name:       objectName,
			Metadata:   metadata,
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
		PartNumber: partNumber,
		Content:    partBody,
	}
	mpu.parts[partNumber] = part

	return xmlResponse{
		status: http.StatusOK,
	}
}

type completeMultipartUploadPart struct {
	PartNumber int    `xml:"PartNumber"`
	Etag       string `xml:"ETag"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name                      `xml:"CompleteMultipartUpload"`
	Parts   []completeMultipartUploadPart `xml:"Part"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

func (s *Server) completeMultipartUpload(r *http.Request) xmlResponse {
	vars := unescapeMuxVars(mux.Vars(r))
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]
	uploadID := vars["uploadId"]

	val, ok := s.mpus.LoadAndDelete(uploadID)
	if !ok {
		return xmlResponse{
			status:       http.StatusNotFound,
			errorMessage: "upload id not found",
		}
	}
	mpu := val.(*multipartUpload)
	if mpu.BucketName != bucketName {
		return xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: "bucket name mismatch",
		}
	}
	if mpu.Name != objectName {
		return xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: "object name mismatch",
		}
	}

	request := completeMultipartUploadRequest{}
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return xmlResponse{
			status:       http.StatusInternalServerError,
			errorMessage: fmt.Sprintf("failed to read request body: %s", err),
		}
	}
	err = xml.Unmarshal(bodyBytes, &request)
	if err != nil {
		return xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: fmt.Sprintf("failed to parse request body: %s", err),
		}
	}

	// TODO: Calculate the ETag based on the parts.
	result := completeMultipartUploadResult{
		Bucket:   mpu.BucketName,
		Key:      mpu.Name,
		Location: fmt.Sprintf("http://%s/%s/%s", r.Host, bucketName, objectName),
		ETag:     mpu.Etag,
	}
	return xmlResponse{
		status: 200,
		data:   result,
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

type listObjectPartsPart struct {
	XMLName      xml.Name  `xml:"Part"`
	PartNumber   int       `xml:"PartNumber"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
}

type listObjectPartsResult struct {
	XMLName              xml.Name              `xml:"ListPartsResult"`
	Bucket               string                `xml:"Bucket"`
	Key                  string                `xml:"Key"`
	UploadID             string                `xml:"UploadId"`
	StorageClass         string                `xml:"StorageClass"`
	PartNumberMarker     int                   `xml:"PartNumberMarker"`
	NextPartNumberMarker int                   `xml:"NextPartNumberMarker"`
	MaxParts             int                   `xml:"MaxParts"`
	IsTruncated          bool                  `xml:"IsTruncated"`
	Parts                []listObjectPartsPart `xml:"Part"`
}

// TODO: Implement this function
// - [ ] Query string for max-parts
// - [ ] Query string for part-number-marker
// - [ ] Query string for upload-id-marker
func (s *Server) listObjectParts(r *http.Request) xmlResponse {
	vars := unescapeMuxVars(mux.Vars(r))
	bucketName := vars["bucketName"]
	uploadID := vars["uploadId"]

	val, ok := s.mpus.LoadAndDelete(uploadID)
	if !ok {
		return xmlResponse{
			status: http.StatusNotFound,
		}
	}
	mpu := val.(*multipartUpload)
	if mpu.BucketName != bucketName {
		return xmlResponse{ // TODO: Verify this in the docs.
			status:       http.StatusBadRequest,
			errorMessage: "bucket name mismatch",
		}
	}

	// TODO: Unit Test for not found.
	result := listObjectPartsResult{
		Bucket:   bucketName,
		Key:      mpu.Name,
		UploadID: uploadID,
	}
	for _, part := range mpu.parts {
		result.Parts = append(result.Parts, listObjectPartsPart{
			PartNumber:   part.PartNumber,
			LastModified: part.LastModified,
			ETag:         part.Etag,
			Size:         part.Size,
		})
	}
	return xmlResponse{
		status: 200,
		data:   result,
	}
}
