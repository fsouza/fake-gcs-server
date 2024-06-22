package fakestorage

import (
	"encoding/xml"
	"net/http"

	"github.com/gorilla/mux"
)

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

	s.mpus.Store(uploadID, nil)
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
	return xmlResponse{
		status: 501,
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

func (s *Server) listMultipartUploads(r *http.Request) xmlResponse {
	return xmlResponse{
		status: 501,
	}
}

func (s *Server) listObjectParts(r *http.Request) xmlResponse {
	return xmlResponse{
		status: 501,
	}
}
