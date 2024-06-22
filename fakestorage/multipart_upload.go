package fakestorage

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jonmseaman/gcs-xml-multipart-client/multipartclient"
)

func (s *Server) initiateMultipartUpload(r *http.Request) xmlResponse {
	vars := unescapeMuxVars(mux.Vars(r))
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]
	uploadID, err := generateUploadID()
	if err != nil {
		return xmlResponse{errorMessage: err.Error()}
	}

	s.mpus.Store(uploadID, nil)
	return xmlResponse{
		status: http.StatusOK,
		data: multipartclient.InitiateMultipartUploadResult{
			Bucket:   bucketName,
			Key:      objectName,
			UploadID: uploadID,
		},
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
	return xmlResponse{
		status: 501,
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
