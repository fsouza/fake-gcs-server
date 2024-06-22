package fakestorage

import (
	"net/http"
)

func (s *Server) initiateMultipartUpload(r *http.Request) xmlResponse {
	return xmlResponse{
		status: 501,
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
