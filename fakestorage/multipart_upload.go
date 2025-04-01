package fakestorage

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// Multipart Upload
//
// Potential Follow Ups:
// - [ ] Store in-progress multipart upload part content in Storage.

type objectPart struct {
	PartNumber   int
	LastModified time.Time
	Etag         string
	Size         int64
	Content      []byte
	CRC32C       uint32
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

func formatCRC32C(crc32c uint32) string {
	crc32Bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(crc32Bytes, crc32c)
	crc32Str := base64.StdEncoding.EncodeToString(crc32Bytes)

	return crc32Str
}

func timeFromRequest(r *http.Request) (time.Time, error) {
	dateStr := r.Header.Get("Date")
	date, err := time.Parse(time.RFC1123, dateStr)
	if err != nil {
		return time.Time{}, err
	}

	return date, nil
}

func validateUploadObjectPartHashes(r *http.Request, crc32c string, md5 string) *xmlResponse {
	if r.Header.Get("Content-MD5") != "" && r.Header.Get("Content-MD5") != md5 {
		return &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: "BadDigest",
		}
	}
	hashes := r.Header["X-Goog-Hash"]
	for _, hash := range hashes {
		if !strings.HasPrefix(hash, "md5=") && !strings.HasPrefix(hash, "crc32c=") {
			return &xmlResponse{
				status:       http.StatusBadRequest,
				errorMessage: "InvalidDigest",
			}
		}
		if strings.HasPrefix(hash, "md5=") && strings.TrimPrefix(hash, "md5=") == md5 {
			continue
		}
		if strings.HasPrefix(hash, "crc32c=") && strings.TrimPrefix(hash, "crc32c") == crc32c {
			continue
		}
		return &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: "BadDigest",
		}
	}

	return nil
}

// TODO: Common error codes more than 5GiB part = 400 bad request
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
			errorMessage: "NoSuchUpload",
		}
	}
	mpu := val.(*multipartUpload)
	md5Writer := md5.New()
	crcWriter := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	wrappedReader := io.TeeReader(r.Body, io.MultiWriter(md5Writer, crcWriter))
	partBody, err := io.ReadAll(wrappedReader)
	if err != nil {
		return xmlResponse{
			status:       http.StatusInternalServerError,
			errorMessage: fmt.Sprintf("failed to read request body: %s", err),
		}
	}
	partMD5 := md5Writer.Sum(nil)
	partMD5Str := base64.StdEncoding.EncodeToString(partMD5[:])
	etag := fmt.Sprintf("\"%s\"", partMD5Str)
	crc32c := crcWriter.Sum32()
	crc32Str := formatCRC32C(crc32c)

	hashResp := validateUploadObjectPartHashes(r, crc32Str, partMD5Str)
	if hashResp != nil {
		return *hashResp
	}

	lastModified, err := timeFromRequest(r)
	if err != nil {
		return xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: fmt.Sprintf("failed to parse date: %s", err),
		}
	}

	part := objectPart{
		PartNumber:   partNumber,
		Content:      partBody,
		Etag:         etag,
		Size:         int64(len(partBody)),
		CRC32C:       crc32c,
		LastModified: lastModified,
	}
	mpu.parts[partNumber] = part

	return xmlResponse{
		status: http.StatusOK,
		header: http.Header{
			"ETag": []string{etag},
			"X-Goog-Hash": []string{
				fmt.Sprintf("md5=%s", partMD5Str),
				fmt.Sprintf("crc32c=%s", crc32Str)},
		},
		// Upload Object Part does not include a response body.
		data: nil,
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
