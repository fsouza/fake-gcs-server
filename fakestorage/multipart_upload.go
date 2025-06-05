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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// Part size validation constants according to GCS documentation
const (
	minAllowedPartSize   = 5 * 1024 * 1024        // 5 MiB minimum (except last part)
	maxAllowedPartSize   = 5 * 1024 * 1024 * 1024 // 5 GiB maximum
	minAllowedPartNumber = 1
	maxAllowedPartNumber = 10000

	// List object parts pagination constants
	defaultMaxParts = 1000
	maxMaxParts     = 1000
)

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

func validateUploadObjectPart(r *http.Request, partNumber int, partSize int64) *xmlResponse {
	// Validate part number range
	if partNumber < minAllowedPartNumber || partNumber > maxAllowedPartNumber {
		return &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: fmt.Sprintf("InvalidPartNumber: Part number must be between %d and %d", minAllowedPartNumber, maxAllowedPartNumber),
		}
	}

	// Validate maximum part size
	if partSize > maxAllowedPartSize {
		return &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: fmt.Sprintf("EntityTooLarge: Part size cannot exceed %d bytes", maxAllowedPartSize),
		}
	}

	// Validate Content-Length header if provided
	if contentLengthHeader := r.Header.Get("Content-Length"); contentLengthHeader != "" {
		contentLength, err := strconv.ParseInt(contentLengthHeader, 10, 64)
		if err != nil {
			return &xmlResponse{
				status:       http.StatusBadRequest,
				errorMessage: "InvalidHeader: Content-Length must be a valid integer",
			}
		}

		// Content-Length should match the actual part size
		if contentLength != partSize {
			return &xmlResponse{
				status:       http.StatusBadRequest,
				errorMessage: fmt.Sprintf("ContentLengthMismatch: Content-Length (%d) does not match actual part size (%d)", contentLength, partSize),
			}
		}

		// Apply the same size limits to Content-Length
		if contentLength > maxAllowedPartSize {
			return &xmlResponse{
				status:       http.StatusBadRequest,
				errorMessage: fmt.Sprintf("EntityTooLarge: Content-Length cannot exceed %d bytes", maxAllowedPartSize),
			}
		}
	}

	// For upload, we don't enforce minimum part size since we don't know if it's the last part yet
	// The minimum size validation will be done during complete multipart upload

	return nil
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
			errorMessage: "NoSuchUpload",
		}
	}
	mpu := val.(*multipartUpload)
	// Calculate hashes.
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

	// Validate the part number, Content-Length, and part sizes.
	partSize := int64(len(partBody))
	if validationResp := validateUploadObjectPart(r, partNumber, partSize); validationResp != nil {
		return *validationResp
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
		Size:         partSize,
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

func validateCompletePartSizeAndNum(partSize int64, partNumber int, isLastPart bool) *xmlResponse {
	// Validate part number range
	if partNumber < minAllowedPartNumber || partNumber > maxAllowedPartNumber {
		return &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: fmt.Sprintf("InvalidPartNumber: Part number must be between %d and %d", minAllowedPartNumber, maxAllowedPartNumber),
		}
	}

	// Validate maximum part size
	if partSize > maxAllowedPartSize {
		return &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: fmt.Sprintf("EntityTooLarge: Part size cannot exceed %d bytes", maxAllowedPartSize),
		}
	}

	// Validate minimum part size (except for the last part)
	if !isLastPart && partSize < minAllowedPartSize {
		return &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: fmt.Sprintf("PartTooSmall: Part size must be at least %d bytes (except for the last part)", minAllowedPartSize),
		}
	}

	return nil
}

func validateCompleteMultipartUploadRequest(s *Server, uploadID string, bucketName string, objectName string, r *http.Request) (*multipartUpload, *xmlResponse) {
	val, ok := s.mpus.LoadAndDelete(uploadID)
	if !ok {
		return nil, &xmlResponse{
			status:       http.StatusNotFound,
			errorMessage: "upload id not found",
		}
	}
	mpu := val.(*multipartUpload)
	if mpu.BucketName != bucketName {
		return nil, &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: "bucket name mismatch",
		}
	}
	if mpu.Name != objectName {
		return nil, &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: "object name mismatch",
		}
	}

	request := completeMultipartUploadRequest{}
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, &xmlResponse{
			status:       http.StatusInternalServerError,
			errorMessage: fmt.Sprintf("failed to read request body: %s", err),
		}
	}
	err = xml.Unmarshal(bodyBytes, &request)
	if err != nil {
		return nil, &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: fmt.Sprintf("failed to parse request body: %s", err),
		}
	}

	// Validate that all requested parts exist and ETags match
	if len(request.Parts) == 0 {
		return nil, &xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: "MalformedXML: Must specify at least one part",
		}
	}

	// Find the highest part number to determine which is the last part
	maxPartNumber := 0
	for _, requestedPart := range request.Parts {
		if requestedPart.PartNumber > maxPartNumber {
			maxPartNumber = requestedPart.PartNumber
		}
	}

	// Validate each requested part
	for _, requestedPart := range request.Parts {
		storedPart, exists := mpu.parts[requestedPart.PartNumber]
		if !exists {
			return nil, &xmlResponse{
				status:       http.StatusBadRequest,
				errorMessage: fmt.Sprintf("InvalidPart: Part number %d was not uploaded", requestedPart.PartNumber),
			}
		}

		// Validate ETag matches (if not wildcard "*")
		if requestedPart.Etag != "*" && requestedPart.Etag != storedPart.Etag {
			return nil, &xmlResponse{
				status:       http.StatusBadRequest,
				errorMessage: fmt.Sprintf("InvalidPart: ETag mismatch for part %d", requestedPart.PartNumber),
			}
		}

		isLastPart := requestedPart.PartNumber == maxPartNumber
		if sizeValidationResp := validateCompletePartSizeAndNum(storedPart.Size, requestedPart.PartNumber, isLastPart); sizeValidationResp != nil {
			return nil, sizeValidationResp
		}
	}
	return mpu, nil
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

	mpu, xr := validateCompleteMultipartUploadRequest(s, uploadID, bucketName, objectName, r)
	if xr != nil {
		return *xr
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

func (s *Server) listObjectParts(r *http.Request) xmlResponse {
	vars := unescapeMuxVars(mux.Vars(r))
	bucketName := vars["bucketName"]
	uploadID := vars["uploadId"]

	val, ok := s.mpus.Load(uploadID)
	if !ok {
		return xmlResponse{
			status: http.StatusNotFound,
		}
	}
	mpu := val.(*multipartUpload)
	if mpu.BucketName != bucketName {
		return xmlResponse{
			status:       http.StatusBadRequest,
			errorMessage: "bucket name mismatch",
		}
	}

	// Parse query parameters
	query := r.URL.Query()

	// Parse max-parts parameter (default 1000, max 1000)
	maxParts := defaultMaxParts
	if maxPartsStr := query.Get("max-parts"); maxPartsStr != "" {
		if parsed, err := strconv.Atoi(maxPartsStr); err == nil {
			if parsed > 0 && parsed <= maxMaxParts {
				maxParts = parsed
			}
		}
	}

	// Parse part-number-marker parameter
	partNumberMarker := 0
	if markerStr := query.Get("part-number-marker"); markerStr != "" {
		if parsed, err := strconv.Atoi(markerStr); err == nil && parsed > 0 {
			partNumberMarker = parsed
		}
	}

	// Convert parts map to sorted slice
	var allParts []objectPart
	for _, part := range mpu.parts {
		allParts = append(allParts, part)
	}

	// Sort parts by part number
	sort.Slice(allParts, func(i, j int) bool {
		return allParts[i].PartNumber < allParts[j].PartNumber
	})

	// Filter parts after the marker
	var filteredParts []objectPart
	for _, part := range allParts {
		if part.PartNumber > partNumberMarker {
			filteredParts = append(filteredParts, part)
		}
	}

	// Apply pagination
	var resultParts []listObjectPartsPart
	isTruncated := false
	nextPartNumberMarker := 0

	for i, part := range filteredParts {
		if i >= maxParts {
			isTruncated = true
			nextPartNumberMarker = part.PartNumber
			break
		}

		resultParts = append(resultParts, listObjectPartsPart{
			PartNumber:   part.PartNumber,
			LastModified: part.LastModified,
			ETag:         part.Etag,
			Size:         part.Size,
		})
	}

	// If we processed all remaining parts and there are more than maxParts total, check if truncated
	if !isTruncated && len(filteredParts) == maxParts && len(filteredParts) < len(allParts) {
		// Find if there are more parts after the last one we included
		if len(resultParts) > 0 {
			lastIncludedPartNumber := resultParts[len(resultParts)-1].PartNumber
			for _, part := range allParts {
				if part.PartNumber > lastIncludedPartNumber {
					isTruncated = true
					nextPartNumberMarker = part.PartNumber
					break
				}
			}
		}
	}

	result := listObjectPartsResult{
		Bucket:               bucketName,
		Key:                  mpu.Name,
		UploadID:             uploadID,
		StorageClass:         "STANDARD", // Default storage class
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                resultParts,
	}

	return xmlResponse{
		status: 200,
		data:   result,
	}
}
