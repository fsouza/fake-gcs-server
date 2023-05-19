package grpc

import (
	"context"
	"io"

	pb "github.com/fsouza/fake-gcs-server/genproto/googleapis/storage/v1"
	"github.com/fsouza/fake-gcs-server/internal/backend"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	pb.UnimplementedStorageServerServer // To satisfy the interface without having to implement every method

	backend backend.Storage // share the same backend as fakeserver does
}

func InitServer(backend backend.Storage) *Server {
	return &Server{backend: backend}
}

func NewServerWithBackend(backend backend.Storage) *grpc.Server {
	grpcServer := grpc.NewServer()
	pb.RegisterStorageServerServer(grpcServer, InitServer(backend))
	reflection.Register(grpcServer)
	return grpcServer
}

func (g *Server) GetBucket(ctx context.Context, req *pb.GetBucketRequest) (*pb.Bucket, error) {
	bucket, err := g.backend.GetBucket(req.Bucket)
	if err != nil {
		return nil, err
	}

	// // using line 193 in storage_resources.pb.go, fill out this, using whatever the bucket returned has
	// // will have to just do this manually
	grpc_bucket := &pb.Bucket{
		Id:          bucket.Name,
		Name:        bucket.Name,
		Versioning:  &pb.Bucket_Versioning{Enabled: bucket.VersioningEnabled},
		TimeCreated: timestamppb.New(bucket.TimeCreated),
	}
	return grpc_bucket, nil
	///return GetBucketFromBackend(g.backend, req.Bucket)
}

func (g *Server) DeleteBucket(ctx context.Context, req *pb.DeleteBucketRequest) (*pb.Empty, error) {
	err := g.backend.DeleteBucket(req.Bucket)
	return &pb.Empty{}, err
}

func (g *Server) InsertBucket(ctx context.Context, req *pb.InsertBucketRequest) (*pb.Empty, error) {
	err := g.backend.CreateBucket(req.Bucket.Name, backend.BucketAttrs{})
	return &pb.Empty{}, err
}

func (g *Server) ListBuckets(context.Context, *pb.ListBucketsRequest) (*pb.Buckets, error) {
	buckets, err := g.backend.ListBuckets()
	if err != nil {
		return nil, err
	}

	resp := &pb.Buckets{}

	for _, bucket := range buckets {
		// tc, err := ptypes.TimestampProto(bucket.TimeCreated)
		tc := timestamppb.Now()
		if err != nil {
			return nil, err
		}
		resp.Bucket = append(resp.Bucket, &pb.Bucket{
			Name:        bucket.Name,
			TimeCreated: tc,
			Versioning: &pb.Bucket_Versioning{
				Enabled: bucket.VersioningEnabled,
			},
		})
	}

	return resp, nil
}

func (g *Server) InsertObject(ctx context.Context, req *pb.InsertObjectRequest) (*pb.Empty, error) {
	insert_obj_req := *(req.GetFirstMessage().(*pb.InsertObjectRequest_InsertObjectSpec))
	validObject := backend.Object{
		ObjectAttrs: backend.ObjectAttrs{
			BucketName: insert_obj_req.InsertObjectSpec.Resource.Bucket,
			Name:       insert_obj_req.InsertObjectSpec.Resource.Name,
		},
		Content: req.GetChecksummedData().Content,
	}
	_, err := g.backend.CreateObject(validObject.StreamingObject(), backend.NoConditions{})
	if err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (g *Server) GetObject(ctx context.Context, req *pb.GetObjectRequest) (*pb.GetObjectMediaResponse, error) {
	obj, err := g.backend.GetObject(req.Bucket, req.Object)
	if err != nil {
		return nil, err
	}
	content, err := io.ReadAll(obj.Content)
	if err != nil {
		return nil, err
	}

	checksummed_data := &pb.ChecksummedData{
		Content: content,
	}
	metadata := &pb.Object{
		Name:        obj.ObjectAttrs.Name,
		Bucket:      obj.ObjectAttrs.BucketName,
		Md5Hash:     obj.ObjectAttrs.Md5Hash,
		Generation:  obj.ObjectAttrs.Generation,
		ContentType: obj.ObjectAttrs.ContentType,
	}
	return &pb.GetObjectMediaResponse{
		ChecksummedData: checksummed_data,
		Metadata:        metadata,
	}, nil
}

func (g *Server) DeleteObject(ctx context.Context, req *pb.DeleteObjectRequest) (*pb.Empty, error) {
	err := g.backend.DeleteObject(req.Bucket, req.Object)
	return &pb.Empty{}, err
}

func (g *Server) UpdateObject(ctx context.Context, req *pb.UpdateObjectRequest) (*pb.Empty, error) {
	attrs := backend.ObjectAttrs{
		Metadata: req.Metadata.Metadata,
	}
	_, err := g.backend.UpdateObject(req.Bucket, req.Object, attrs)
	return &pb.Empty{}, err
}

func (g *Server) PatchObject(ctx context.Context, req *pb.PatchObjectRequest) (*pb.Empty, error) {
	attrs := backend.ObjectAttrs{
		Metadata: req.Metadata.Metadata,
	}
	_, err := g.backend.PatchObject(req.Bucket, req.Object, attrs)
	return &pb.Empty{}, err
}

// ComposeObject(bucketName string, objectNames []string, destinationName string, metadata map[string]string, contentType string)
func (g *Server) ComposeObject(ctx context.Context, req *pb.ComposeObjectRequest) (*pb.Empty, error) {
	sourceObjNames := make([]string, 2)
	for i := 0; i < len(req.SourceObjects); i++ {
		sourceObjNames[i] = req.SourceObjects[i].Name
	}
	_, err := g.backend.ComposeObject(req.DestinationBucket, sourceObjNames, req.DestinationObject, map[string]string{}, "")
	return &pb.Empty{}, err
}

func (g *Server) ListObjects(ctx context.Context, req *pb.ListObjectsRequest) (*pb.Objects, error) {
	objs, err := g.backend.ListObjects(req.Bucket, req.Prefix, req.Versions)
	if err != nil {
		return nil, err
	}

	resp := &pb.Objects{}
	for _, obj := range objs {
		resp.Object = append(resp.Object, &pb.Object{
			Name:        obj.Name,
			Bucket:      obj.BucketName,
			Md5Hash:     obj.Md5Hash,
			Generation:  obj.Generation,
			ContentType: obj.ContentType,
		})
	}

	return resp, nil
}
