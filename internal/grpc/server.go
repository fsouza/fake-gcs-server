package grpc

import (
	"context"

	"github.com/fsouza/fake-gcs-server/internal/backend"
	pb "google.golang.org/genproto/googleapis/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	pb.UnimplementedStorageServer // To satisfy the interface without having to implement every method

	backend backend.Storage // share the same backend as fakeserver does
}

func InitServer(backend backend.Storage) *Server {
	return &Server{backend: backend}
}

func NewServerWithBackend(backend backend.Storage) *grpc.Server {
	grpcServer := grpc.NewServer()
	pb.RegisterStorageServer(grpcServer, InitServer(backend))
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
		Id:                    bucket.Name,
		Name:                  bucket.Name,
		Versioning:            &pb.Bucket_Versioning{Enabled: bucket.VersioningEnabled},
		DefaultEventBasedHold: bucket.DefaultEventBasedHold,
		TimeCreated:           timestamppb.New(bucket.TimeCreated),
	}
	return grpc_bucket, nil
}

func (g *Server) DeleteBucket(ctx context.Context, req *pb.DeleteBucketRequest) (*emptypb.Empty, error) {
	err := g.backend.DeleteBucket(req.Bucket)
	return &emptypb.Empty{}, err
}

func (g *Server) InsertBucket(ctx context.Context, req *pb.InsertBucketRequest) (*pb.Bucket, error) {
	err := g.backend.CreateBucket(req.Bucket.Name, backend.BucketAttrs{DefaultEventBasedHold: req.Bucket.DefaultEventBasedHold})
	return &pb.Bucket{Name: req.Bucket.Name}, err
}

func (g *Server) UpdateBucket(ctx context.Context, req *pb.UpdateBucketRequest) (*pb.Bucket, error) {
	updatedBucketAttrs := backend.BucketAttrs{
		DefaultEventBasedHold: req.Metadata.DefaultEventBasedHold,
	}
	err := g.backend.UpdateBucket(req.Bucket, updatedBucketAttrs)
	return &pb.Bucket{}, err
}

func (g *Server) ListBuckets(context.Context, *pb.ListBucketsRequest) (*pb.ListBucketsResponse, error) {
	buckets, err := g.backend.ListBuckets()
	if err != nil {
		return nil, err
	}

	var resp pb.ListBucketsResponse
	for _, bucket := range buckets {
		resp.Items = append(resp.Items, &pb.Bucket{
			Name:        bucket.Name,
			TimeCreated: timestamppb.New(bucket.TimeCreated),
			Versioning: &pb.Bucket_Versioning{
				Enabled: bucket.VersioningEnabled,
			},
		})
	}

	return &resp, nil
}

func (g *Server) InsertObject(server pb.Storage_InsertObjectServer) error {
	req, err := server.Recv()
	if err != nil {
		return err
	}
	insert_obj_req := *(req.GetFirstMessage().(*pb.InsertObjectRequest_InsertObjectSpec))
	validObject := backend.Object{
		ObjectAttrs: backend.ObjectAttrs{
			BucketName: insert_obj_req.InsertObjectSpec.Resource.Bucket,
			Name:       insert_obj_req.InsertObjectSpec.Resource.Name,
		},
		Content: req.GetChecksummedData().Content,
	}
	_, err = g.backend.CreateObject(validObject.StreamingObject(), backend.NoConditions{})
	return err
}

func (g *Server) GetObject(ctx context.Context, req *pb.GetObjectRequest) (*pb.Object, error) {
	obj, err := g.backend.GetObject(req.Bucket, req.Object)
	if err != nil {
		return nil, err
	}

	return &pb.Object{
		Name:               obj.ObjectAttrs.Name,
		Bucket:             obj.ObjectAttrs.BucketName,
		StorageClass:       obj.ObjectAttrs.StorageClass,
		Md5Hash:            obj.ObjectAttrs.Md5Hash,
		Generation:         obj.ObjectAttrs.Generation,
		ContentType:        obj.ObjectAttrs.ContentType,
		ContentDisposition: obj.ObjectAttrs.ContentDisposition,
		ContentLanguage:    obj.ObjectAttrs.ContentLanguage,
	}, nil
}

func (g *Server) DeleteObject(ctx context.Context, req *pb.DeleteObjectRequest) (*emptypb.Empty, error) {
	err := g.backend.DeleteObject(req.Bucket, req.Object)
	return &emptypb.Empty{}, err
}

func (g *Server) UpdateObject(ctx context.Context, req *pb.UpdateObjectRequest) (*pb.Object, error) {
	attrs := backend.ObjectAttrs{
		Metadata: req.Metadata.Metadata,
	}
	obj, err := g.backend.UpdateObject(req.Bucket, req.Object, attrs)
	return makeObject(obj), err
}

func (g *Server) PatchObject(ctx context.Context, req *pb.PatchObjectRequest) (*pb.Object, error) {
	attrs := backend.ObjectAttrs{
		Metadata:           req.Metadata.Metadata,
		ContentType:        req.Metadata.ContentType,
		ContentEncoding:    req.Metadata.ContentEncoding,
		ContentDisposition: req.Metadata.ContentDisposition,
		ContentLanguage:    req.Metadata.ContentLanguage,
	}
	obj, err := g.backend.PatchObject(req.Bucket, req.Object, attrs)
	return makeObject(obj), err
}

// ComposeObject(bucketName string, objectNames []string, destinationName string, metadata map[string]string, contentType string, contentDisposition string, contentLanguage string)
func (g *Server) ComposeObject(ctx context.Context, req *pb.ComposeObjectRequest) (*pb.Object, error) {
	sourceObjNames := make([]string, 2)
	for i := 0; i < len(req.SourceObjects); i++ {
		sourceObjNames[i] = req.SourceObjects[i].Name
	}
	obj, err := g.backend.ComposeObject(req.DestinationBucket, sourceObjNames, req.DestinationObject, map[string]string{}, "", "", "")
	return makeObject(obj), err
}

func (g *Server) ListObjects(ctx context.Context, req *pb.ListObjectsRequest) (*pb.ListObjectsResponse, error) {
	objs, err := g.backend.ListObjects(req.Bucket, req.Prefix, req.Versions)
	if err != nil {
		return nil, err
	}

	var resp pb.ListObjectsResponse
	for _, obj := range objs {
		resp.Items = append(resp.Items, &pb.Object{
			Name:        obj.Name,
			Bucket:      obj.BucketName,
			Md5Hash:     obj.Md5Hash,
			Generation:  obj.Generation,
			ContentType: obj.ContentType,
		})
	}

	return &resp, nil
}

func makeObject(obj backend.StreamingObject) *pb.Object {
	return &pb.Object{
		Name:        obj.Name,
		Bucket:      obj.BucketName,
		Md5Hash:     obj.Md5Hash,
		Generation:  obj.Generation,
		ContentType: obj.ContentType,
	}
}
