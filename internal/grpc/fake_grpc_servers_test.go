package grpc

import (
	"context"

	pb "google.golang.org/genproto/googleapis/storage/v1"
	"google.golang.org/grpc/metadata"
)

type fakeInsertObjectServer struct {
	req *pb.InsertObjectRequest
}

// Context implements storage.Storage_InsertObjectServer.
func (f *fakeInsertObjectServer) Context() context.Context {
	panic("unimplemented")
}

// Recv implements storage.Storage_InsertObjectServer.
func (f *fakeInsertObjectServer) Recv() (*pb.InsertObjectRequest, error) {
	return f.req, nil
}

// RecvMsg implements storage.Storage_InsertObjectServer.
func (f *fakeInsertObjectServer) RecvMsg(m any) error {
	panic("unimplemented")
}

// SendAndClose implements storage.Storage_InsertObjectServer.
func (f *fakeInsertObjectServer) SendAndClose(*pb.Object) error {
	panic("unimplemented")
}

// SendHeader implements storage.Storage_InsertObjectServer.
func (f *fakeInsertObjectServer) SendHeader(metadata.MD) error {
	panic("unimplemented")
}

// SendMsg implements storage.Storage_InsertObjectServer.
func (f *fakeInsertObjectServer) SendMsg(m any) error {
	panic("unimplemented")
}

// SetHeader implements storage.Storage_InsertObjectServer.
func (f *fakeInsertObjectServer) SetHeader(metadata.MD) error {
	panic("unimplemented")
}

// SetTrailer implements storage.Storage_InsertObjectServer.
func (f *fakeInsertObjectServer) SetTrailer(metadata.MD) {
	panic("unimplemented")
}
