// Package rpc exposes a real gRPC payment endpoint using protobuf Struct messages.
// A typed .proto schema is provided in proto/payment.proto for production evolution.
package rpc

import (
	"context"
	"fmt"
	"github.com/saiteja0367/nexora-payment-platform/distributed/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"net"
)

type PaymentServer interface {
	Create(context.Context, *structpb.Struct) (*structpb.Struct, error)
	Get(context.Context, *structpb.Struct) (*structpb.Struct, error)
}
type Handler struct{ DB *store.Store }

func (h *Handler) Create(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	key := req.GetFields()["idempotency_key"].GetStringValue()
	amount := req.GetFields()["amount_cents"].GetNumberValue()
	if key == "" || amount <= 0 || amount != float64(int64(amount)) || amount > 9007199254740991 {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key and positive integer amount_cents required")
	}
	p, err := h.DB.Create(ctx, key, int64(amount))
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return structpb.NewStruct(map[string]any{"id": p.ID, "status": p.Status, "amount_cents": float64(p.AmountCents)})
}
func (h *Handler) Get(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	p, err := h.DB.Get(ctx, req.GetFields()["id"].GetStringValue())
	if err != nil {
		return nil, status.Error(codes.NotFound, "payment not found")
	}
	return structpb.NewStruct(map[string]any{"id": p.ID, "status": p.Status, "amount_cents": float64(p.AmountCents)})
}
func Register(s *grpc.Server, h PaymentServer) {
	s.RegisterService(&grpc.ServiceDesc{ServiceName: "nexora.PaymentService", HandlerType: (*PaymentServer)(nil), Methods: []grpc.MethodDesc{
		{MethodName: "Create", Handler: func(_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			r := new(structpb.Struct)
			if err := dec(r); err != nil {
				return nil, err
			}
			f := func(ctx context.Context, req any) (any, error) { return h.Create(ctx, req.(*structpb.Struct)) }
			if interceptor == nil {
				return f(ctx, r)
			}
			return interceptor(ctx, r, &grpc.UnaryServerInfo{FullMethod: "/nexora.PaymentService/Create"}, f)
		}},
		{MethodName: "Get", Handler: func(_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			r := new(structpb.Struct)
			if err := dec(r); err != nil {
				return nil, err
			}
			f := func(ctx context.Context, req any) (any, error) { return h.Get(ctx, req.(*structpb.Struct)) }
			if interceptor == nil {
				return f(ctx, r)
			}
			return interceptor(ctx, r, &grpc.UnaryServerInfo{FullMethod: "/nexora.PaymentService/Get"}, f)
		}},
	}}, h)
}
func Serve(addr string, h PaymentServer) error {
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return fmt.Errorf("grpc listen: %w", e)
	}
	s := grpc.NewServer()
	Register(s, h)
	return s.Serve(l)
}
