package main

import (
	"context"
	"fmt"
	market "goSync/proto"
	"log"
	"net"

	"github.com/go-redis/redis/v8"
	"goSync/proto/market"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// redis init
var rdb *redis.Client

func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis failed connection: %v", err)
	}
	log.Println("Redis succeed init！")
}

type marketDataServer struct {
	market.UnimplementedMarketDataServiceServer
}

// GetLatestPrice
func (s *marketDataServer) GetLatestPrice(ctx context.Context, req *market.PriceRequest) (*market.PriceResponse, error) {
	symbol := req.GetSymbol()
	redisKey := fmt.Sprintf("market:%s", symbol)

	// read price
	price, err := rdb.HGet(ctx, redisKey, "price").Float64()
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to get price from Redis: %v", err)
	}

	// read qty
	qty, err := rdb.HGet(ctx, redisKey, "qty").Float64()
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to get qty from Redis: %v", err)
	}

	return &market.PriceResponse{
		Symbol: symbol,
		Price:  price,
		Qty:    qty,
	}, nil
}

func main() {
	// init redis
	initRedis()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("can;t listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	market.RegisterMarketDataServiceServer(grpcServer, &marketDataServer{})

	log.Println("gRPC server running on 50051...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC service failed at launch: %v", err)
	}
}
