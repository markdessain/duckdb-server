package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
	flightpb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/marcboeker/go-duckdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	Launch(context.Background())
}

// SimpleFlightServer implements the Flight service
type SimpleFlightServer struct {
	flight.BaseFlightServer
	alloc memory.Allocator
}

func NewSimpleFlightServer() *SimpleFlightServer {
	return &SimpleFlightServer{
		alloc: memory.NewGoAllocator(),
	}
}

func (s *SimpleFlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	fmt.Println("DoAction")
	// 	switch action.Type {
	// 	// case "BeginTransaction":
	// 	// 	return s.BeginTransaction(stream)
	// 	default:
	return status.Errorf(codes.Unimplemented, "unknown action type: %s", action.Type)
	// }
}

// func (s *SimpleFlightServer) BeginTransaction(stream flight.FlightService_DoActionServer) error {

// 	stream.SendMsg("test")
// 	return nil
// }

// func (s *SimpleFlightServer) BeginTransaction(_ context.Context, req flightsql.ActionBeginTransactionRequest) (id []byte, err error) {
// 	return []byte("my_transaction_id_1234"), nil
// }

func (s *SimpleFlightServer) GetFlightInfo(ctx context.Context, ticket *flightpb.FlightDescriptor) (*flightpb.FlightInfo, error) {
	fmt.Println("GetFlightInfo")

	fmt.Println("A")
	fmt.Println(ticket)
	fmt.Println("B")
	fmt.Println(string(ticket.GetCmd()))
	fmt.Println("C")
	// query := string(ticket.GetCmd())
	query := "SELECT" + strings.Split(string(ticket.GetCmd()), "SELECT")[1]
	c, err := duckdb.NewConnector("", nil)
	if err != nil {
		fmt.Println(err)
	}
	defer c.Close()

	conn, err := c.Connect(context.Background())
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()

	// Obtain the Arrow from the connection.
	arrow, err := duckdb.NewArrowFromConn(conn)

	rdr, err := arrow.QueryContext(context.Background(), query)
	if err != nil {
		fmt.Println(err)
	}
	defer rdr.Release()

	arrowSchema := rdr.Schema()
	schemaBytes := flight.SerializeSchema(arrowSchema, s.alloc)

	return &flight.FlightInfo{
		Schema: schemaBytes, // Start with no schema to avoid serialization issues
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flightpb.FlightDescriptor_CMD,
			Cmd:  ticket.Cmd,
		},
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{Ticket: []byte(query)},
			},
		},
		TotalRecords: 0,
		TotalBytes:   -1,
		// AppMetadata:  packed,
	}, nil
}

func (s *SimpleFlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	fmt.Println("DoGet")

	query := string(ticket.GetTicket())
	c, err := duckdb.NewConnector("", nil)
	if err != nil {
		fmt.Println(err)
	}
	defer c.Close()

	conn, err := c.Connect(context.Background())
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()

	// Obtain the Arrow from the connection.
	arrow, err := duckdb.NewArrowFromConn(conn)

	rdr, err := arrow.QueryContext(context.Background(), query)
	if err != nil {
		fmt.Println(err)
	}
	defer rdr.Release()

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(rdr.Schema()))
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Error closing writer: %v", err)
		}
	}()
	for rdr.Next() {
		writer.Write(rdr.RecordBatch())
	}

	return nil
}

func Launch(ctx context.Context) {
	server := NewSimpleFlightServer()

	// Create gRPC server
	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, server)

	// Listen on port 8080
	listener, err := net.Listen("tcp", ":32010")
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	go func() {
		<-ctx.Done()
		log.Println("Graceful stopping ...")
		grpcServer.GracefulStop()
		log.Println("Graceful shutdown completed.")
	}()

	log.Println("Starting Apache Flight server running on :32010")
	err = grpcServer.Serve(listener)
	if err != nil {
		log.Println("Failed to serve:", err)
	}
}
