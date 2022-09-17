package main

import (
	"bytes"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"grpc-workshop/go_server/pb"
	"io"
	"log"
	"net"
)

const port = ":9000"

type employeeService struct {
	pb.UnimplementedEmployeeServiceServer
}

func (e *employeeService) GetByBadgeNumber(ctx context.Context, request *pb.GetByBadgeNumberRequest) (*pb.EmployeeResponse, error) {
	if metaData, ok := metadata.FromIncomingContext(ctx); ok {
		fmt.Printf("Metadata received %v", metaData)
	}
	for _, e := range employees {
		if request.BadgeNumber == e.BadgeNumber {
			return &pb.EmployeeResponse{Employee: &e}, nil
		}
	}
	return nil, fmt.Errorf("employee with badge number %d not found", request.BadgeNumber)
}

func (e *employeeService) GetAll(request *pb.GetAllRequest, stream pb.EmployeeService_GetAllServer) error {
	for _, e := range employees {
		stream.Send(&pb.EmployeeResponse{Employee: &e}) // stream is closed when method exits
	}
	return nil
}

func (e *employeeService) Save(ctx context.Context, request *pb.EmployeeRequest) (*pb.EmployeeResponse, error) {
	employees = append(employees, *request.Employee)
	return &pb.EmployeeResponse{Employee: request.Employee}, nil
}

// SaveAll Send one message back for one received. This does not have to be that way.
// Possible to send as many messages back as needed: Spawn Go Routines and pass the stream around.
// Important: Method need to be kept alive (for example with blocking channel) as long as data is written to the stream.
// On Method Exit the Client is notified that the conn is closed.
func (e *employeeService) SaveAll(stream pb.EmployeeService_SaveAllServer) error {
	for { // Client always initiates the connection also in bidi streaming case
		emp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving incoming data stream %w", err)
		}
		employees = append(employees, *emp.Employee)
		if err := stream.Send(&pb.EmployeeResponse{Employee: emp.Employee}); err != nil {
			return fmt.Errorf("error confirming saved data  %w", err)
		}
	}
	fmt.Println("updated employees to:")
	for _, e := range employees {
		fmt.Printf("%v\n", e)
	}
	return nil
}

// AddPhoto impl: Instead of sending the badge id with every byte chunk we pass it via metadata
func (e *employeeService) AddPhoto(stream pb.EmployeeService_AddPhotoServer) error {
	if metaData, ok := metadata.FromIncomingContext(stream.Context()); ok {
		fmt.Printf("Badge number from metadata received %v\n", metaData["badgenumber"][0])
	} else {
		return errors.New("request without badge number in metadata")
	}
	buffer := &bytes.Buffer{}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF { // Stream closed client is finished sending
			fmt.Printf("Client closed connection. Flushing the buffer with final bytes %v\n", buffer.Bytes())
			return stream.SendAndClose(&pb.AddPhotoResponse{IsOk: true}) // Send confirm then settle
		}
		if err != nil {
			return fmt.Errorf("error receiving incoming byte stream %w", err)
		}
		fmt.Printf("Received partial byte chunk of size %d\n", len(chunk.Data))
		buffer.Write(chunk.Data)
	}
	return nil
}

func main() {
	transport, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterEmployeeServiceServer(server, &employeeService{})
	log.Printf("Starting grpc server on port %s\n", port)
	if err := server.Serve(transport); err != nil {
		panic(err)
	}
}

var employees = []pb.Employee{
	{
		Id:                  1,
		BadgeNumber:         2080,
		FirstName:           "Grace",
		LastName:            "Decker",
		VacationAccrualRate: 2,
		VacationAccrued:     30,
	},
	{
		Id:                  2,
		BadgeNumber:         2020,
		FirstName:           "Mark",
		LastName:            "Mowner",
		VacationAccrualRate: 4,
		VacationAccrued:     30.2,
	},
	{
		Id:                  3,
		BadgeNumber:         2010,
		FirstName:           "Tim",
		LastName:            "Limits",
		VacationAccrualRate: 8,
		VacationAccrued:     31.5,
	},
}
