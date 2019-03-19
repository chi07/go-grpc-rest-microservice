package v1

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/shinichi2510/go-grpc-rest-microservice/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

const (
	apiVersion = "v1"
)

type toDoServiceServer struct {
	db *sql.DB
}

// NewTodoServiceServer creates new todo service
func NewTodoServiceServer(db *sql.DB) v1.ToDoServiceServer {
	return &toDoServiceServer{db: db}
}

// check API
func (s *toDoServiceServer) checkAPI(api string) error {
	if len(api) > 0 {
		if apiVersion != api {
			return status.Errorf(codes.Unimplemented, "unsupported api version: service implements API version '%s', but asked api version '%s'", apiVersion, api)
		}
	}
	return nil
}

// connect function returns database connection from pool
func (s *toDoServiceServer) connect(ctx context.Context) (*sql.Conn, error) {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return nil, status.Error(codes.Unknown, "cannot connect to database "+err.Error())
	}
	return c, nil
}

func (s *toDoServiceServer) Create(ctx context.Context, r *v1.CreateRequest) (*v1.CreateResponse, error) {
	if err := s.checkAPI(r.Api); err != nil {
		return nil, err
	}

	// get db connection
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	reminder, err := ptypes.Timestamp(r.ToDo.Reminder)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "reminder filed has invalid "+err.Error())
	}

	// insert in database
	query := "INSERT INTO ToDo(`Title`, `Description`, `Reminder`) VALUES(?, ?, ?)"
	stm, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "reminder filed has invalid "+err.Error())
	}

	res, err := stm.ExecContext(ctx, r.ToDo.Title, r.ToDo.Description, reminder)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to insert into ToDo-> "+err.Error())
	}

	id, err := res.LastInsertId()
	if err != nil {
		return nil, status.Error(codes.Unknown, "can not retrieve id for create todo "+err.Error())
	}

	return &v1.CreateResponse{
		Api: apiVersion,
		Id:  id,
	}, nil
}

func (s *toDoServiceServer) Read(ctx context.Context, r *v1.ReadRequest) (*v1.ReadResponse, error) {
	if err := s.checkAPI(r.Api); err != nil {
		return nil, err
	}

	// get db connection
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// insert in database
	query := "SELECT `ID`, `Title`, `Description`, `Reminder` FROM ToDo WHERE `ID`=?"
	stm, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "can not prepare query: "+err.Error())
	}

	rows, err := stm.QueryContext(ctx, r.Id)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select from ToDo-> "+err.Error())
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, status.Error(codes.Unknown, "failed to retrieve data from ToDo-> "+err.Error())
		}
		return nil, status.Error(codes.NotFound, fmt.Sprintf("ToDo with ID='%d' is not found",
			r.Id))
	}

	// get ToDo data
	var td v1.ToDo
	var reminder time.Time
	if err := rows.Scan(&td.Id, &td.Title, &td.Description, &reminder); err != nil {
		return nil, status.Error(codes.Unknown, "failed to retrieve field values from ToDo row-> "+err.Error())
	}
	td.Reminder, err = ptypes.TimestampProto(reminder)
	if err != nil {
		return nil, status.Error(codes.Unknown, "reminder field has invalid format-> "+err.Error())
	}

	if rows.Next() {
		return nil, status.Error(codes.Unknown, fmt.Sprintf("found multiple ToDo rows with ID='%d'",
			r.Id))
	}

	return &v1.ReadResponse{
		Api:  apiVersion,
		ToDo: &td,
	}, nil
}

// Read all todo tasks
func (s *toDoServiceServer) ReadAll(ctx context.Context, req *v1.ReadAllRequest) (*v1.ReadAllResponse, error) {
	// check if the API version requested by client is supported by server
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	// get SQL connection from pool
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// get ToDo list
	query := "SELECT `ID`, `Title`, `Description`, `Reminder` FROM ToDo"
	stm, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Can not prepare context "+err.Error())
	}

	rows, err := stm.QueryContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select from ToDo-> "+err.Error())
	}
	defer rows.Close()

	var reminder time.Time
	list := make([]*v1.ToDo, 0)
	for rows.Next() {
		td := new(v1.ToDo)
		if err := rows.Scan(&td.Id, &td.Title, &td.Description, &reminder); err != nil {
			return nil, status.Error(codes.Unknown, "failed to retrieve field values from ToDo row-> "+err.Error())
		}
		td.Reminder, err = ptypes.TimestampProto(reminder)
		if err != nil {
			return nil, status.Error(codes.Unknown, "reminder field has invalid format-> "+err.Error())
		}
		list = append(list, td)
	}

	if err := rows.Err(); err != nil {
		return nil, status.Error(codes.Unknown, "failed to retrieve data from ToDo-> "+err.Error())
	}

	return &v1.ReadAllResponse{
		Api:   apiVersion,
		ToDos: list,
	}, nil
}

func (s *toDoServiceServer) Update(ctx context.Context, r *v1.UpdateRequest) (*v1.UpdateResponse, error) {
	if err := s.checkAPI(r.Api); err != nil {
		return nil, err
	}

	// get db connection
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	reminder, err := ptypes.Timestamp(r.ToDo.Reminder)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "reminder filed has invalid "+err.Error())
	}
	// insert in database
	query := "UPDATE ToDo SET `Title`=?, `Description`=?, `Reminder`=?"
	stm, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Can not prepare context "+err.Error())
	}

	res, err := stm.ExecContext(ctx, r.ToDo.Title, r.ToDo.Description, reminder)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to update into ToDo-> "+err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to retrieve row affected "+err.Error())
	}

	if rows == 0 {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Todo with ID='%d' is not found", r.ToDo.Id))
	}

	return &v1.UpdateResponse{
		Api:     apiVersion,
		Updated: rows,
	}, nil
}

// Delete todo task
func (s *toDoServiceServer) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	// check if the API version requested by client is supported by server
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	// get SQL connection from pool
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// delete ToDo
	query := "DELETE FROM ToDo WHERE `ID`=?"
	stmt, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to prepare statement ToDo-> "+err.Error())
	}

	res, err := stmt.ExecContext(ctx, req.Id)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to delete ToDo-> "+err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to retrieve rows affected value-> "+err.Error())
	}

	if rows == 0 {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("ToDo with ID='%d' is not found",
			req.Id))
	}

	return &v1.DeleteResponse{
		Api:     apiVersion,
		Deleted: rows,
	}, nil
}
