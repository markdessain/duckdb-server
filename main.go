package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	flightpb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/marcboeker/go-duckdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func main() {

	flag := flag.NewFlagSet("server", flag.ExitOnError)
	dbPath := flag.String("db", "", "Path to DuckDB database file")
	flag.Parse(os.Args[1:])
	Launch(context.Background(), *dbPath)
}

// SimpleFlightServer implements the Flight service
type SimpleFlightServer struct {
	flight.BaseFlightServer
	alloc memory.Allocator
	conn  driver.Conn
}

func NewSimpleFlightServer(dbPath string) *SimpleFlightServer {

	// Create a blank DuckDB database if it doesn't exist
	if dbPath != "" {
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			// Create the database file
			db, err := sql.Open("duckdb", dbPath)
			if err != nil {
				panic(fmt.Sprintf("Failed to create database: %v", err))
			}
			db.Close()
			fmt.Printf("Created new database at: %s\n", dbPath)
		}
	}

	var connectionString string
	if dbPath != "" {
		connectionString = dbPath + "?access_mode=read_only"
	} else {
		connectionString = ""
	}
	c, err := duckdb.NewConnector(connectionString, nil)
	if err != nil {
		fmt.Println(err)
	}

	conn, err := c.Connect(context.Background())
	if err != nil {
		fmt.Println(err)
	}

	db := conn.(*duckdb.Conn)

	rows, err := db.QueryContext(context.Background(), "SELECT extension_name FROM duckdb_extensions() WHERE installed", []driver.NamedValue{})
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var extensions []string
	for {

		var name string
		x := []driver.Value{&name}
		err := rows.Next(x)

		if err != nil {
			break
		}
		extensions = append(extensions, x[0].(string))
	}

	fmt.Println(extensions)
	// Step 2: Load each extension
	for _, ext := range extensions {
		_, err := db.ExecContext(context.Background(), fmt.Sprintf("LOAD %s;", ext), []driver.NamedValue{})
		if err != nil {
			fmt.Printf("Failed to load extension %s: %v\n", ext, err)
		} else {
			fmt.Printf("Loaded extension: %s\n", ext)
		}
	}

	ret := &SimpleFlightServer{
		alloc: memory.NewGoAllocator(),
		conn:  conn,
	}

	return ret
}

func (s *SimpleFlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	fmt.Println("DoAction")
	return status.Errorf(codes.Unimplemented, "unknown action type: %s", action.Type)
}

func (s *SimpleFlightServer) GetFlightInfo(ctx context.Context, ticket *flightpb.FlightDescriptor) (*flightpb.FlightInfo, error) {
	fmt.Println("GetFlightInfo")

	msg := flightpb.CommandStatementQuery{}
	err := proto.Unmarshal(ticket.Cmd, &msg)
	if err != nil {
		fmt.Println(err)
	}

	if msg.GetQuery() == "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery" {
		fmt.Println("type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery")

		a := string(msg.GetTransactionId())
		query := string(a)[2:len(a)]

		// Obtain the Arrow from the connection.
		arrow, err := duckdb.NewArrowFromConn(s.conn)

		rdr, err := arrow.QueryContext(context.Background(), query)
		if err != nil {
			return nil, status.Errorf(codes.Canceled, err.Error())
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

	if msg.GetQuery() == "type.googleapis.com/arrow.flight.protocol.sql.CommandGetSqlInfo" {
		fmt.Println("type.googleapis.com/arrow.flight.protocol.sql.CommandGetSqlInfo")

		bldr := array.NewRecordBuilder(s.alloc, schema_ref.SqlInfo)
		defer bldr.Release()
		schemaBytes := flight.SerializeSchema(bldr.Schema(), s.alloc)

		return &flight.FlightInfo{
			Schema: schemaBytes,
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flightpb.FlightDescriptor_CMD,
				Cmd:  ticket.Cmd,
			},
			Endpoint: []*flight.FlightEndpoint{
				{
					Ticket: &flight.Ticket{Ticket: []byte("CommandGetSqlInfo")},
				},
			},
			TotalRecords: 0,
			TotalBytes:   -1,
		}, nil
	}

	return nil, status.Errorf(codes.Unimplemented, "unknown ticket type: %s", ticket.String())
}

func (s *SimpleFlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	fmt.Println("DoGet")

	query := string(ticket.GetTicket())

	if query == "CommandGetSqlInfo" {
		bldr := array.NewRecordBuilder(s.alloc, schema_ref.SqlInfo)
		defer bldr.Release()

		writer := flight.NewRecordWriter(stream, ipc.WithSchema(bldr.Schema()))
		fmt.Println(writer)

		nameFieldBldr := bldr.Field(0).(*array.Uint32Builder)
		valFieldBldr := bldr.Field(1).(*array.DenseUnionBuilder)

		sqlInfoResultBldr := newSqlInfoResultBuilder(valFieldBldr)

		for k, v := range SqlInfoResultMap() {
			nameFieldBldr.Append(k)
			sqlInfoResultBldr.Append(v)
		}

		batch := bldr.NewRecord()
		defer batch.Release()

		writer.Write(batch)

		return nil
	} else {

		// Obtain the Arrow from the connection.
		arrow, err := duckdb.NewArrowFromConn(s.conn)

		rdr, err := arrow.QueryContext(context.Background(), query)
		if err != nil {
			return status.Errorf(codes.Canceled, err.Error())
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

	// return status.Errorf(codes.Unimplemented, "unknown ticket type: %s", ticket.String())

}

func Launch(ctx context.Context, dbPath string) {
	server := NewSimpleFlightServer(dbPath)
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

// HELPERS
//
//

const (
	strValIdx arrow.UnionTypeCode = iota
	boolValIdx
	bigintValIdx
	int32BitMaskIdx
	strListIdx
	int32ToInt32ListIdx
)

// sqlInfoResultBldr is a helper for building up the dense union response
// of a SqlInfo request.
type sqlInfoResultBldr struct {
	valueBldr *array.DenseUnionBuilder

	strBldr              *array.StringBuilder
	boolBldr             *array.BooleanBuilder
	bigintBldr           *array.Int64Builder
	int32BitmaskBldr     *array.Int32Builder
	strListBldr          *array.ListBuilder
	int32Toint32ListBldr *array.MapBuilder
}

func newSqlInfoResultBuilder(valueBldr *array.DenseUnionBuilder) *sqlInfoResultBldr {
	return &sqlInfoResultBldr{
		valueBldr:            valueBldr,
		strBldr:              valueBldr.Child(int(strValIdx)).(*array.StringBuilder),
		boolBldr:             valueBldr.Child(int(boolValIdx)).(*array.BooleanBuilder),
		bigintBldr:           valueBldr.Child(int(bigintValIdx)).(*array.Int64Builder),
		int32BitmaskBldr:     valueBldr.Child(int(int32BitMaskIdx)).(*array.Int32Builder),
		strListBldr:          valueBldr.Child(int(strListIdx)).(*array.ListBuilder),
		int32Toint32ListBldr: valueBldr.Child(int(int32ToInt32ListIdx)).(*array.MapBuilder),
	}
}

func (s *sqlInfoResultBldr) Append(v interface{}) {
	switch v := v.(type) {
	case string:
		s.valueBldr.Append(strValIdx)
		s.strBldr.Append(v)
	case bool:
		s.valueBldr.Append(boolValIdx)
		s.boolBldr.Append(v)
	case int64:
		s.valueBldr.Append(bigintValIdx)
		s.bigintBldr.Append(v)
	case int32:
		s.valueBldr.Append(int32BitMaskIdx)
		s.int32BitmaskBldr.Append(v)
	case []string:
		s.valueBldr.Append(strListIdx)
		s.strListBldr.Append(true)
		chld := s.strListBldr.ValueBuilder().(*array.StringBuilder)
		chld.AppendValues(v, nil)
	case map[int32][]int32:
		s.valueBldr.Append(int32ToInt32ListIdx)
		s.int32Toint32ListBldr.Append(true)

		kb := s.int32Toint32ListBldr.KeyBuilder().(*array.Int32Builder)
		ib := s.int32Toint32ListBldr.ItemBuilder().(*array.ListBuilder)
		ch := ib.ValueBuilder().(*array.Int32Builder)

		for key, val := range v {
			kb.Append(key)
			ib.Append(true)
			for _, c := range val {
				ch.Append(c)
			}
		}
	}
}

func SqlInfoResultMap() flightsql.SqlInfoResultMap {
	return flightsql.SqlInfoResultMap{
		uint32(flightsql.SqlInfoFlightSqlServerName):         "db_name",
		uint32(flightsql.SqlInfoFlightSqlServerVersion):      "duckdb 1.4.1",
		uint32(flightsql.SqlInfoFlightSqlServerArrowVersion): arrow.PkgVersion,
		uint32(flightsql.SqlInfoFlightSqlServerReadOnly):     false,
		// uint32(flightsql.SqlInfoDDLCatalog):                  false,
		// uint32(flightsql.SqlInfoDDLSchema):                   false,
		// uint32(flightsql.SqlInfoDDLTable):                    true,
		// uint32(flightsql.SqlInfoIdentifierCase):              int64(flightsql.SqlCaseSensitivityCaseInsensitive),
		uint32(flightsql.SqlInfoIdentifierQuoteChar): `"`,
		// uint32(flightsql.SqlInfoQuotedIdentifierCase):        int64(flightsql.SqlCaseSensitivityCaseInsensitive),
		// uint32(flightsql.SqlInfoAllTablesAreASelectable):     true,
		// uint32(flightsql.SqlInfoNullOrdering):                int64(flightsql.SqlNullOrderingSortAtStart),
		// uint32(flightsql.SqlInfoFlightSqlServerTransaction):  int32(flightsql.SqlTransactionTransaction),
		// uint32(flightsql.SqlInfoTransactionsSupported):       true,
		// uint32(flightsql.SqlInfoKeywords): []string{"ABORT",
		// 	"ACTION",
		// 	"ADD",
		// 	"AFTER",
		// 	"ALL",
		// 	"ALTER",
		// 	"ALWAYS",
		// 	"ANALYZE",
		// 	"AND",
		// 	"AS",
		// 	"ASC",
		// 	"ATTACH",
		// 	"AUTOINCREMENT",
		// 	"BEFORE",
		// 	"BEGIN",
		// 	"BETWEEN",
		// 	"BY",
		// 	"CASCADE",
		// 	"CASE",
		// 	"CAST",
		// 	"CHECK",
		// 	"COLLATE",
		// 	"COLUMN",
		// 	"COMMIT",
		// 	"CONFLICT",
		// 	"CONSTRAINT",
		// 	"CREATE",
		// 	"CROSS",
		// 	"CURRENT",
		// 	"CURRENT_DATE",
		// 	"CURRENT_TIME",
		// 	"CURRENT_TIMESTAMP",
		// 	"DATABASE",
		// 	"DEFAULT",
		// 	"DEFERRABLE",
		// 	"DEFERRED",
		// 	"DELETE",
		// 	"DESC",
		// 	"DETACH",
		// 	"DISTINCT",
		// 	"DO",
		// 	"DROP",
		// 	"EACH",
		// 	"ELSE",
		// 	"END",
		// 	"ESCAPE",
		// 	"EXCEPT",
		// 	"EXCLUDE",
		// 	"EXCLUSIVE",
		// 	"EXISTS",
		// 	"EXPLAIN",
		// 	"FAIL",
		// 	"FILTER",
		// 	"FIRST",
		// 	"FOLLOWING",
		// 	"FOR",
		// 	"FOREIGN",
		// 	"FROM",
		// 	"FULL",
		// 	"GENERATED",
		// 	"GLOB",
		// 	"GROUP",
		// 	"GROUPS",
		// 	"HAVING",
		// 	"IF",
		// 	"IGNORE",
		// 	"IMMEDIATE",
		// 	"IN",
		// 	"INDEX",
		// 	"INDEXED",
		// 	"INITIALLY",
		// 	"INNER",
		// 	"INSERT",
		// 	"INSTEAD",
		// 	"INTERSECT",
		// 	"INTO",
		// 	"IS",
		// 	"ISNULL",
		// 	"JOIN",
		// 	"KEY",
		// 	"LAST",
		// 	"LEFT",
		// 	"LIKE",
		// 	"LIMIT",
		// 	"MATCH",
		// 	"MATERIALIZED",
		// 	"NATURAL",
		// 	"NO",
		// 	"NOT",
		// 	"NOTHING",
		// 	"NOTNULL",
		// 	"NULL",
		// 	"NULLS",
		// 	"OF",
		// 	"OFFSET",
		// 	"ON",
		// 	"OR",
		// 	"ORDER",
		// 	"OTHERS",
		// 	"OUTER",
		// 	"OVER",
		// 	"PARTITION",
		// 	"PLAN",
		// 	"PRAGMA",
		// 	"PRECEDING",
		// 	"PRIMARY",
		// 	"QUERY",
		// 	"RAISE",
		// 	"RANGE",
		// 	"RECURSIVE",
		// 	"REFERENCES",
		// 	"REGEXP",
		// 	"REINDEX",
		// 	"RELEASE",
		// 	"RENAME",
		// 	"REPLACE",
		// 	"RESTRICT",
		// 	"RETURNING",
		// 	"RIGHT",
		// 	"ROLLBACK",
		// 	"ROW",
		// 	"ROWS",
		// 	"SAVEPOINT",
		// 	"SELECT",
		// 	"SET",
		// 	"TABLE",
		// 	"TEMP",
		// 	"TEMPORARY",
		// 	"THEN",
		// 	"TIES",
		// 	"TO",
		// 	"TRANSACTION",
		// 	"TRIGGER",
		// 	"UNBOUNDED",
		// 	"UNION",
		// 	"UNIQUE",
		// 	"UPDATE",
		// 	"USING",
		// 	"VACUUM",
		// 	"VALUES",
		// 	"VIEW",
		// 	"VIRTUAL",
		// 	"WHEN",
		// 	"WHERE",
		// 	"WINDOW",
		// 	"WITH",
		// 	"WITHOUT"},
		// uint32(flightsql.SqlInfoNumericFunctions): []string{
		// 	"ACOS", "ACOSH", "ASIN", "ASINH", "ATAN", "ATAN2", "ATANH", "CEIL",
		// 	"CEILING", "COS", "COSH", "DEGREES", "EXP", "FLOOR", "LN", "LOG",
		// 	"LOG10", "LOG2", "MOD", "PI", "POW", "POWER", "RADIANS",
		// 	"SIN", "SINH", "SQRT", "TAN", "TANH", "TRUNC"},
		// uint32(flightsql.SqlInfoStringFunctions): []string{"SUBSTR", "TRIM", "LTRIM", "RTRIM", "LENGTH",
		// 	"REPLACE", "UPPER", "LOWER", "INSTR"},
		// uint32(flightsql.SqlInfoSupportsConvert): map[int32][]int32{
		// 	int32(flightsql.SqlConvertBigInt): {int32(flightsql.SqlConvertInteger)},
		// },
	}
}
