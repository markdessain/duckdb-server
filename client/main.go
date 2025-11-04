package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver"
)

func main() {

	config := driver.DriverConfig{
		Address:  "localhost:32010",
		Username: "",
		Password: "",
		Timeout:  10 * time.Second,
		Params:   map[string]string{},
	}

	conn, err := sql.Open("flightsql", config.DSN())

	if err != nil {
		fmt.Println(err)
	}

	rows, err := conn.Query("SELECT extension_name FROM duckdb_extensions()")

	if err != nil {
		fmt.Println(err)
	}

	for rows.Next() {

		var name string
		err = rows.Scan(&name)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(name)
	}

}
