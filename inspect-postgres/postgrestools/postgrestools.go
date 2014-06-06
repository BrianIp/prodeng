//Copyright (c) 2014 Square, Inc

package postgrestools

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/lib/pq"
)

const (
	MAX_RETRIES = 5
)

type PostgresDB struct {
	db        *sql.DB
	dsnString string
	Logger    *log.Logger
}

//wrapper for make_query, where if there is an error querying the database
// retry connecting to the db and make the query
func (database *PostgresDB) queryDb(query string) ([]string, [][]string, error) {
	var err error
	for attempts := 0; attempts <= MAX_RETRIES; attempts++ {
		err = database.db.Ping()
		if err == nil {
			if cols, data, err := database.makeQuery(query); err == nil {
				return cols, data, nil
			} else {
				return nil, nil, err
			}
		}
		database.db.Close()
		database.db, err = sql.Open("postgres", database.dsnString)
	}
	return nil, nil, err
}

//makes a query to the database
// returns array of column names and arrays of data stored as string
// string equivalent to []byte
// data stored as 2d array with each subarray containing a single column's data
func (database *PostgresDB) makeQuery(query string) ([]string, [][]string, error) {
	rows, err := database.db.Query(query)
	if err != nil {
		return nil, nil, err
	}

	column_names, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	columns := len(column_names)
	values := make([][]string, columns)
	tmp_values := make([]sql.RawBytes, columns)

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &tmp_values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, nil, err
		}
		for i, col := range tmp_values {
			str := string(col)
			values[i] = append(values[i], str)
		}
	}
	err = rows.Err()

	return column_names, values, nil
}

//return values of query in a mapping of column_name -> column
func (database *PostgresDB) QueryReturnColumnDict(query string) (map[string][]string, error) {
	column_names, values, err := database.queryDb(query)
	result := make(map[string][]string)
	for i, col := range column_names {
		result[col] = values[i]
	}
	return result, err
}

//return values of query in a mapping of first columns entry -> row
func (database *PostgresDB) QueryMapFirstColumnToRow(query string) (map[string][]string, error) {
	_, values, err := database.queryDb(query)
	result := make(map[string][]string)
	if len(values) == 0 {
		return nil, nil
	}
	for i, name := range values[0] {
		for j, vals := range values {
			if j != 0 {
				result[string(name)] = append(result[string(name)], vals[i])
			}
		}
	}
	return result, err
}

//New creates connection to postgres database
func New(dsn map[string]string) (*PostgresDB, error) {
	dsnString := makeDsn(dsn)

	pgdb := new(PostgresDB)
	pgdb.Logger = log.New(os.Stderr, "LOG: ", log.Lshortfile)
	db, err := sql.Open("postgres", dsnString)
	if err != nil {
		return pgdb, err
	}
	err = db.Ping()
	if err != nil {
		return pgdb, err
	}
	pgdb.db = db

	return pgdb, nil
}

func makeDsn(dsn map[string]string) string {
	var dsnString string
	for key, value := range dsn {
		dsnString += " " + key + "=" + value
	}
	return dsnString
}

func (database *PostgresDB) Close() {
	database.db.Close()
}
