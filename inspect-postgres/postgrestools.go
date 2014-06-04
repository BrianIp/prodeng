//Copyright (c) 2014 Square, Inc

package postgrestools

import (
	"database/sql"

	_ "github.com/lib/sql"
)

type PostgresDB struct {
	db        *sql.DB
	dsnString string
}

//New creates connection to postgres database
func New() (*PostgresDB, error) {
	dsn := map[string]string{"dbname": "postgres", "user": "postgres"}
	dsnString := makeDsn(dsn)

	pgdb := new(PostgresDB)
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
