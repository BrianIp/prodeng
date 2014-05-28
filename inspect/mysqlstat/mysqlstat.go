// Copyright (c) 2014 Square, Inc
//
// Must download driver for mysql use. Run the following command:
//      go get github.com/go-sql-driver/mysql
// in order to successfully build/install

package mysqlstat

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/square/prodeng/inspect/misc"
	"github.com/square/prodeng/metrics"
)

import "database/sql"
import _ "github.com/go-sql-driver/mysql"

const (
	DEFAULT_MYSQL_USER = "root"
)

type Configuration struct {
	password []string
}

type MysqlStat struct {
	Metrics *MysqlStatMetrics
	m       *metrics.MetricContext
	db      *sql.DB
}

type MysqlStatMetrics struct {
	SecondsBehindMaster    *metrics.Gauge
	SlaveSeqFile           *metrics.Gauge
	SlavePosition          *metrics.Gauge
	ThreadsRunning         *metrics.Gauge
	UptimeSinceFlushStatus *metrics.Gauge
	OpenTables             *metrics.Gauge
}

//makes a query to the database
// returns array of column names and arrays of data stored in each column
func (s *MysqlStat) make_query(query string) ([]string, [][]sql.RawBytes, error) {
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, nil, err
	}

	column_names, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	columns := len(column_names)
	values := make([][]sql.RawBytes, columns)
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
			values[i] = append(values[i], col)
		}
	}
	err = rows.Err()

	return column_names, values, err
}

//return value of columns in a mapping of column_name -> column
func (s *MysqlStat) query_return_columns_dict(query string) (map[string][]sql.RawBytes, error) {
	column_names, values, err := s.make_query(query)
	result := make(map[string][]sql.RawBytes)
	for i, col := range column_names {
		result[col] = values[i]
	}
	return result, err
}

func (s *MysqlStat) query_map_first_column_to_row(query string) (map[string][]sql.RawBytes, error) {
	_, values, err := s.make_query(query)
	result := make(map[string][]sql.RawBytes)
	for i, name := range values[0] {
		for j, vals := range values {
			if j != 0 {
				result[string(name)] = append(result[string(name)], vals[i])
			}
		}
	}
	return result, err
}

func New(m *metrics.MetricContext, Step time.Duration, user string, password string) (*MysqlStat, error) {
	fmt.Println("starting")
	s := new(MysqlStat)
	// connect to database
	err := s.connect(user, password)
	if err != nil { //error in connecting to database
		return nil, err
	}
	s.Metrics = MysqlStatMetricsNew(m, Step)

	err = s.db.Ping()
	if err != nil {
		fmt.Println("Cant ping database")
		fmt.Println(err)
	}

	defer s.db.Close()
	fmt.Println("starting collect")
	s.Collect()

	ticker := time.NewTicker(Step)
	//go func() {
	func() {
		for _ = range ticker.C {
			s.Collect()
		}
	}()
	return s, nil
}

func MysqlStatMetricsNew(m *metrics.MetricContext, Step time.Duration) *MysqlStatMetrics {
	//fmt.Println("starting")
	c := new(MysqlStatMetrics)
	misc.InitializeMetrics(c, m, "mysqlstat", true)
	return c
}

func make_dsn(dsn map[string]string) string {
	var dsn_string string
	user, ok := dsn["user"]
	if ok {
		dsn_string = user
	}
	password, ok := dsn["password"]
	if ok {
		dsn_string = dsn_string + ":" + password
	}
	dsn_string = dsn_string + "@"
	dsn_string = dsn_string + dsn["unix_socket"]
	dsn_string = dsn_string + "/" + dsn["db"]
	fmt.Println("dsn string: " + dsn_string)
	return dsn_string
}

func (s *MysqlStat) connect(user, password string) error {
	dsn := map[string]string{"db": "information_schema"}
	creds := map[string]string{"root": "/root/.my.cnf", "nrpe": "/etc/my_nrpe.cnf"}
	if user == "" {
		user = DEFAULT_MYSQL_USER
		dsn["user"] = DEFAULT_MYSQL_USER
	} else {
		dsn["user"] = user
	}
	if password != "" {
		dsn["password"] = password
	}
	socket_file := "/var/lib/mysql/mysql.sock"
	if _, err := os.Stat(socket_file); err == nil {
		dsn["unix_socket"] = socket_file
	}
	db, err := sql.Open("mysql", make_dsn(dsn))
	if err == nil {
		fmt.Println("opened database without password")
		s.db = db
		return nil
	}
	ini_file := creds[user]
	if _, err := os.Stat(ini_file); err != nil {
		return errors.New("'" + ini_file + "' does not exist")
	}
	// read ini file to get password
	file, _ := os.Open(ini_file)
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err = decoder.Decode(&configuration)
	dsn["password"] = configuration.password[0]
	db, err = sql.Open("mysql", make_dsn(dsn))
	if err == nil {
		s.db = db
		return nil
	}
	return err
}

func (s *MysqlStat) Collect() {
	s.get_slave_stats()
	s.get_global_status()
}

// get_slave_stats gets slave statistics
func (s *MysqlStat) get_slave_stats() error {
	res, _ := s.query_return_columns_dict("SHOW SLAVE STATUS;")
	//	fmt.Println("Result when querying 'SHOW SLAVE STATUS;'")
	//	fmt.Println(res)

	if len(res["Seconds_Behind_Master"]) > 0 {
		seconds_behind_master, _ := strconv.Atoi(string(res["Seconds_Behind_Master"][0]))
		s.Metrics.SecondsBehindMaster.Set(float64(seconds_behind_master))
	} else {
		fmt.Println("no seconds behind master data")
	}

	relay_master_log_file, _ := res["Relay_Master_Log_File"]
	if len(relay_master_log_file) > 0 {
		slave_seqfile, err := strconv.Atoi(strings.Split(string(relay_master_log_file[0]), ".")[1])
		s.Metrics.SlaveSeqFile.Set(float64(slave_seqfile))

		if err != nil {
			return err
		}
	} else {
		fmt.Println("no relay master log file")
	}

	if len(res["Exec_Master_Log_Pos"]) > 0 {
		slave_position, err := strconv.Atoi(string(res["Exec_Master_Log_Pos"][0]))
		if err != nil {
			return err
		}
		s.Metrics.SlavePosition.Set(float64(slave_position))
	} else {
		fmt.Println("no Exec_Master_Log_Pos")
	}
	return nil
}

func (s *MysqlStat) get_global_status() error {
	res, _ := s.query_map_first_column_to_row("SHOW GLOBAL STATUS;")
	//    fmt.Println("Result when querying 'SHOW GLOBAL STATUS;")
	//    fmt.Println(res)
	v, ok := res["Threads_running"]
	if ok && len(v) > 0 {
		fmt.Println("Threads_running: " + string(v[0]))
		threads_running, _ := strconv.Atoi(string(v[0]))
		s.Metrics.ThreadsRunning.Set(float64(threads_running))
	} else {
		fmt.Println("cannot find Threads_running")
	}
	v, ok = res["Uptime_since_flush_status"]
	if ok && len(v) > 0 {
		fmt.Println("Uptime_since_flush_status: " + string(v[0]))
		uptime, _ := strconv.Atoi(string(v[0]))
		s.Metrics.UptimeSinceFlushStatus.Set(float64(uptime))
	} else {
		fmt.Println("cannot find Uptime_since_flush_status")
	}
	v, ok = res["Open_tables"]
	if ok && len(v) > 0 {
		fmt.Println("Open_tables: " + string(v[0]))
		com, _ := strconv.Atoi(string(v[0]))
		s.Metrics.OpenTables.Set(float64(com))
	} else {
		fmt.Println("cannot find Open_tables")
	}

	return nil
}
