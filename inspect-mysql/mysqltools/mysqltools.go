// Copyright (c) 2014 Square, Inc
//
// Must download driver for mysql use. Run the following command:
//      go get github.com/go-sql-driver/mysql
// in order to successfully build/install

package mysqltools

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"code.google.com/p/goconf/conf" // used for parsing config files
)

import "database/sql"
import _ "github.com/go-sql-driver/mysql"

type MysqlDB struct {
	db        *sql.DB
	dsnString string
}

const (
	DEFAULT_MYSQL_USER = "root"
	MAX_RETRIES        = 5
)

type Configuration struct {
	password []string
}

type Config struct {
	Client struct {
		Password string
	}
}

type InnodbStats struct {
	FileIO           map[string]string
	Log              map[string]string
	BufferPoolAndMem map[string]string
	Transactions     map[string]string
	Metrics          map[string]string
}

//wrapper for make_query, where if there is an error querying the database
// retry connecting to the db and make the query
func (database *MysqlDB) queryDb(query string) ([]string, [][]string, error) {
	var err error
	for attempts := 0; attempts <= MAX_RETRIES; attempts++ {
		err = database.db.Ping()
		if err == nil {
			if cols, data, err := database.makeQuery(query); err == nil {
				return cols, data, nil
			} else {
				fmt.Println("query error: ")
				fmt.Println(err)
				return nil, nil, err
			}
		}
		database.db.Close()
		database.db, err = sql.Open("mysql", database.dsnString)
	}
	return nil, nil, err
}

//makes a query to the database
// returns array of column names and arrays of data stored as string
// string equivalent to []byte
// data stored as 2d array with each subarray containing a single column's data
func (database *MysqlDB) makeQuery(query string) ([]string, [][]string, error) {
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
func (database *MysqlDB) QueryReturnColumnDict(query string) (map[string][]string, error) {
	column_names, values, err := database.queryDb(query)
	result := make(map[string][]string)
	for i, col := range column_names {
		result[col] = values[i]
	}
	return result, err
}

//return values of query in a mapping of first columns entry -> row
func (database *MysqlDB) QueryMapFirstColumnToRow(query string) (map[string][]string, error) {
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

//makes dsn to open up connection
//dsn is made up of the format:
//     [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
func makeDsn(dsn map[string]string) string {
	var dsnString string
	user, ok := dsn["user"]
	if ok {
		dsnString = user
	}
	password, ok := dsn["password"]
	if ok {
		dsnString = dsnString + ":" + password
	}
	dsnString = dsnString + "@"
	dsnString = dsnString + dsn["unix_socket"]
	dsnString = dsnString + "/" + dsn["db"]
	return dsnString
}

func New(user, password, config string) (*MysqlDB, error) {
	fmt.Println("connecting to database")
	database := new(MysqlDB)
	// build dsn info here
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
	//	socket_file := "/var/lib/mysql/mysql.sock"
	//	if _, err := os.Stat(socket_file); err == nil {
	//		dsn["unix_socket"] = socket_file
	//	}

	//Parse ini file to get password

	ini_file := creds[user]
	if config != "" {
		ini_file = config
	}
	_, err := os.Stat(ini_file)
	if err != nil {
		fmt.Println(err)
		return nil, errors.New("'" + ini_file + "' does not exist")
	}
	// read ini file to get password
	c, err := conf.ReadConfigFile(ini_file)
	if err != nil {
		return nil, err
	}
	pw, err := c.GetString("client", "password")
	dsn["password"] = strings.Trim(pw, "\"")
	database.dsnString = makeDsn(dsn)

	db, err := sql.Open("mysql", database.dsnString)
	if err != nil {
		return nil, err
	}
	database.db = db

	err = database.db.Ping()
	if err != nil {
		return nil, err
	}
	return database, nil
}

func (database *MysqlDB) Close() {
	database.db.Close()
}

func ParseInnodbStats(blob string) (*InnodbStats, error) {
	idb := new(InnodbStats)
	idb.Metrics = make(map[string]string)
	//	idb.Mets = make(map[string]string)
	//	idb.Mets = make(map[string]string)
	//	idb.Mets = make(map[string]string)

	chunks := regexp.MustCompile("\n[-=]{3,80}\n").Split(blob, -1)
	for i, chunk := range chunks {
		m := regexp.MustCompile("([/ A-Z])\\s*$").MatchString(chunk)
		if m {
			chunk = strings.Trim(chunk, " \n")
			if m, _ := regexp.MatchString("FILE I/O", chunk); m {
				idb.parseFileIO(chunks[i+1])
				//	fmt.Println(idb.Metrics)
			} else if chunk == "LOG" {
				idb.parseLog(chunks[i+1])
				//	fmt.Println(idb.Metrics)
			} else if chunk == "BUFFER POOL AND MEMORY" {
				idb.parseBufferPoolAndMem(chunks[i+1])
				//	fmt.Println(idb.Metrics)
			} else if chunk == "TRANSACTIONS" {
				idb.parseTransactions(chunks[i+1])
				//	fmt.Println(idb.Metrics)
			}
		}
	}
	return idb, nil
}

//parse the File I/O section of the "show engine innodb status;" command
//stores expressions of the form:     123.456 metric_name
func (idb *InnodbStats) parseFileIO(blob string) {
	lines := strings.Split(blob, "\n")
	for _, line := range lines {
		if strings.Contains(line, ",") {
			elements := strings.Split(line, ",")
			for _, element := range elements {
				element = strings.Trim(element, " \n")
				m := regexp.MustCompile("^(\\d+(\\.\\d+)?) ([A-Za-z/ ]+)\\s*$").MatchString(element)
				if m {
					s := strings.Split(strings.Trim(element, " \n"), " ")
					key := strings.Replace(strings.Join(s[1:], "_"), "/", "_per_", -1)
					idb.Metrics[key] = s[0]
				}
			}
		}
	}
}

//parse the log section of the "show engine innodb status;" command
func (idb *InnodbStats) parseLog(blob string) {
	lines := strings.Split(blob, "\n")
	for _, line := range lines {
		line := strings.Trim(line, " \n")
		if regexp.MustCompile("^(.+?)\\s+(\\d+)\\s*$").MatchString(line) {
			elements := strings.Split(line, " ")
			c := len(elements)
			val := elements[c-1]
			key := strings.Trim(strings.ToLower(strings.Join(elements[:c-1], "_")), "_")
			idb.Metrics[key] = val
		} else {
			elements := strings.Split(line, ",")
			for _, element := range elements {
				element = strings.Trim(element, " \n\t\r\f")
				if regexp.MustCompile("(\\d+) ([A-Za-z/ ,']+)\\s*$").MatchString(element) {
					element = strings.Replace(strings.Replace(element, "i/o's", "io", -1), "/second", "_per_sec", -1)
					words := strings.Split(element, " ")
					key := strings.Trim(strings.ToLower(strings.Join(words[1:], "_")), "_")
					idb.Metrics[key] = words[0]
				}
			}
		}
	}
}

func (idb *InnodbStats) parseBufferPoolAndMem(blob string) {
	lines := strings.Split(blob, "\n")
	matches := []string{"Page hash", "Dictionary cache", "File system", "Lock system", "Recovery system",
		"Dictionary memory allocated", "Buffer pool size", "Free buffers", "Database pages", "Old database pages",
		"Modified db pages", "Pending reads", "Pending writes: LRU", "Pages made young", "Pages read"}
	for _, line := range lines {
		line = strings.Split(strings.Trim(line, " \n"), ",")[0]
		//so many regular expressions. just gonna hard code some of them
		words := strings.Split(line, " ")
		if m, _ := regexp.MatchString("Total memory allocated by read views \\d+", line); m {
			idb.Metrics["total_mem_by_read_views"] = words[len(words)-1]
		} else if m, _ := regexp.MatchString("Total memory allocated \\d+", line); m {
			line := strings.Split(line, ";")[0]
			words := strings.Split(line, " ")
			idb.Metrics["total_mem"] = words[len(words)-1]
		} else if m, _ := regexp.MatchString("Adaptive hash index", line); m {
			idb.Metrics["adaptive_hash"] = words[3]
		} else {
			for _, match := range matches {
				if m, _ := regexp.MatchString(match, line); m {
					line = strings.Split(line, ",")[0]
					key := strings.Trim(strings.ToLower(strings.Replace(strings.Replace(match, " ", "_", -1), ":", "", -1)), " \n\t\f\r")
					if _, ok := idb.Metrics[key]; ok {
						continue
					}
					key_len := len(strings.Split(key, "_"))
					idb.Metrics[key] = strings.Trim(strings.Split(strings.Join(words[key_len:], ""), "(")[0], " \n\t\f\r")
				} else if m, _ := regexp.MatchString("Buffer pool hit rate", line); m {
					line := strings.Split(line, ",")[0]
					words := strings.Split(line, " ")
					num, _ := strconv.ParseFloat(words[4], 64)
					den, _ := strconv.ParseFloat(words[6], 64)
					idb.Metrics["buffer_pool_hit_rate"] = strconv.FormatFloat(num/den, 'f', -1, 64)
					idb.Metrics["cache_hit_pct"] = strconv.FormatFloat((num/den)*100.0, 'f', -1, 64)
				}
			}
		}

	}
}

func (idb *InnodbStats) parseTransactions(blob string) {
	trxes_not_started := 0
	undo := 0
	lines := strings.Split(blob, "\n")
	for _, line := range lines {
		line = strings.Trim(line, " ")
		if regexp.MustCompile("^(.+?)\\s+(\\d+)\\s*$").MatchString(line) {
			words := strings.Split(line, " ")
			key := strings.ToLower(strings.Join(words[:len(words)-2], "_"))
			idb.Metrics[key] = words[len(words)-1]
		} else if m, _ := regexp.MatchString("TRANSACTION (.*) not started", line); m {
			trxes_not_started += 1
		} else if m, _ := regexp.MatchString("ROLLING BACK (\\d+)", line); m {
			tmp, _ := strconv.Atoi(strings.Split(line, " ")[2])
			if tmp > undo {
				undo = tmp
			}
		}
	}
	idb.Metrics["trxes_not_started"] = strconv.Itoa(trxes_not_started)
	idb.Metrics["undo"] = strconv.Itoa(undo)
}
