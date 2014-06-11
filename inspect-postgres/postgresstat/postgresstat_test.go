package postgresstat

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/square/prodeng/metrics"
)

type testPostgresDB struct {
	Logger   *log.Logger
	pidCol   string
	queryCol string
	idleCol  string
	idleStr  string
}

var (
	//testquerycol and testqueryrow map a query string to the desired test result
	//simulates QueryReturnColumnDict()
	testquerycol = map[string]map[string][]string{}

	//Simulates QueryMapFirstColumnToRow
	testqueryrow = map[string]map[string][]string{}

	//Mapping of metric and its expected value
	// defined as map of interface{}->interface{} so
	// can switch between metrics.Gauge and metrics.Counter
	// and between float64 and uint64 easily
	expectedValues = map[interface{}]interface{}{}
)

//functions that behave like mysqltools but we can make it return whatever
func (s *testPostgresDB) QueryReturnColumnDict(query string) (map[string][]string, error) {
	return testquerycol[query], nil
}

func (s *testPostgresDB) QueryMapFirstColumnToRow(query string) (map[string][]string, error) {
	return testquerycol[query], nil
}

func (s *testPostgresDB) Log(in interface{}) {
	s.Logger.Println(in)
}

func (s *testPostgresDB) Close() {
	return
}

func initPostgresStat() *PostgresStat {
	s := new(PostgresStat)
	s.db = &testPostgresDB{
		Logger: log.New(os.Stderr, "TESTING LOG: ", log.Lshortfile),
	}
	s.Metrics = PostgresStatMetricsNew(metrics.NewMetricContext("system"),
		time.Millisecond*time.Duration(1)*1000)
	s.pidCol = "procpid"
	s.queryCol = "current_query"
	s.idleCol = s.queryCol
	s.idleStr = "<IDLE>"

	return s
}

//checks ressults between expected and actual metrics gathered
func checkResults() string {
	for metric, expected := range expectedValues {
		switch m := metric.(type) {
		case *metrics.Counter:
			val, ok := expected.(uint64)
			if !ok {
				return "unexpected type"
			}
			if m.Get() != val {
				return ("unexpected value - got: " +
					strconv.FormatInt(int64(m.Get()), 10) + " but wanted " + strconv.FormatInt(int64(val), 10))
			}
		case *metrics.Gauge:
			val, ok := expected.(float64)
			if !ok {
				return "unexpected type"
			}
			if m.Get() != val {
				return ("unexpected value - got: " +
					strconv.FormatFloat(float64(m.Get()), 'f', 5, 64) + " but wanted " +
					strconv.FormatFloat(float64(val), 'f', 5, 64))
			}
		}
	}
	return ""
}

func TestBasic(t *testing.T) {
	fmt.Println("Basic Test")
	//initialize PostgresStat
	s := initPostgresStat()
	//set desired test output
	testquerycol = map[string]map[string][]string{
		uptimeQuery: map[string][]string{
			"uptime": []string{"15110"},
		},
		versionQuery: map[string][]string{
			"version": []string{"PostgreSQL 9.1.5 x86_64-linuc-gnu"},
		},
		tpsQuery: map[string][]string{
			"tps": []string{"15122"},
		},
		cacheInfoQuery: map[string][]string{
			"block_reads_disk":  []string{"4"},
			"block_reads_cache": []string{"6"},
		},
		commitRatioQuery: map[string][]string{
			"commit_ratio": []string{"0.5"},
		},
		walKeepSegmentsQuery: map[string][]string{
			"setting": []string{"15.210"},
		},
		sessionMaxQuery: map[string][]string{
			"setting": []string{"200"},
		},
		fmt.Sprintf(sessionQuery, s.idleCol, s.idleStr, s.idleCol, s.idleStr): map[string][]string{
			"idle":   []string{"40"},
			"active": []string{"60"},
		},
		fmt.Sprintf(oldestQuery, "xact_start", s.idleCol, s.idleStr, s.queryCol): map[string][]string{
			"oldest": []string{"15251"},
		},
		fmt.Sprintf(oldestQuery, "query_start", s.idleCol, s.idleStr, s.queryCol): map[string][]string{
			"oldest": []string{"15112"},
		},
		fmt.Sprintf(longEntriesQuery, "30", s.idleCol, s.idleStr): map[string][]string{
			"entries": []string{"1", "2", "3", "4", "5", "6", "7"},
		},
		fmt.Sprintf(lockWaitersQuery, s.queryCol, s.queryCol, s.pidCol, s.pidCol): map[string][]string{
			"waiters": []string{"1", "2", "3", "4", "5", "6", "7", "8"},
		},
		locksQuery: map[string][]string{
			"lock1": []string{"15213"},
			"lock2": []string{"15322"},
			"lock3": []strimg{"15396"},
		},
	}
	s.Collect()
	time.Sleep(time.Millisecond * 1000 * 1)
	expectedValues = map[interface{}]interface{}{
		s.Metrics.Uptime:               uint64(15110),
		s.Metrics.Version:              float64(9.15),
		s.Metrics.TPS:                  uint64(15122),
		s.Metrics.BlockReadsDisk:       uint64(1),
		s.Metrics.BlockReadsCache:      uint64(2),
		s.Metrics.CacheHitPct:          float64(60),
		s.Metrics.CommitRatio:          float64(0.5),
		s.Metrics.WalKeepSegments:      float64(15.210),
		s.Metrics.SessionMax:           float64(200),
		s.Metrics.SessionCurrentTotal:  float64(100),
		s.Metrics.SessionBusyPct:       float64(60),
		s.Metrics.ConnMaxPct:           float64(50),
		s.Metrics.OldestTrxS:           float64(15251),
		s.Metrics.OldestQueryS:         float64(15112),
		s.Metrics.ActiveLongRunQueries: float64(7),
		s.Metrics.LockWaiters:          float64(8),
		s.Modes["lock1"].Locks:         float64(15213),
		s.Modes["lock2"].Locks:         float64(15322),
		s.Modes["lock3"].Locks:         float64(15396),
		//TODO: get vacuums in progress

	}
	err := checkResults()
	if err != "" {
		t.Error(err)
	}
	fmt.Println("PASS")
}
