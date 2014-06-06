package postgresstat

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/square/prodeng/inspect-postgres/postgrestools"
	"github.com/square/prodeng/inspect/misc"
	"github.com/square/prodeng/metrics"
)

type PostgresStat struct {
	Metrics *PostgresStatMetrics
	m       *metrics.MetricContext
	db      *postgrestools.PostgresDB
	idleCol string
	idleStr string
}

type PostgresStatMetrics struct {
	Uptime              *metrics.Counter
	Version             *metrics.Gauge
	TPS                 *metrics.Counter
	BlockReadsDisk      *metrics.Counter
	BlockReadsCache     *metrics.Counter
	CacheHitPct         *metrics.Gauge
	CommitRatio         *metrics.Gauge
	WalKeepSegments     *metrics.Gauge
	SessionMax          *metrics.Gauge
	SessionCurrentTotal *metrics.Gauge
	SessionBusyPct      *metrics.Gauge
	ConnMaxPct          *metrics.Gauge
}

func New(m *metrics.MetricContext, Step time.Duration, user, password, config string) (*PostgresStat, error) {
	s := new(PostgresStat)

	var err error
	s.db, err = postgrestools.New()
	if err != nil {
		s.db.Logger.Println(err)
		return nil, err
	}

	s.Metrics = PostgresStatMetricsNew(m, Step)

	s.Collect()

	ticker := time.NewTicker(Step)
	go func() {
		for _ = range ticker.C {
			s.Collect()
		}
	}()
	return s, nil
}

func PostgresStatMetricsNew(m *metrics.MetricContext, Step time.Duration) *PostgresStatMetrics {
	c := new(PostgresStatMetrics)
	misc.InitializeMetrics(c, m, "postgresstat", true)
	return c
}

func (s *PostgresStat) Collect() {
	collects := []error{
		s.getUptime(),
	}
	for _, err := range collects {
		if err != nil {
			s.db.Logger.Println(err)
		}
	}
}

//get uptime
func (s *PostgresStat) getUptime() error {
	cmd := `
  SELECT EXTRACT(epoch FROM now())
       - EXTRACT(epoch From pg_postmaster_start_time());`
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	for _, val := range res {
		if len(val) == 0 {
			return errors.New("Couldn't get uptime")
		}
		time, _ := strconv.ParseInt(val[0], 10, 64)
		s.Metrics.Uptime.Set(uint64(time))
	}
	return nil
}

//get version
//looks like:
//  'PostgreSQL 9.1.5 on x86_64-unknown-linux-gnu....'
func (s *PostgresStat) getVersion() error {
	res, err := s.db.QueryReturnColumnDict("SELECT VERSION();")
	if err != nil || len(res["Version()"]) == 0 {
		return errors.New("Couldn't get version")
	}
	version := res["Version()"][0]
	//filter out letters'
	version = strings.Split(version, " ")[1]
	leading := float64(len(strings.Split(version, ".")[0]))
	version = strings.Replace(version, ".", "", -1)
	ver, err := strconv.ParseFloat(version, 64)
	ver /= math.Pow(10.0, float64(len(version))-leading)
	s.Metrics.Version.Set(ver)
	return err
}

func (s *PostgresStat) getTPS() error {
	//This is only writes? idk
	cmd := "SELECT SUM(xact_commit + xact_rollback) FROM pg_stat_database;"
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	for _, val := range res {
		if len(val) == 0 {
			return errors.New("Unable to get tps")
		}
		v, err := strconv.ParseInt(val[0], 10, 64)
		if err != nil {
			return err
		}
		s.Metrics.TPS.Set(uint64(v))
	}
	return nil
}

func (s *PostgresStat) getCacheInfo() error {
	cmd := "SELECT SUM(blks_read) AS block_reads_disk, SUM(blks_hit) AS block_reads_cache FROM pg_stat_database;"
	res, err := s.db.QueryReturnColumnDict(cmd)
	disk := 0
	cache := 0
	if err != nil {
		return err
	}
	if len(res["block_reads_disk"]) == 0 {
		s.db.Logger.Println("cannot get block_reads_disk")
	} else {
		disk, err := strconv.ParseInt(res["block_reads_disk"][0], 10, 64)
		if err != nil {
			s.db.Logger.Println(err)
		}
		s.Metrics.BlockReadsDisk.Set(uint64(disk))
	}
	if len(res["block_reads_cache"]) == 0 {
		s.db.Logger.Println("cannot get block_reads_cache")
	} else {
		cache, err := strconv.ParseInt(res["block_reads_cache"][0], 10, 64)
		if err != nil {
			s.db.Logger.Println(err)
		}
		s.Metrics.BlockReadsDisk.Set(uint64(cache))
	}
	if disk != 0 && cache != 0 {
		pct := (float64(cache) / float64(cache+disk)) * 100.0
		s.Metrics.CacheHitPct.Set(float64(pct))
	}
	return nil
}

func (s *PostgresStat) getCommitRatio() error {
	cmd := `
SELECT AVG(ROUND((100.0*sd.xact_commit)/(sd.xact_commit+sd.xact_rollback), 2))
  FROM pg_stat_database sd
  JOIN pg_database d ON (d.oid=sd.datid)
  JOIN pg_user u ON (u.usesysid=d.datdba)
 WHERE sd.xact_commit+sd.xact_rollback != 0;`
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	for _, val := range res {
		if len(val) == 0 {
			return errors.New("Can't get commit ratio")
		}
		v, err := strconv.ParseFloat(val[0], 64)
		if err != nil {
			return err
		}
		s.Metrics.CommitRatio.Set(v)
	}
	return nil
}

func (s *PostgresStat) getWalKeepSegments() error {
	res, err := s.db.QueryReturnColumnDict("SELECT setting FROM pg_settings WHERE name = 'wal_keep_segments';")
	if err != nil {
		return err
	}
	v, ok := res["setting"]
	if !ok || len(v) == 0 {
		return errors.New("Can't get WalKeepSegments")
	}
	val, err := strconv.ParseFloat(v[0], 64)
	s.Metrics.WalKeepSegments.Set(float64(val))
	return nil
}

func (s *PostgresStat) getSessions() error {
	res, err := s.db.QueryReturnColumnDict("SELECT setting FROM pg_settings WHERE name = 'max_connections';")
	if err != nil {
		return err
	}
	v, ok := res["setting"]
	if !ok || len(v) == 0 {
		return errors.New("Can't get session max")
	}
	sessMax, err := strconv.ParseInt(v[0], 10, 64)
	if err != nil {
		return err
	}
	s.Metrics.SessionMax.Set(float64(sessMax))

	cmd := fmt.Sprintf(`
SELECT (SELECT COUNT(*) FROM pg_stat_activity 
         WHERE %s = '%s') AS idle,
       (SELECT COUNT(*) FROM pg_stat_activity 
         WHERE %s != '%s') AS active;`, s.idleCol, s.idleStr, s.idleCol, s.idleStr)

	res, err = s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	idle := int64(0)
	v, ok = res["idle"]
	if ok || len(v) > 0 {
		idle, err = strconv.ParseInt(v[0], 10, 64)
		if err != nil {
			s.db.Logger.Print(err)
		}
	}
	active := int64(0)
	v, ok = res["active"]
	if ok || len(v) > 0 {
		active, err = strconv.ParseInt(v[0], 10, 64)
		if err != nil {
			s.db.Logger.Print(err)
		}
	}
	total := float64(active + idle)
	s.Metrics.SessionCurrentTotal.Set(total)
	s.Metrics.SessionBusyPct.Set((float64(active) / total) * 100)
	s.Metrics.ConnMaxPct.Set(float64(total/float64(sessMax)) * 100.0)
	return errors.New("not implemented")
}

func (s *PostgresStat) getOldest() error {
	info := map[string]string{"xact_start": "oldest_trx_s", "query_start": "oldest_query_s"}
	for col, infocol := range info {

	}
}
