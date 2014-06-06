package postgresstat

import (
	"errors"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/square/prodeng/inspect-postgres/postgrestools"
	"github.com/square/prodeng/inspect/misc"
	"github.com/square/prodeng/metrics"
)

type PostgresStat struct {
	Metrics  *PostgresStatMetrics
	m        *metrics.MetricContext
	db       *postgrestools.PostgresDB
	idleCol  string
	idleStr  string
	queryCol string
	pidCol   string
}

type PostgresStatMetrics struct {
	Uptime               *metrics.Counter
	Version              *metrics.Gauge
	TPS                  *metrics.Counter
	BlockReadsDisk       *metrics.Counter
	BlockReadsCache      *metrics.Counter
	CacheHitPct          *metrics.Gauge
	CommitRatio          *metrics.Gauge
	WalKeepSegments      *metrics.Gauge
	SessionMax           *metrics.Gauge
	SessionCurrentTotal  *metrics.Gauge
	SessionBusyPct       *metrics.Gauge
	ConnMaxPct           *metrics.Gauge
	OldestTrxS           *metrics.Gauge
	OldestQueryS         *metrics.Gauge
	ActiveLongRunQueries *metrics.Gauge
	LockWaiters          *metrics.Gauge
	CpuPct               *metrics.Gauge
	MemPct               *metrics.Gauge
	VSZ                  *metrics.Gauge
	RSS                  *metrics.Gauge
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
	return nil
}

func (s *PostgresStat) getOldest() error {
	info := map[string]*metrics.Gauge{"xact_start": s.Metrics.OldestTrxS, "query_start": s.Metrics.OldestQueryS}
	for col, metric := range info {
		cmd := fmt.Sprintf(`
SELECT EXTRACT(epoch FROM NOW()) - EXTRACT(epoch FROM %s) 
  FROM pg_stat_activity 
 WHERE %s != '%s'
   AND UPPER(%s) NOT LIKE '%%VACUUM%%'
 ORDER BY 1 DESC LIMIT 1;`, col, s.idleCol, s.idleStr, s.queryCol)
		res, err := s.db.QueryReturnColumnDict(cmd)
		if err != nil {
			return err
		}
		for _, v := range res {
			if len(v) == 0 {
				return errors.New("cant get oldest")
			}
			val, err := strconv.ParseFloat(v[0], 64)
			if err != nil {
				s.db.Logger.Println(err)
			}
			metric.Set(val)
		}
	}
	return nil
}

func (s *PostgresStat) getNumLongEntries() error {
	threshold := "30"
	cmd := fmt.Sprintf(`SELECT * FROM pg_stat_activity 
 WHERE EXTRACT(epoch FROM NOW()) - EXTRACT(epoch FROM query_start) > %s
   AND %s != '%s';`, threshold, s.idleCol, s.idleStr)
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	for _, col := range res {
		s.Metrics.ActiveLongRunQueries.Set(float64(len(col)))
		return nil
	}
	return nil
}

func (s *PostgresStat) getLocks() error {
	cmd := fmt.Sprintf(`
SELECT bl.pid                 AS blocked_pid,
       a.usename              AS blocked_user,
       ka.%s       AS blocking_statement,
       NOW() - ka.query_start AS blocking_duration,
       kl.pid                 AS blocking_pid,
       ka.usename             AS blocking_user,
       a.%s        AS blocked_statement,
       NOW() - a.query_start  AS blocked_duration
  FROM pg_catalog.pg_locks bl
  JOIN pg_catalog.pg_stat_activity a
    ON a.%s = bl.pid
  JOIN pg_catalog.pg_locks kl 
    ON kl.transactionid = bl.transactionid AND kl.pid != bl.pid
  JOIN pg_catalog.pg_stat_activity ka ON ka.%s = kl.pid
 WHERE NOT bl.granted;`, s.queryCol, s.queryCol, s.pidCol, s.pidCol)
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	for _, col := range res {
		s.Metrics.LockWaiters(float64(len(col)))
		break
	}
	cmd = "SELECT mode, COUNT(*) FROM pg_locks WHERE granted GROUP BY 1;"
	res, err = s.db.QueryReturnColumnDict(cmd)
	//TODO: look into column names here
	return nil
}

/*
func (s *PostgresStat) getVacuumsInProgress() error {
    cmd := s.Sprintf(`
SELECT * FROM pg_stat_activity
 WHERE UPPER(%s) LIKE '%%VACUUM%%';`, s.querCol)
    res, err := s.db.QueryReturnColumnDict(cmd)
    if err != nil {
      return err
    }
    auto := 0
    manual := 0
    for i, val := res[s.queryCol] {
        if strings.Contains(val, "datfrozenxid") {
          continue
        }
        m, err := regexp.MustCompile("(?i)(\s*autovacuum:\s*)?(\s*VACUUM\s*)?(\s*ANALYZE\s*)?\s*(.+?)$").
    }
    //TODO: wtf this is so long
}
*/
func (s *PostgresStat) getMainProcesInfo() error {
	out, err := exec.Commmand("ps", "aux").Output()
	if err != nil {
		return err
	}
	blob := string(out)
	lines := strings.Split(blob, "\n")
	info := make([][]string, 10)
	//TODO: if this gets used more than once, make into own function
	//mapping for info: 0-user, 1-pid, 2-cpu, 3-mem, 4-vsz, 5-rss, 6-tty, 7-stat, 8-start, 9-time, 10-cmd

	for _, line := range lines {
		line = strings.Trim(line, " ")

		if len(words) < 10 {
			continue
		}
		cmd := strings.Join(words[10:], " ")

		if strings.Contains(cmd, "postmaster") {
			words := strings.Split(line, " ")

			for i, word := range words {
				info[i] = append(info[i], word)
			}
			cpu, _ := strconv.ParseFloat(info[2], 64) //TODO: correctly handle these errors
			mem, _ := strconv.ParseFloat(info[3], 64)
			vsz, _ := strconv.ParseFloat(info[4], 64)
			rss, _ := strconv.ParseFloat(info[5], 64)
			s.Metrics.CpuPct.Set(cpu)
			s.Metrics.MemPct.Set(mem)
			s.Metrics.VSZ.Set(vsz)
			s.Metrics.RSS.Set(rss)
		}
	}
	return nil
}

func (s *PostgresStat) getWriteability() error {
	return nil
}

func (s *PostgresStat) getSecurity() error {
	cmd := "SELECT usename FROM pg_shadow WHERE passwd IS NULL;"
	res, err := s.db.QueryReturnColumnRows(cmd)
	if err != nil {
		return err
	}
	if len(res) > 0 {
		s.Metrics.UnsecureUsers.Set(uint64(len(res)))
	}
	return nil
}
