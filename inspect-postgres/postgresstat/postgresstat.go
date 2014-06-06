package postgresstat

import (
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
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
	DBs      map[string]*DBMetrics
	Modes    map[string]*ModeMetrics
	idleCol  string
	idleStr  string
	queryCol string
	pidCol   string
	PGDATA   string
	dsn      map[string]string
}

type ModeMetrics struct {
	Locks *metrics.Gauge
}

type DBMetrics struct {
	Tables    map[string]*TableMetrics
	SizeBytes *metrics.Gauge
}

type TableMetrics struct {
	SizeBytes *metrics.Gauge
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
	UnsecureUsers        *metrics.Gauge
	Writable             *metrics.Gauge //0 if not writable, 1 if writable
	BackupsRunning       *metrics.Gauge
	BinlogFiles          *metrics.Gauge
	DBSizeBinlogs        *metrics.Gauge
	SecondsBehindMaster  *metrics.Gauge
	SlavesConnectedToMe  *metrics.Gauge
	VacuumsAutoRunning   *metrics.Gauge
	VacuumsManualRunning *metrics.Gauge
	SlaveBytesBehindMe   *metrics.Gauge
}

func New(m *metrics.MetricContext, Step time.Duration, user, config string) (*PostgresStat, error) {
	s := new(PostgresStat)

	dsn := map[string]string{"dbname": "postgres", "user": "postgres", "sslmode": "disable"}
	var err error
	s.db, err = postgrestools.New(dsn)
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

//checks for initialization of db metrics
func (s *PostgresStat) checkDB(dbname string) error {
	if _, ok := s.DBs[dbname]; !ok {
		o := new(DBMetrics)
		o.Tables = make(map[string]*TableMetrics)
		misc.InitializeMetrics(o, s.m, "postgresstat."+dbname, true)
		s.DBs[dbname] = o
	}
	return nil
}

func (s *PostgresStat) checkMode(name string) error {
	if _, ok := s.Modes[name]; !ok {
		o := new(ModeMetrics)
		misc.InitializeMetrics(o, s.m, "postgresstat.lock"+name, true)
		s.Modes[name] = o
	}
	return nil
}

func (s *PostgresStat) checkTable(dbname, tblname string) error {
	s.checkDB(dbname)
	if _, ok := s.DBs[dbname].Tables[tblname]; !ok {
		o := new(TableMetrics)
		misc.InitializeMetrics(o, s.m, "postgresstat."+dbname+"."+tblname, true)
		s.DBs[dbname].Tables[tblname] = o
	}
	return nil
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
       - EXTRACT(epoch From pg_postmaster_start_time()) AS uptime;`
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	v, ok := res["uptime"]
	if !ok || len(v) == 0 {
		return errors.New("Couldn't get uptime")
	}
	time, err := strconv.ParseInt(v[0], 10, 64)
	s.Metrics.Uptime.Set(uint64(time))
	return err
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
	if ver >= 9.02 {
		s.pidCol = "pid"
		s.queryCol = "query"
		s.idleCol = "state"
		s.idleStr = "idle"
	} else {
		s.pidCol = "procpid"
		s.queryCol = "current_query"
		s.idleCol = s.queryCol
		s.idleStr = "<IDLE>"
	}
	return err
}

func (s *PostgresStat) getTPS() error {
	//This is only writes? idk
	cmd := "SELECT SUM(xact_commit + xact_rollback) AS tps FROM pg_stat_database;"
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	v, ok := res["tps"]
	if !ok || len(v) == 0 {
		return errors.New("Unable to get tps")
	}
	val, err := strconv.ParseInt(v[0], 10, 64)
	s.Metrics.TPS.Set(uint64(val))
	return err
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
	return err
}

func (s *PostgresStat) getCommitRatio() error {
	cmd := `
SELECT AVG(ROUND((100.0*sd.xact_commit)/(sd.xact_commit+sd.xact_rollback), 2)) AS commit_ratio
  FROM pg_stat_database sd
  JOIN pg_database d ON (d.oid=sd.datid)
  JOIN pg_user u ON (u.usesysid=d.datdba)
 WHERE sd.xact_commit+sd.xact_rollback != 0;`
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	v, ok := res["tps"]
	if !ok || len(v) == 0 {
		return errors.New("Can't get commit ratio")
	}
	val, err := strconv.ParseFloat(v[0], 64)
	s.Metrics.CommitRatio.Set(val)
	return err
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
	return err
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
		s.db.Logger.Println(err)
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
SELECT EXTRACT(epoch FROM NOW()) - EXTRACT(epoch FROM %s) AS oldest
  FROM pg_stat_activity 
 WHERE %s != '%s'
   AND UPPER(%s) NOT LIKE '%%VACUUM%%'
 ORDER BY 1 DESC LIMIT 1;`, col, s.idleCol, s.idleStr, s.queryCol)
		res, err := s.db.QueryReturnColumnDict(cmd)
		if err != nil {
			return err
		}
		v, ok := res["oldest"]
		if !ok || len(v) == 0 {
			return errors.New("cant get oldest")
		}
		val, err := strconv.ParseFloat(v[0], 64)
		if err != nil {
			s.db.Logger.Println(err)
		}
		metric.Set(val)
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
		s.Metrics.LockWaiters.Set(float64(len(col)))
		break
	}
	cmd = "SELECT mode, COUNT(*) AS count FROM pg_locks WHERE granted GROUP BY 1;"
	res, err = s.db.QueryMapFirstColumnToRow(cmd)
	for mode, locks := range res {
		lock, _ := strconv.ParseInt(locks[0], 10, 64)
		s.checkMode(mode)
		s.Modes[mode].Locks.Set(float64(lock))
	}
	return nil
}

func (s *PostgresStat) getVacuumsInProgress() error {
	cmd := fmt.Sprintf(`
SELECT * FROM pg_stat_activity
 WHERE UPPER(%s) LIKE '%%VACUUM%%';`, s.queryCol)
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	auto := 0
	manual := 0
	for _, querC := range res[s.queryCol] {
		if strings.Contains(querC, "datfrozenxid") {
			continue
		}
		m := regexp.MustCompile("(?i)(\\s*autovacuum:\\s*)?(\\s*VACUUM\\s*)?(\\s*ANALYZE\\s*)?\\s*(.+?)$").FindStringSubmatch(querC)

		//TODO: extras
		if len(m) > 0 {
			if strings.HasPrefix(querC, "autovacuum:") {
				auto += 1
			} else {
				manual += 1
			}
		}
	}
	s.Metrics.VacuumsAutoRunning.Set(float64(auto))
	s.Metrics.VacuumsManualRunning.Set(float64(manual))
	return nil
}

func (s *PostgresStat) getMainProcessInfo() error {
	out, err := exec.Command("ps", "aux").Output()
	if err != nil {
		return err
	}
	blob := string(out)
	lines := strings.Split(blob, "\n")
	for _, line := range lines {
		line = strings.Trim(line, " ")
		words := strings.Split(line, " ")

		if len(words) < 10 {
			continue
		}
		cmd := strings.Join(words[10:], " ")

		if strings.Contains(cmd, "postmaster") {
			info := make([]string, 10)
			//mapping for info: 0-user, 1-pid, 2-cpu, 3-mem, 4-vsz, 5-rss, 6-tty, 7-stat, 8-start, 9-time, 10-cmd
			for i, word := range words {
				info[i] = word
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

//TODO: double check this before testing
func (s *PostgresStat) getWriteability() error {
	_, err := s.db.QueryReturnColumnDict("CREATE SCHEMA postgres_health;")
	if err != nil {
		s.Metrics.Writable.Set(float64(0))
	}
	cmd := `
        CREATE TABLE IF NOT EXISTS postgres_health.postgres_health 
         (id INT PRIMARY KEY, stamp TIMESTAMP);`
	_, err = s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		s.Metrics.Writable.Set(float64(0))
	}
	cmd = `
        BEGIN;
        DELETE FROM postgres_health.postgres_health;
        INSERT INTO postgres_health.postgres_health VALUES (1, NOW());
        COMMIT;`
	_, err = s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		s.Metrics.Writable.Set(float64(0))
	}
	s.Metrics.Writable.Set(float64(1))
	return nil
}

func (s *PostgresStat) getSizes() error {
	cmd := fmt.Sprintf("ls -l %s/pg_xlog/ | egrep -v 'archive_status|history'", s.PGDATA)
	args := strings.Split(cmd, " ")
	out, err := exec.Command("ls", args[1:]...).Output() //TODO: fix this
	if err != nil {
		return err
	}
	blob := string(out)
	count := 0
	total := 0
	for _, line := range strings.Split(blob, "\n") {
		cols := strings.Split(line, " ")
		if len(cols) < 5 {
			continue
		}
		count += 1
		tmp, _ := strconv.Atoi(cols[4])
		total += tmp
	}
	s.Metrics.BinlogFiles.Set(float64(count))
	s.Metrics.DBSizeBinlogs.Set(float64(total))
	cmd = `
	SELECT datname AS dbname, PG_DATABASE_SIZE(datname) AS size
	  FROM pg_database;`
	//method similar here to the mysql one
	res, err := s.db.QueryMapFirstColumnToRow(cmd)
	if err != nil {
		return err
	}
	for key, value := range res {
		//key being the name of the db, value its size in bytes
		dbname := string(key)
		size, err := strconv.ParseInt(string(value[0]), 10, 64)
		if err != nil {
			s.db.Logger.Println(err)
		}
		if size > 0 {
			s.checkDB(dbname)
			s.DBs[dbname].SizeBytes.Set(float64(size))
		}
	}
	for dbname, _ := range res {
		newDsn := make(map[string]string)
		for k, v := range s.dsn {
			newDsn[k] = v
		}
		newDsn["dbname"] = dbname
		newDB, err := postgrestools.New(newDsn)
		if err != nil {
			s.db.Logger.Println(err)
			continue
		}
		cmd = `
          SELECT nspname || '.' || relname AS relation,
                 PG_TOTAL_RELATION_SIZE(C.oid) AS total_size
            FROM pg_class C
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
           WHERE nspname NOT IN ('pg_catalog', 'information_schema')
             AND C.relkind <> 'i'
             AND nspname !~ '^pg_toast'
           ORDER BY pg_total_relation_size(C.oid) DESC;`
		res, err := newDB.QueryMapFirstColumnToRow(cmd)
		if err != nil {
			s.db.Logger.Println(err)
		}
		for relation, sizes := range res {
			size, _ := strconv.ParseInt(sizes[0], 10, 64)
			if size > 0 {
				s.checkTable(dbname, relation)
				s.DBs[dbname].Tables[relation].SizeBytes.Set(float64(size))
			}
		}
		newDB.Close()
	}
	return nil
}

func (s *PostgresStat) getBackups() error {
	out, err := exec.Command("ps", "aux").Output()
	if err != nil {
		return err
	}
	blob := string(out)
	lines := strings.Split(blob, "\n")
	backupProcs := 0
	for _, line := range lines {
		words := strings.Split(line, " ")
		if len(words) < 10 {
			continue
		}
		command := strings.Join(words[10:], " ")
		if strings.Contains(command, "pg_dump") {
			backupProcs += 1
		}
	}
	s.Metrics.BackupsRunning.Set(float64(backupProcs))
	return nil
}

func (s *PostgresStat) getSecondsBehindMaster() error {
	recoveryConfFile := s.PGDATA + "/recovery.conf"
	recoveryDoneFile := s.PGDATA + "/recovery.done"
	cmd := `
SELECT EXTRACT(epoch FROM NOW()) 
     - EXTRACT(epoch FROM pg_last_xact_replay_timestamp()) AS seconds;`
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	v, ok := res["seconds"]
	if !ok || len(v) == 0 {
		return errors.New("Unable to get seconds behind master")
	}
	seconds, err := strconv.ParseInt(res["seconds"][0], 10, 64)
	if err != nil {
		return err
	}
	s.Metrics.SecondsBehindMaster.Set(float64(seconds))
	_, confErr := os.Stat(recoveryConfFile)
	if confErr == nil {
		s.Metrics.SecondsBehindMaster.Set(float64(-1))
	}
	_, doneErr := os.Stat(recoveryDoneFile)
	if doneErr == nil && os.IsNotExist(confErr) {
		s.Metrics.SecondsBehindMaster.Set(float64(-1))
	}
	return nil
}

func (s *PostgresStat) getSlaveDelayBytes() error {
	cmd := `
SELECT pg_current_xlog_location(), write_location, client_hostname
  FROM pg_stat_replication;`
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	s.Metrics.SlavesConnectedToMe.Set(float64(len(res["client_hostname"])))
	for _, val := range res["pg_current_xlog_location()"] {
		str := strings.Split(val, "/")
		if len(str) < 2 {
			return errors.New("Can't get slave delay bytes")
		} //This part can probably be cleaned up a bit when the exact format is figured out
		var masterFile, masterPos, slaveFile, slavePos int64
		masterFile, err = strconv.ParseInt(str[0], 16, 64)
		if err != nil {
			masterFile, _ = strconv.ParseInt(str[0], 0, 64)
		}
		masterPos, _ = strconv.ParseInt(str[1], 16, 64)
		if err != nil {
			masterPos, _ = strconv.ParseInt(str[1], 0, 64)
		}
		str = strings.Split(res["write_location"][0], "/")
		if len(str) < 2 {
			return errors.New("Can't get slave delay bytes")
		}
		slaveFile, _ = strconv.ParseInt(str[0], 16, 64)
		if err != nil {
			slaveFile, _ = strconv.ParseInt(str[0], 0, 64)
		}
		slavePos, _ = strconv.ParseInt(str[1], 16, 64)
		if err != nil {
			slavePos, _ = strconv.ParseInt(str[1], 0, 64)
		}
		segmentSize, _ := strconv.ParseInt("0xFFFFFFFF", 0, 64)
		r := ((masterFile * segmentSize) + masterPos) - ((slaveFile * segmentSize) + slavePos)
		s.Metrics.SlaveBytesBehindMe.Set(float64(r))
	}
	return nil
}

func (s *PostgresStat) getSecurity() error {
	cmd := "SELECT usename FROM pg_shadow WHERE passwd IS NULL;"
	res, err := s.db.QueryReturnColumnDict(cmd)
	if err != nil {
		return err
	}
	if len(res) > 0 {
		s.Metrics.UnsecureUsers.Set(float64(len(res)))
	}
	return nil
}
