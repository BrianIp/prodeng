package postgresstat

import (
	"time"

	"github.com/square/prodeng/inspect-postgres/postgrestools"
	"github.com/square/prodeng/metrics"
)

type PostgresStat struct {
	Metrics *PostgresStatMetrics
	m       *metrics.MetricContext
	db      *postgrestools.PostgresDB
}

type PostgresStatsMetrics struct {
	Uptime *metrics.Counter
}

func New(m *metrics.MetricContext, Step time.Duration, user, password, confid string) (*PostgresStat, error) {
	s := new(PostgresStat)

	var err error
	s.db, err = postgrestools.New(user, password, config)
	if err != nil {
		s.db.Logger.Println(err)
		return nil, err
	}

}
