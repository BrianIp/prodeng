//Copyright (c) 2014 Square, Inc

package healthcheck

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"

	"code.google.com/p/goconf/conf" // used for parsing config files
	"github.com/square/prodeng/metrics"
)

var (
	testconfigurationfile = "/Users/brianip/Development/go/src/github.com/square/prodeng/health_check/test.config"
	expectedValues        = map[string]float64{"testGauge2": float64(200), "testGauge3": float64(300), "testGauge4": float64(400), "testGauge5": float64(500)}
)

func initTestChecker(t testing.TB) healthChecker {
	c, err := conf.ReadConfigFile(testconfigurationfile)
	if err != nil {
		t.Fatal(err)
	}
	nagRouter := map[string]string{
		"mysql.slave":    "^Slave.+$",
		"mysql.com":      "^.*Com.+$",
		"mysql.sessions": "^(conn_max_pct|sess.+|loadavg.+)$",
		"mysql.long":     "^.*(ActiveLongRunQueries|Oldest_query_s|innodb_history_link_list).*$",
	}
	hc := healthChecker{
		hostport:    "localhost:12345",
		serviceType: "mysql",
		Metrics:     make(map[string]metric),
		CritMsgs:    make(map[string]string),
		WarnMsgs:    make(map[string]string),
		OkMsgs:      make(map[string]string),
		Warnings:    make(map[string]string),
		nagServer:   "test-server",
		routers:     nagRouter,
		c:           c,
	}

	return hc
}

func initMetricsJson() {
	m := metrics.NewMetricContext("test")
	g1 := metrics.NewGauge()
	m.Register(g1, "testGauge1")
	g2 := metrics.NewGauge()
	m.Register(g2, "testGauge2")
	g3 := metrics.NewGauge()
	m.Register(g3, "testGauge3")
	g4 := metrics.NewGauge()
	m.Register(g4, "testGauge4")
	g5 := metrics.NewGauge()
	m.Register(g5, "testGauge5")
	g2.Set(float64(200))
	g3.Set(float64(300))
	g4.Set(float64(400))
	g5.Set(float64(500))
	go func() {
		http.HandleFunc("/api/v1/metrics.json", m.HttpJsonHandler)
		http.ListenAndServe("localhost:12345", nil)
	}()
}

//Tests get metrics json correctly
func TestGetMetrics(t *testing.T) {
	//initialize checkers
	hc := initTestChecker(t)
	initMetricsJson()
	//get metrics here
	err := hc.getMetrics()
	if err != nil {
		t.Fatal(err)
	}
	//now check we collected the right metrics
	for name, metric := range hc.Metrics {
		v, ok := expectedValues[name]
		if !ok {
			t.Errorf("Unexpected metric collected: " + name)
			continue
		}
		if metric.Value != v {
			t.Errorf(fmt.Sprintf("Unexpected value in %s. Expected %f, got %f", name, v, metric.Value))
		}
	}
}

//test check metric against threshold correctly: expecting CRIT
func TestCheckMetric1(t *testing.T) {
	//initialize thresholds
	m := metricThresholds{
		metricname:    "testmetric",
		critThresh:    float64(9000),
		warnThresh:    float64(8000),
		critMsg:       "its over 9000!",
		warnMsg:       "warn message",
		okMsg:         "ok message",
		check:         " > ",
		levelNotFound: CRIT,
		msgNotFound:   "not found :(",
	}
	expected_value := float64(9001)
	expected_level := CRIT
	expected_mssge := "its over 9000!"
	//do check
	level, val, message := checkMetric(m, expected_value)
	//check results
	if level != expected_level {
		t.Errorf(fmt.Sprintf("Wrong warning level returned, expected %d, got %d", CRIT, level))
	}
	if val != fmt.Sprintf("%s=%f", m.metricname, expected_value) {
		t.Errorf("wrong val message: " + val)
	}
	if message != expected_mssge {
		t.Errorf(fmt.Sprintf("Wrong warning message, expected: %s, but got %s", message, expected_mssge))
	}
}

//test check metric against threshold correctly: expecting WARN
func TestCheckMetric2(t *testing.T) {
	//initialize thresholds
	m := metricThresholds{
		metricname:    "testmetric",
		critThresh:    float64(9000),
		warnThresh:    float64(8000),
		critMsg:       "its over 9000!",
		warnMsg:       "warn message",
		okMsg:         "ok message",
		check:         " > ",
		levelNotFound: CRIT,
		msgNotFound:   "not found :(",
	}
	expected_value := float64(8001)
	expected_level := WARN
	expected_mssge := "warn message"
	//do check
	level, val, message := checkMetric(m, expected_value)
	//check results
	if level != expected_level {
		t.Errorf(fmt.Sprintf("Wrong warning level returned, expected %d, got %d", CRIT, level))
	}
	if val != fmt.Sprintf("%s=%f", m.metricname, expected_value) {
		t.Errorf("wrong val message: " + val)
	}
	if message != expected_mssge {
		t.Errorf(fmt.Sprintf("Wrong warning message, expected: %s, but got %s", message, expected_mssge))
	}
}

//test check metric against threshold correctly: expecting OK
func TestCheckMetric3(t *testing.T) {
	//initialize thresholds
	m := metricThresholds{
		metricname:    "testmetric",
		critThresh:    float64(9000),
		warnThresh:    float64(8000),
		critMsg:       "its over 9000!",
		warnMsg:       "warn message",
		okMsg:         "ok message",
		check:         " > ",
		levelNotFound: CRIT,
		msgNotFound:   "not found :(",
	}
	expected_value := float64(7999)
	expected_level := OK
	expected_mssge := "ok message"
	//do check
	level, val, message := checkMetric(m, expected_value)
	//check results
	if level != expected_level {
		t.Errorf(fmt.Sprintf("Wrong warning level returned, expected %d, got %d", CRIT, level))
	}
	if val != fmt.Sprintf("%s=%f", m.metricname, expected_value) {
		t.Errorf("wrong val message: " + val)
	}
	if message != expected_mssge {
		t.Errorf(fmt.Sprintf("Wrong warning message, expected: %s, but got %s", message, expected_mssge))
	}
}

//test check metric against threshold correctly: expecting not found
func TestCheckMetric4(t *testing.T) {
	//initialize thresholds
	m := metricThresholds{
		metricname:    "testmetric",
		critThresh:    float64(9000),
		warnThresh:    float64(8000),
		critMsg:       "its over 9000!",
		warnMsg:       "warn message",
		okMsg:         "ok message",
		check:         " > ",
		levelNotFound: CRIT,
		msgNotFound:   "not found :(",
	}
	expected_value := math.NaN()
	expected_level := CRIT
	expected_mssge := "not found :("
	//do check
	level, val, message := checkMetric(m, expected_value)
	//check results
	if level != expected_level {
		t.Errorf(fmt.Sprintf("Wrong warning level returned, expected %d, got %d", CRIT, level))
	}
	if val != fmt.Sprintf("%s=%f", m.metricname, expected_value) {
		t.Errorf("wrong val message: " + val)
	}
	if message != expected_mssge {
		t.Errorf(fmt.Sprintf("Wrong warning message, expected: %s, but got %s", message, expected_mssge))
	}
}

//test check metric against threshold correctly, testing check <=
func TestCheckMetric5(t *testing.T) {
	//initialize thresholds
	m := metricThresholds{
		metricname:    "testmetric",
		critThresh:    float64(7000),
		warnThresh:    float64(8000),
		critMsg:       "critical message",
		warnMsg:       "warn message",
		okMsg:         "ok message",
		check:         " <= ",
		levelNotFound: CRIT,
		msgNotFound:   "not found :(",
	}
	expected_value := float64(6000)
	expected_level := CRIT
	expected_mssge := "critical message"
	//do check
	level, val, message := checkMetric(m, expected_value)
	//check results
	if level != expected_level {
		t.Errorf(fmt.Sprintf("Wrong warning level returned, expected %d, got %d", CRIT, level))
	}
	if val != fmt.Sprintf("%s=%f", m.metricname, expected_value) {
		t.Errorf("wrong val message: " + val)
	}
	if message != expected_mssge {
		t.Errorf(fmt.Sprintf("Wrong warning message, expected: %s, but got %s", message, expected_mssge))
	}
}

//test finding all metrics with regexp
func TestMatchMetrics1(t *testing.T) {
	hc := initTestChecker(t)
	hc.Metrics = map[string]metric{
		"database1.testMetrics":  metric{Value: float64(1)},
		"database2.testMetrics":  metric{Value: float64(2)},
		"database3.testMetrics":  metric{Value: float64(3)},
		"testMetrics.submetric1": metric{Value: float64(4)},
		"testMetrics.submetric2": metric{Value: float64(5)},
		"testMetrics.submetric3": metric{Value: float64(6)},
	}
	expectedValues := map[string]float64{
		"database1.testMetrics":  float64(1),
		"database2.testMetrics":  float64(2),
		"database3.testMetrics":  float64(3),
		"testMetrics.submetric1": float64(4),
		"testMetrics.submetric2": float64(5),
		"testMetrics.submetric3": float64(6),
	}
	res := hc.matchMetrics("testMetrics")
	for name, val := range res {
		if v, ok := expectedValues[name]; !ok {
			t.Errorf(fmt.Sprintf("Unexpected metric found: %s", name))
		} else if v != val {
			t.Errorf(fmt.Sprintf("Unexpected value for metric %s: wanted %f, but got %f", name, v, val))
		}
	}
}

//test finding some metrics with regexp
func TestMatchMetrics2(t *testing.T) {
	hc := initTestChecker(t)
	hc.Metrics = map[string]metric{
		"database1.testMetrics":   metric{Value: float64(1)},
		"database2.testMetrics":   metric{Value: float64(2)},
		"database3.testMetrics":   metric{Value: float64(3)},
		"testMetrics.submetric1":  metric{Value: float64(4)},
		"testMetrics.submetric2":  metric{Value: float64(5)},
		"testMetrics.submetric3":  metric{Value: float64(6)},
		"metricshouldntmatch":     metric{Value: float64(7)},
		"metricalsoshouldntmatch": metric{Value: float64(8)},
	}
	expectedValues := map[string]float64{
		"database1.testMetrics":  float64(1),
		"database2.testMetrics":  float64(2),
		"database3.testMetrics":  float64(3),
		"testMetrics.submetric1": float64(4),
		"testMetrics.submetric2": float64(5),
		"testMetrics.submetric3": float64(6),
	}
	res := hc.matchMetrics("testMetrics")
	for name, val := range res {
		if v, ok := expectedValues[name]; !ok {
			t.Errorf(fmt.Sprintf("Unexpected metric found: %s", name))
		} else if v != val {
			t.Errorf(fmt.Sprintf("Unexpected value for metric %s: wanted %f, but got %f", name, v, val))
		}
	}
}

//test finding no metrics with regexp
func TestMatchMetrics3(t *testing.T) {
	hc := initTestChecker(t)
	hc.Metrics = map[string]metric{
		"database1.testMetrics":   metric{Value: float64(1)},
		"database2.testMetrics":   metric{Value: float64(2)},
		"database3.testMetrics":   metric{Value: float64(3)},
		"testMetrics.submetric1":  metric{Value: float64(4)},
		"testMetrics.submetric2":  metric{Value: float64(5)},
		"testMetrics.submetric3":  metric{Value: float64(6)},
		"metricshouldntmatch":     metric{Value: float64(7)},
		"metricalsoshouldntmatch": metric{Value: float64(8)},
	}
	expectedValues := map[string]float64{}
	res := hc.matchMetrics("tableSizes")
	for name, val := range res {
		if v, ok := expectedValues[name]; !ok {
			t.Errorf(fmt.Sprintf("Unexpected metric found: %s", name))
		} else if v != val {
			t.Errorf(fmt.Sprintf("Unexpected value for metric %s: wanted %f, but got %f", name, v, val))
		}
	}
}

func TestFormatWarnings(t *testing.T) {
	hc := initTestChecker(t)
	hc.CritMsgs = map[string]string{
		"service1.metric1": "1",
		"service1.metric2": "2",
		"service2.metric1": "5",
		"service3.metric2": "6",
		"service1.metric3": "3",
	}
	hc.AllMsgs = map[int]map[string]string{CRIT: hc.CritMsgs, WARN: hc.WarnMsgs, OK: hc.OkMsgs}
	expected_level := CRIT
	//expected chars: 1 2 3
	res, level := hc.formatWarnings("service1")
	if level != expected_level {
		t.Errorf(fmt.Sprintf("Unexpected level: got $d, but wanted %d", level, expected_level))
	}
	if !(strings.Contains(res, " 1") &&
		strings.Contains(res, " 2") &&
		strings.Contains(res, " 3") &&
		!strings.Contains(res, " 5") &&
		!strings.Contains(res, " 6")) {
		t.Errorf(fmt.Sprintf("Unexpected string: %s", res))
	}
}

func TestFormatWarnings2(t *testing.T) {
	hc := initTestChecker(t)
	hc.WarnMsgs = map[string]string{
		"service1.metric1": "1",
		"service1.metric2": "2",
		"service2.metric1": "5",
		"service3.metric2": "6",
		"service1.metric3": "3",
	}
	hc.AllMsgs = map[int]map[string]string{CRIT: hc.CritMsgs, WARN: hc.WarnMsgs, OK: hc.OkMsgs}
	expected_level := WARN
	//expected chars: 1 2 3
	res, level := hc.formatWarnings("service1")
	if level != expected_level {
		t.Errorf(fmt.Sprintf("Unexpected level: got $d, but wanted %d", level, expected_level))
	}
	if !(strings.Contains(res, " 1") &&
		strings.Contains(res, " 2") &&
		strings.Contains(res, " 3") &&
		!strings.Contains(res, " 5") &&
		!strings.Contains(res, " 6")) {
		t.Errorf(fmt.Sprintf("Unexpected string: %s", res))
	}
}

func TestFormatWarnings3(t *testing.T) {
	hc := initTestChecker(t)
	hc.OkMsgs = map[string]string{
		"service1.metric1": "1",
		"service1.metric2": "2",
		"service2.metric1": "5",
		"service3.metric2": "6",
		"service1.metric3": "3",
	}
	hc.AllMsgs = map[int]map[string]string{CRIT: hc.CritMsgs, WARN: hc.WarnMsgs, OK: hc.OkMsgs}
	expected_level := OK
	//expected chars: 1 2 3
	res, level := hc.formatWarnings("service1")
	if level != expected_level {
		t.Errorf(fmt.Sprintf("Unexpected level: got $d, but wanted %d", level, expected_level))
	}
	if !(strings.Contains(res, " 1") &&
		strings.Contains(res, " 2") &&
		strings.Contains(res, " 3") &&
		!strings.Contains(res, " 5") &&
		!strings.Contains(res, " 6")) {
		t.Errorf(fmt.Sprintf("Unexpected string: %s", res))
	}
}

//TODO: implement tests for NagiosCheck and SendNagios
