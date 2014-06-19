//Copyright (c) 2014 Square, Inc

package healthcheck

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	//"os"
	//"os/exec"
	"regexp"
	//"strconv"
	"strings"

	"code.google.com/p/goconf/conf" // used for parsing config files
)

//TODO: send to Nagios
//TODO: compile nag warning messages formatted correctly

const (
	OK = iota
	WARN
	CRIT
	UNKNOWN
	NSCA_BINARY_PATH      = "/usr/sbin/send_nsca"
	NSCA_CONFIG_PATH      = "/etc/nagios/send_nsca.cfg"
	DEFAULT_NAGIOS_SERVER = "system-nagios-internal"
	DEFAULT_CRIT_PCT      = 95
	DEFAULT_WARN_PCT      = 85
)

type healthChecker struct {
	hostport    string
	serviceType string //mysql, postgres, etc.
	Metrics     map[string]metric
	AllMsgs     map[int]map[string]string //map of warning level mapping to CritMsgs, WarnMsgs, or OkMsgs
	CritMsgs    map[string]string
	WarnMsgs    map[string]string
	Warnings    map[string]string
	OkMsgs      map[string]string
	routers     map[string]string //maps service name to a regexp string that matches metrics collected for that service
	c           *conf.ConfigFile
	nagServer   string
}

type metricThresholds struct {
	metricname    string
	metrictype    string
	critThresh    float64
	warnThresh    float64
	critMsg       string
	warnMsg       string
	okMsg         string
	check         string
	levelNotFound int // level of warning if metric is not found, default is WARN
	msgNotFound   string
}

type warning struct {
	level int
	msg   string
}

type metric struct {
	Type  string
	Name  string
	Value float64
	Rate  float64
}

var (
	nagLevels = map[string]int{"OK": 0, "WARN": 1, "CRIT": 2, "UNKNOWN": 3}
)

//Creates new healthChecker
//hostport is address to listen on for metrics json
func New(hostport, configFile, nagServer, serviceType string) /*, routers map[string]string)*/ (HealthChecker, error) {
	c, err := conf.ReadConfigFile(configFile)
	if err != nil {
		return nil, err
	}

	hc := &healthChecker{
		hostport:    hostport, //hostport to listen on for metrics json
		serviceType: serviceType,
		Metrics:     make(map[string]metric),
		CritMsgs:    make(map[string]string),
		WarnMsgs:    make(map[string]string),
		OkMsgs:      make(map[string]string),
		Warnings:    make(map[string]string),
		nagServer:   nagServer,
		//routers:   routers,
		c: c,
	}
	hc.AllMsgs = map[int]map[string]string{CRIT: hc.CritMsgs, WARN: hc.WarnMsgs, OK: hc.OkMsgs}
	hc.getServices()
	return hc, nil
}

//Sends nagios server metrics warnings
func (hc *healthChecker) SendNagiosPassive() error {
	//for service, regex := range hc.routers {
	//message, state_code := hc.formatWarnings(regex)
	//hostname, _ := os.Hostname()
	//info := strings.Join([]string{hostname, service, strconv.Itoa(state_code), message}, "\t")
	//printCmd := exec.Command("printf", fmt.Sprintf("\"%s\\n\"", info))
	//sendCmd := exec.Command(NSCA_BINARY_PATH, hc.nagServer, "-c "+NSCA_CONFIG_PATH)
	//sendCmd.Stdin, _ = printCmd.StdoutPipe()
	//sendCmd.Start()
	//printCmd.Run()
	//err := sendCmd.Wait()
	//if err != nil {
	//	return err
	//}
	//}
	return nil
}

//gets metrics and unmarshals from JSON
func (hc *healthChecker) getMetrics() error {
	//get metrics from metrics collector
	resp, err := http.Get("http://" + hc.hostport + "/api/v1/metrics.json?allowNaN=false")
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	//unmarshal metrics
	var metrics []metric
	err = json.Unmarshal(body, &metrics)
	if err != nil {
		return err
	}
	//store metrics in map, so they can be found easily by name
	for _, m := range metrics {
		hc.Metrics[m.Name] = m
	}
	return nil
}

//Checks metrics
func (hc *healthChecker) NagiosCheck() error {
	err := hc.getMetrics()
	if err != nil {
		fmt.Println(err)
		return err
	}

	tests := hc.c.GetSections()
	for _, test := range tests {
		if test == "default" {
			continue
		}
		m := getVals(hc.c, test)
		//If the user has specified a general type of metric to be checked against
		// a common threshold
		if m.metrictype != "" {
			vals := hc.matchMetrics(m.metrictype)
			for name, val := range vals {
				if _, ok := hc.Warnings[name]; ok {
					continue
				}
				m.metricname = name
				lvl, value, message := checkMetric(m, val)
				hc.AllMsgs[lvl][name] = value + " : " + message
			}
			continue
		}
		//Else just compare the one metric against its thresholds
		var val float64
		//Assign val NaN if not found. Usually the metrics collector will collect the metric and
		// assign NaN to it itself, but the json api currently used does not support nan so these
		// metrics were filtered out.
		if m.metricname != "" {
			if _, ok := hc.Metrics[m.metricname]; !ok {
				val = math.NaN()
			} else {
				val = hc.Metrics[m.metricname].Value
			}
			lvl, value, message := checkMetric(m, val)
			hc.AllMsgs[lvl][m.metricname] = value + " : " + message + ";"
		}
	}
	return nil
}

//formats warning messages to be sent to nagios server
func (hc *healthChecker) formatWarnings(service string) (string, int) {
	res := ""
	re := regexp.MustCompile(service)
	for i := 2; i >= 0; i-- { //start with critical messages first
		for key, msg := range hc.AllMsgs[i] {
			if re.MatchString(key) {
				res += " " + msg
			}
		}
		if res != "" {
			return res, i
		}
	}
	return res, 0
}

//checks the metric's measured value against thresholds and returns the
//corresponsing warning level, value (in string form), and warning message
func checkMetric(m metricThresholds, val float64) (int, string, string) {
	//If the metric val = NaN or was not found, assign the warning messages specified
	if math.IsNaN(val) {
		return m.levelNotFound, fmt.Sprintf("%s=%f", m.metricname, val), m.msgNotFound
	}
	type compFunc func(float64, float64) bool
	fns := map[string]compFunc{
		">":  func(x, y float64) bool { return x > y },
		"<":  func(x, y float64) bool { return x < y },
		">=": func(x, y float64) bool { return x >= y },
		"<=": func(x, y float64) bool { return x <= y },
		"==": func(x, y float64) bool { return x == y },
	}
	cmp := fns[strings.TrimSpace(m.check)]
	valstring := fmt.Sprintf("%s=%f", m.metricname, val)
	if cmp(val, m.critThresh) {
		return CRIT, valstring, m.critMsg
	} else if cmp(val, m.warnThresh) {
		return WARN, valstring, m.warnMsg
	}
	return OK, valstring, m.okMsg
}

//Goes through metric names and returns matches to input regexp.
//Returns a map of metric name -> metric value
func (hc *healthChecker) matchMetrics(re string) map[string]float64 {
	res := make(map[string]float64)
	r := regexp.MustCompile(re)
	for key, metric := range hc.Metrics {
		if r.MatchString(key) {
			res[key] = metric.Value
		}
	}
	return res
}

//gets the services and metrics for the nagios server
func (hc *healthChecker) getServices() {
	nag_routers := map[string]string{hc.serviceType + ".health": ".+"}
	serviceNames, err := hc.c.GetOptions("services")
	if err != nil {
		hc.routers = nag_routers
		return
	}
	for _, serviceName := range serviceNames {
		serviceMetrics, _ := hc.c.GetString("services", serviceName)
		nag_routers[serviceName] = serviceMetrics
	}
	hc.routers = nag_routers
	return
}

//Reads the thresholds and messages from the config file
func getVals(c *conf.ConfigFile, test string) metricThresholds {
	metricName, _ := c.GetString(test, "metric-name")
	crit, _ := c.GetFloat64(test, "crit-threshold")
	warn, _ := c.GetFloat64(test, "warn-threshold")
	wm, _ := c.GetString(test, "warn-message")
	cm, _ := c.GetString(test, "crit-message")
	om, _ := c.GetString(test, "ok-message")
	re, _ := c.GetString(test, "metric-type")
	check, _ := c.GetString(test, "check")
	levelNotFound, err := c.GetString(test, "level-if-not-found")
	if err != nil {
		levelNotFound = "WARN"
	}
	msgNotFound, err := c.GetString(test, "message-if-not-found")
	if err != nil {
		msgNotFound = "metric not collected"
	}
	m := &metricThresholds{
		metricname:    strings.TrimSpace(metricName),
		metrictype:    strings.TrimSpace(re),
		critThresh:    crit,
		warnThresh:    warn,
		warnMsg:       strings.TrimSpace(wm),
		critMsg:       strings.TrimSpace(cm),
		okMsg:         strings.TrimSpace(om),
		check:         strings.TrimSpace(check),
		levelNotFound: nagLevels[strings.ToUpper(strings.TrimSpace(levelNotFound))],
		msgNotFound:   strings.TrimSpace(msgNotFound),
	}
	return *m
}

func (hc *healthChecker) GetWarnings() map[string]string {
	return hc.Warnings
}

func (hc *healthChecker) GetAllMsgs() map[int]map[string]string {
	return hc.AllMsgs
}
