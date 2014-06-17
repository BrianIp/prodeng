//Copyright (c) 2014 Square, Inc

package healthcheck

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	//	"os/exec"
	"os"
	"regexp"
	"strconv"
	"strings"

	"code.google.com/p/goconf/conf" // used for parsing config files
)

//TODO: send to Nagios
//TODO: compile nag warning messages

const (
	NSCA_BINARY_PATH      = "/usr/sbin/send_nsca"
	NSCA_CONFIG_PATH      = "/etc/nagios/send_nsca.cfg"
	DEFAULT_NAGIOS_SERVER = "system-nagios-internal"
	DEFAULT_CRIT_PCT      = 95
	DEFAULT_WARN_PCT      = 85
	OK                    = iota
	WARN                  = iota
	CRIT                  = iota
)

type healthChecker struct {
	hostport  string
	collector *http.Client
	conn      net.Conn
	Metrics   map[string]metric
	Warnings  map[string]string
	c         *conf.ConfigFile
}

type metricThresholds struct {
	metricname string
	metrictype string
	critThresh float64
	warnThresh float64
	critMsg    string
	warnMsg    string
	okMsg      string
	check      string
}

type metric struct {
	Type  string
	Name  string
	Value float64
	Rate  float64
}

func New(hostport, configFile string) (HealthChecker, error) {
	c, err := conf.ReadConfigFile(configFile)
	if err != nil {
		return nil, err
	}

	hc := &healthChecker{
		hostport:  hostport,
		collector: nil,
		conn:      nil,
		Metrics:   make(map[string]metric),
		Warnings:  make(map[string]string),
		c:         c,
	}

	return hc, nil
}

func (hc *healthChecker) SendNagiosPassive() error {
	hostname, _ := os.Hostname()
	service := "test_service"        //For Testing
	state_code := strconv.Itoa(CRIT) //For Testing
	message := "hello"               //Obviously for testing
	info := strings.Join([]string{hostname, service, state_code, message}, "\t")
	cmd := fmt.Sprintf("printf %s\\n", info)
	fmt.Println(cmd)
	/*	out, err := exec.Command("TODO: command").Output()
		if err != nil {
			return err
		}
		fmt.Println(out)*/
	return errors.New("Not yet Implemented")
}

//gets metrics and unmarshals from JSON
func (hc *healthChecker) getMetrics() error {
	resp, err := http.Get("http://" + hc.hostport + "/metrics.json")
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("1")
		return err
	}
	var metrics []metric
	err = json.Unmarshal(body, &metrics)
	if err != nil {
		fmt.Println("2")
		return err
	}
	for _, m := range metrics {
		hc.Metrics[m.Name] = m
	}
	return nil
}

//Checks metrics and TODO: send to Nagios
func (hc *healthChecker) NagiosCheck() error {
	err := hc.getMetrics()
	if err != nil {
		fmt.Println(err)
		fmt.Println("3")
		return err
	}

	tests := hc.c.GetSections()
	for _, test := range tests {
		if test == "default" {
			continue
		}
		m := getVals(hc.c, test)
		if m.metrictype != "" {
			vals := hc.matchMetrics(m.metrictype)
			for name, val := range vals {
				if _, ok := hc.Warnings[name]; ok {
					continue
				}
				m.metricname = name
				hc.Warnings[m.metricname] = checkMetric(m, val)
			}
			continue
		}
		if _, ok := hc.Metrics[m.metricname]; !ok {
			hc.Warnings[m.metricname] = "not collected"
		}
		val := hc.Metrics[m.metricname].Value
		hc.Warnings[m.metricname] = checkMetric(m, val)
	}
	return nil
}

func checkMetric(m metricThresholds, val float64) string {
	type compFunc func(float64, float64) bool
	fns := map[string]compFunc{
		">":  func(x, y float64) bool { return x > y },
		"<":  func(x, y float64) bool { return x < y },
		">=": func(x, y float64) bool { return x >= y },
		"<=": func(x, y float64) bool { return x <= y },
		"==": func(x, y float64) bool { return x == y },
	}
	cmp := fns[m.check]
	if cmp(val, m.critThresh) {
		return "CRIT: " + fmt.Sprintf("%s=%f", m.metricname, val) + " : " + m.critMsg
	} else if cmp(val, m.warnThresh) {
		return "WARN: " + fmt.Sprintf("%s=%f", m.metricname, val) + " : " + m.warnMsg
	}
	return "OK: " + fmt.Sprintf("%s=%f", m.metricname, val) + " : " + m.okMsg
}

//Goes through metric names and returns matches to regexp
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

func getVals(c *conf.ConfigFile, test string) metricThresholds {
	metricName, _ := c.GetString(test, "metric-name")
	crit, _ := c.GetFloat64(test, "crit-threshold")
	warn, _ := c.GetFloat64(test, "warn-threshold")
	wm, _ := c.GetString(test, "warn-message")
	cm, _ := c.GetString(test, "crit-message")
	om, _ := c.GetString(test, "ok-message")
	re, _ := c.GetString(test, "metric-type")
	check, _ := c.GetString(test, "check")
	m := &metricThresholds{
		metricname: strings.TrimSpace(metricName),
		metrictype: strings.TrimSpace(re),
		critThresh: crit,
		warnThresh: warn,
		warnMsg:    strings.TrimSpace(wm),
		critMsg:    strings.TrimSpace(cm),
		okMsg:      strings.TrimSpace(om),
		check:      strings.TrimSpace(check),
	}
	return *m
}

func (hc *healthChecker) GetWarnings() map[string]string {
	return hc.Warnings
}
