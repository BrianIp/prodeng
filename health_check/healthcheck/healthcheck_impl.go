//Copyright (c) 2014 Square, Inc

package healthcheck

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os/exec"
	"strconv"

	"code.google.com/p/goconf/conf" // used for parsing config files
)

//TODO: send to Nagios
//TODO: compile nag warnings

const (
	NSCA_BINARY_PATH      = "/usr/sbin/send_nsca"
	NSCA_CONFIG_PATH      = "/etc/nagios/send_nsca.cfg"
	DEFAULT_NAGIOS_SERVER = "system-nagios-internal"
	DEFAULT_CRIT_PCT      = 95
	DEFAULT_WARN_PCT      = 85
	CRIT                  = "CRIT"
	WARN                  = "WARN"
	OK                    = "OK"
)

var (
	NAG_CODE              = map[string]int{OK: 0, WARN: 1, CRIT: 2}
	testconfigurationfile = "/Users/brianip/Development/go/src/github.com/square/prodeng/health_check/test.config"
)

type healthChecker struct {
	hostport  string
	collector *http.Client
	conn      net.Conn
	Metrics   map[string]metric
	Warnings  map[string]string
	c         *conf.ConfigFile
}

type metric struct {
	Type  string
	Name  string
	Value float64
	Rate  float64
}

func New(hostport string) (HealthChecker, error) {
	//TODO: connection to nagios server
	c, err := conf.ReadConfigFile(testconfigurationfile)
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
	out, err := exec.Command("TODO: command").Output()
	if err != nil {
		return err
	}
	fmt.Println(out)
	return errors.New("Not yet Implemented")
}

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

func (hc *healthChecker) NagiosCheck() error {
	err := hc.getMetrics()
	if err != nil {
		fmt.Println(err)
		fmt.Println("3")
		return err
	}

	tests := hc.c.GetSections()
	for _, test := range tests {
		metricName, crit, warn := getVals(hc.c, test)

		val := hc.Metrics[metricName].Value

		if val > crit {
			hc.Warnings[metricName] = "CRIT"
		} else if val > warn {
			hc.Warnings[metricName] = "WARN"
		} else {
			hc.Warnings[metricName] = "OK"
		}
	}
	return nil
}

func getVals(c *conf.ConfigFile, test string) (string, float64, float64) {
	metricName, err := c.GetString(test, "metric")
	if err != nil {
		fmt.Println(err)
		return "", math.NaN(), math.NaN()
	}
	tmp1, err := c.GetString(test, "crit-threshold")
	if err != nil {
		fmt.Println(err)
		return "", math.NaN(), math.NaN()
	}
	crit, err := strconv.ParseFloat(tmp1, 64)
	if err != nil {
		fmt.Println(err)
		return "", math.NaN(), math.NaN()
	}
	tmp2, err := c.GetString(test, "warn-threshold")
	if err != nil {
		fmt.Println(err)
		return "", math.NaN(), math.NaN()
	}
	warn, err := strconv.ParseFloat(tmp2, 64)
	if err != nil {
		fmt.Println(err)
		return "", math.NaN(), math.NaN()
	}
	return metricName, crit, warn
}

func (hc *healthChecker) GetWarnings() map[string]string {
	return hc.Warnings
}
