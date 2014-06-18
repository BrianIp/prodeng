//Copyright (c) 2014 Square, Inc

package main

import (
	"flag"
	"fmt"
	"strconv"

	"github.com/square/prodeng/health_check/healthcheck"
)

var (
	testconfigurationfile = "/Users/brianip/Development/go/src/github.com/square/prodeng/health_check/test.config"
)

// basic checker
// launches new healthChecker
// collects metric analysis once and prints results
// Note to self: different health checker is needed for mysql vs postgres
// TODO: put on loop/integrate with inspect
func main() {
	var hostport, configFile, nagServer string

	flag.StringVar(&hostport, "p", "localhost:12345", "hostport to grab metrics")
	flag.StringVar(&configFile, "conf", "", "config file to read metric thresholds")
	flag.StringVar(&nagServer, "-nagios-server", "", "Default is '' but you probably want 'system-nagios-internal'")
	flag.Parse()
	if configFile == "" {
		configFile = testconfigurationfile
	}

	fmt.Println("starting metrics checker on: ", hostport)
	nagRouter := map[string]string{
		"mysql.slave":    "^Slave.+$",
		"mysql.com":      "^.*Com.+$",
		"mysql.sessions": "^(conn_max_pct|sess.+|loadavg.+)$",
		"mysql.long":     "^.*(ActiveLongRunQueries|Oldest_query_s|innodb_history_link_list).*$",
	}
	hc, err := healthcheck.New(hostport, configFile, nagServer, nagRouter)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = hc.NagiosCheck() // gets and analyzes metrics
	if err != nil {
		fmt.Println(err)
	}
	warnings := hc.GetAllMsgs() // only returns collection of warnings, does not generate warnings
	for lvl, msg := range warnings {
		fmt.Println(strconv.Itoa(lvl) + " : ")
		fmt.Println(msg)
	}
	hc.SendNagiosPassive()
}
