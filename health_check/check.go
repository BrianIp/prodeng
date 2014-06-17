//Copyright (c) 2014 Square, Inc

package main

import (
	"flag"
	"fmt"

	"github.com/square/prodeng/health_check/healthcheck"
)

var (
	testconfigurationfile = "/Users/brianip/Development/go/src/github.com/square/prodeng/health_check/test.config"
)

// basic checker
// launches new healthChecker
// collects metric analysis once and prints results
// TODO: put on loop/integrate with inspect
func main() {
	var hostport, configFile string

	flag.StringVar(&hostport, "p", "localhost:12345", "hostport to grab metrics")
	flag.StringVar(&configFile, "conf", "", "config file to read metric thresholds")
	flag.Parse()
	if configFile == "" {
		configFile = testconfigurationfile
	}

	fmt.Println("starting metrics checker on: ", hostport)
	hc, err := healthcheck.New(hostport, configFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = hc.NagiosCheck() // gets and analyzes metrics
	if err != nil {
		fmt.Println(err)
	}
	warnings := hc.GetWarnings() // only returns collection of warnings, does not generate warnings

	for _, msg := range warnings {
		fmt.Println(msg)
	}

}
