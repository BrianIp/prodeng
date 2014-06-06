//Copyright (c) 2014 Square, Inc
//Launches metrics collector for postgres databases

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/square/prodeng/inspect-postgres/postgresstat"
	"github.com/square/prodeng/metrics"
)

func main() {
	var user, address, conf string
	var stepSec int
	var servermode, human bool

	m := metrics.NewMetricContext("system")

	flag.StringVar(&user, "u", "root", "user using database")
	flag.BoolVar(&servermode, "server", false, "Runs continously and exposes metrics as JSON on HTTP")
	flag.StringVar(&address, "address", ":12345", "address to listen on for http if running in server mode")
	flag.IntVar(&stepSec, "step", 2, "metrics are collected every step seconds")
	flag.StringVar(&conf, "conf", "/root/.my.cnf", "configuration file")
	flag.BoolVar(&human, "h", false, "Makes output in MB for human readable sizes")
	flag.Parse()

	if servermode {
		go func() {
			http.HandleFunc("/metrics.json", m.HttpJsonHandler)
			log.Fatal(http.ListenAndServe(address, nil))
		}()
	}
	step := time.Millisecond * time.Duration(stepSec) * 1000

	sqlstat, err := postgresstat.New(m, step, user, conf)
	if err != nil {
		fmt.Println(err)
		return
	}

	ticker := time.NewTicker(step * 2)
	for _ = range ticker.C {
		//Print stats here, more stats than printed are actually collected
		fmt.Println("--------------------------")
		fmt.Println("Uptime: " + strconv.Itoa(int(sqlstat.Metrics.Uptime.Get())))
	}

}
