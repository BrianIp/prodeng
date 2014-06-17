//Copyright (c) 2014 Square, Inc

package main

import (
	"fmt"

	"github.com/square/prodeng/health_check/healthcheck"
)

func main() {
	fmt.Println("starting metrics checker")
	hc, err := healthcheck.New("localhost:12345")
	if err != nil {
		fmt.Println(err)
		return
	}

	err = hc.NagiosCheck()
	if err != nil {
		fmt.Println(err)
	}

}
