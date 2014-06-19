#health checks

health_check is a collection of libraries for checking and sending metrics to nagios.

health_check command line is a utility that launches a goroutine that will loop and send nagios reports on a specified interval, it will also display the results of the metrics checks.

health_check gets the metrics via http json, and checks the metric values against thresholds in a given configuration file.

##Installation

1. Get Go
2. `go get -v -u github.com/square/prodeng/health_check`

##Usage

###Configuration File
The configuration file specifies which metrics will be reported and their thresholds.
An example of the wanted format:
```
[services]
mysql.service1 = ^.*metric_name.+$
mysql.service2 = ^.+other_metrics$

[test1]
metric-name = mysqlstat.some_metric_name
check = >
crit-threshold = 1060000
warn-threshold = 1040000
crit-message = super critical
warn-message = warning message
ok-message = ok message
level-if-not-found = CRIT
message-if-not-found = metric not gathered

[test2]
metric-type = .*SizeBytes.+
check = <=
crit-threshold = 100000
warn-threshold = 50000
crit-message = size way too big
warn-message = size kinda too big
ok-message = size is ok
```
The `services` section specifies the service names and the metrics related to those services. In that section, each service name corresponds to a regexp that will match the metric names desired.

The remaining sections specify thresholds for metrics.
Each section requires$ that either a `metric-name` or `metric-type` be specified. `metric-name` specifies an exact namematch for a metric. `metric-type` specifies a regexp that will match a set of metrics.
`crit-threshold` and `warn-threshold` specify the levels for `CRIT` and `WARN` level messages respectively. `check` specifies the comparison type; the metric value will be on the left-hand side, and the threshold on the right hand side. For example, if `check = >` then the checks made will be of the form `metric_value > threshold`. If the result of the check is `true`, then the message level will be set to the threshold's level. If the check does not fall into either level, then it will fall into the `OK` level.

`crit-message`, `warn-message`, and `ok-message` specify a message that will be sent to nagios for each metric.
`level-if-not-found` specifies the message level if the metric desired is not collected/found. Usually this occurs with metrics that have NaN values since the json api used cannot handle NaN values and filters them out. `message-if-not-found` specifies the message that will be sent for this case.

###Command Line

Example Usage
`./bin/health_check`
```
starting metrics checker on: localhost:12345
Warnings for warning level CRIT:
...
Warnings for warning level WARN:
...
Warnings for warning level OK:
...
```

###Example API Use
```
//Import packages
import "github.com/square/prodeng/health_check"

//Create new health checker
// hostport refers to the hostport to listen on for metrics
// configFile specifies the path to the config file described above
// nagServer is the nagios server messages are sent to
// serviceType refers to: mysql, postgres, etc.
hc, err := healthcheck.New(hostport, configFile, nagServer, serviceType)

//Get and check the metrics against their thresholds
err := NagiosCheck()

//Send the compiled warnings to the nagios server
err := SendNagiosPassive()

//Get all warnings found. Returns a map of metric name -> warning message
warnings := GetWarnings()

//Get all warnings found. Grouped by message level, i.e. CRIT, WARN, OK
messages := GetAllMsgs()
```

##Testing
Packages are tested using Go's testing package.
Tests for each package are found in their corresponding `_test.go` file.

To test:
1. cd to tbe directory containing the `.go` and `_test.go` files
2. Run `go test`. You can also run with the `-v` option for a verbose output.


