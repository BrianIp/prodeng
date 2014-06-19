//Copyright (c) 2014 Square, Inc

package healthcheck

type HealthChecker interface {
	//Send the recored metrics warnings to the nagios server
	SendNagiosPassive() error

	//Check the metrics against their thresholds
	NagiosCheck() error

	//Get the a list of all warnings found
	GetWarnings() map[string]string

	//Group the warnings by their levels, i.e. CRIT, WARN, OK
	GetAllMsgs() map[int]map[string]string
}
