//Copyright (c) 2014 Square, Inc

package healthcheck

type HealthChecker interface {
	SendNagiosPassive() error
	NagiosCheck() error
	GetWarnings() map[string]string
}
