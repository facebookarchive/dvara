package proxy

import (
	"fmt"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type ProxySuite struct{}

var _ = Suite(&ProxySuite{})

func (s *ProxySuite) SetUpTest(c *C) {
}

func (s *ProxySuite) getNewProxy(url string) *Proxy {
	return &Proxy{
		ProxyAddr:           fmt.Sprintf("%:2000", url),
		MongoAddr:           fmt.Sprintf("%:3000", url),
		MaxConnections:      5,
		MinIdleConnections:  5,
		ServerIdleTimeout:   5 * time.Minute,
		ServerClosePoolSize: 5,
		ClientIdleTimeout:   5 * time.Minute,
		GetLastErrorTimeout: 5 * time.Minute,
		MessageTimeout:      5 * time.Second,
	}
}

type nopLogger struct{}

func (n nopLogger) Error(args ...interface{})                 {}
func (n nopLogger) Errorf(format string, args ...interface{}) {}
func (n nopLogger) Warn(args ...interface{})                  {}
func (n nopLogger) Warnf(format string, args ...interface{})  {}
func (n nopLogger) Info(args ...interface{})                  {}
func (n nopLogger) Infof(format string, args ...interface{})  {}
func (n nopLogger) Debug(args ...interface{})                 {}
func (n nopLogger) Debugf(format string, args ...interface{}) {}
