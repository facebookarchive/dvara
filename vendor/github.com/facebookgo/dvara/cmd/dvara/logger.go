package main

import "log"

// stdLogger provides a logger backed by the standard library logger. This is a
// placeholder until we can open source our logger.
type stdLogger struct{}

func (l *stdLogger) Error(args ...interface{})                 { log.Print(args...) }
func (l *stdLogger) Errorf(format string, args ...interface{}) { log.Printf(format, args...) }
func (l *stdLogger) Warn(args ...interface{})                  { log.Print(args...) }
func (l *stdLogger) Warnf(format string, args ...interface{})  { log.Printf(format, args...) }
func (l *stdLogger) Info(args ...interface{})                  { log.Print(args...) }
func (l *stdLogger) Infof(format string, args ...interface{})  { log.Printf(format, args...) }
func (l *stdLogger) Debug(args ...interface{})                 { log.Print(args...) }
func (l *stdLogger) Debugf(format string, args ...interface{}) { log.Printf(format, args...) }
