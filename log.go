package sharding

import (
	"errors"
	"log"
)

type LogLevel int

const (
	LogLevelError LogLevel = iota
	LogLevelInfo
	LogLevelDebug
	LogLevelTrace
)

var (
	DefaultLogLevel = LogLevelInfo
	ErrNotSupported = errors.New("operation not supported by underlying connection pool")
)

func debugLog(format string, args ...interface{}) {
	if DefaultLogLevel >= LogLevelDebug {
		log.Printf(format, args...)
	}
}

func traceLog(format string, args ...interface{}) {
	if DefaultLogLevel >= LogLevelTrace {
		log.Printf(format, args...)
	}
}

func infoLog(format string, args ...interface{}) {
	if DefaultLogLevel >= LogLevelInfo {
		log.Printf(format, args...)
	}
}

func errorLog(format string, args ...interface{}) {
	log.Printf(format, args...)
}
