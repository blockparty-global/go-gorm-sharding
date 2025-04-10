package sharding

import (
	"errors"
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
	GetLogger().Debug(format, args...)
}

func traceLog(format string, args ...interface{}) {
	GetLogger().Trace(format, args...)
}

func infoLog(format string, args ...interface{}) {
	GetLogger().Info(format, args...)
}

func errorLog(format string, args ...interface{}) {
	GetLogger().Error(format, args...)
}
