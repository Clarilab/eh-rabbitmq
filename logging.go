package rabbitmq

import (
	"github.com/Clarilab/clarimq"
)

type logger struct {
	loggers []clarimq.Logger
}

func newLogger(loggers []clarimq.Logger) *logger {
	return &logger{loggers}
}

func (l *logger) logDebug(msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Debug(msg, args...)
	}
}

func (l *logger) logError(msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Error(msg, args...)
	}
}

func (l *logger) logInfo(msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Info(msg, args...)
	}
}
