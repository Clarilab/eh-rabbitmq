package rabbitmq

import (
	"context"

	"github.com/Clarilab/clarimq"
)

type logger struct {
	loggers []clarimq.Logger
}

func newLogger(loggers []clarimq.Logger) *logger {
	return &logger{loggers}
}

func (l *logger) logDebug(ctx context.Context, msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Debug(ctx, msg, args...)
	}
}

func (l *logger) logError(ctx context.Context, msg string, err error, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Error(ctx, msg, err, args...)
	}
}

func (l *logger) logWarn(ctx context.Context, msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Warn(ctx, msg, args...)
	}
}

func (l *logger) logInfo(ctx context.Context, msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Info(ctx, msg, args...)
	}
}
