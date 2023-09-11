package rabbitmq

import (
	"errors"
	"fmt"

	"github.com/Clarilab/clarimq"
)

// ErrCouldNotBeRouted is returned when a mandatory message could not be routed.
var ErrCouldNotBeRouted = errors.New("message could not be routed")

// ErrFailedToPublishChannelClosed occurs when the channel accessed but is closed.
var ErrFailedToPublishChannelClosed = errors.New("amqp channel is closed")

type AMQPError clarimq.AMQPError

func (e *AMQPError) Error() string {
	return fmt.Sprintf("Exception (%d) Reason: %q", e.Code, e.Reason)
}

// ErrRecoveryFailed occurs when the recovery failed after a connection loss.
type RecoveryFailedError struct {
	err error
}

// Error implements the Error method of the error interface.
func (e *RecoveryFailedError) Error() string {
	var str = "recovery failed: "

	if e.err != nil {
		str += e.err.Error()
	} else {
		str += "unknown error"
	}

	return str
}
