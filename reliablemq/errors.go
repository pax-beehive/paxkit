package reliablemq

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidEnvelope            = errors.New("reliablemq: invalid envelope")
	ErrInvalidFrame               = errors.New("reliablemq: invalid frame")
	ErrInvalidDecision            = errors.New("reliablemq: invalid decision")
	ErrDuplicate                  = errors.New("reliablemq: duplicate frame")
	ErrRejected                   = errors.New("reliablemq: rejected by hook")
	ErrProducerNotReady           = errors.New("reliablemq: producer is not ready")
	ErrProducerClosed             = errors.New("reliablemq: producer is closed")
	ErrProducerDisconnected       = errors.New("reliablemq: producer network writer is disconnected")
	ErrProducerJournalGap         = errors.New("reliablemq: producer journal has a sequence gap")
	ErrProducerJournalLimit       = errors.New("reliablemq: producer journal safety limit exceeded")
	ErrInboundHistoryInconsistent = errors.New("reliablemq: inbound journal disagrees with applied cursor")
)

// InboundHistoryError reports inconsistent durable inbound history or checkpoints.
// The journal must be inspected before recovery; its payload may be a command.
type InboundHistoryError struct {
	QueueID        string
	Stream         Stream
	Seq            int64
	AppliedThrough int64
	ExpectedSeq    int64
	AckedThrough   int64
}

func (e *InboundHistoryError) Error() string {
	if e.Seq == 0 && e.AppliedThrough > e.AckedThrough {
		return fmt.Sprintf("%s: queue %q/%q applied cursor %d exceeds durable receipt cursor %d; preserve the journal and inspect delivery state before recovery",
			ErrInboundHistoryInconsistent, e.QueueID, e.Stream, e.AppliedThrough, e.AckedThrough)
	}
	if e.ExpectedSeq > 0 {
		return fmt.Sprintf("%s: queue %q/%q needs seq %d before received seq %d, but durable receipt cursor is %d and applied cursor is %d; preserve the journal and inspect delivery state before recovery",
			ErrInboundHistoryInconsistent, e.QueueID, e.Stream, e.ExpectedSeq, e.Seq, e.AckedThrough, e.AppliedThrough)
	}
	return fmt.Sprintf("%s: queue %q/%q has received seq %d at or below applied cursor %d; preserve the journal and inspect delivery state before recovery",
		ErrInboundHistoryInconsistent, e.QueueID, e.Stream, e.Seq, e.AppliedThrough)
}

func (e *InboundHistoryError) Unwrap() error {
	return ErrInboundHistoryInconsistent
}
