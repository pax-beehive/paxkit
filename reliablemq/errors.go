package reliablemq

import "errors"

var (
	ErrInvalidEnvelope      = errors.New("reliablemq: invalid envelope")
	ErrInvalidFrame         = errors.New("reliablemq: invalid frame")
	ErrInvalidDecision      = errors.New("reliablemq: invalid decision")
	ErrDuplicate            = errors.New("reliablemq: duplicate frame")
	ErrRejected             = errors.New("reliablemq: rejected by hook")
	ErrProducerNotReady     = errors.New("reliablemq: producer is not ready")
	ErrProducerClosed       = errors.New("reliablemq: producer is closed")
	ErrProducerDisconnected = errors.New("reliablemq: producer network writer is disconnected")
	ErrProducerJournalGap   = errors.New("reliablemq: producer journal has a sequence gap")
	ErrProducerJournalLimit = errors.New("reliablemq: producer journal safety limit exceeded")
)
