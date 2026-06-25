package reliablemq

import "errors"

var (
	ErrInvalidEnvelope = errors.New("reliablemq: invalid envelope")
	ErrInvalidFrame    = errors.New("reliablemq: invalid frame")
	ErrInvalidDecision = errors.New("reliablemq: invalid decision")
	ErrDuplicate       = errors.New("reliablemq: duplicate frame")
	ErrRejected        = errors.New("reliablemq: rejected by hook")
)
