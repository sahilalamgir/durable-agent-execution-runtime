package events

import "errors"

var (
	// ErrUnknownEventType is returned when an envelope's event_type isn't
	// one of the 9 known constants.
	ErrUnknownEventType = errors.New("unknown event type")
	// ErrMissingField is returned when a required envelope field is absent
	// from the JSON entirely (not merely zero-valued).
	ErrMissingField = errors.New("missing required field")
	// ErrInvalidPayload is returned when a payload's JSON doesn't unmarshal
	// into its event type's struct.
	ErrInvalidPayload = errors.New("invalid payload")
)
