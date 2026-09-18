package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// requiredEnvelopeFields lists the envelope fields Decode requires to be
// present in the raw JSON (not merely non-zero after unmarshaling). FR-3.
var requiredEnvelopeFields = []string{"event_id", "run_id", "event_type", "occurred_at"}

// Envelope is the run-events message contract: one immutable record per
// step of a run. Payload is carried as raw JSON and decoded into its typed
// struct by DecodePayload, keyed off EventType.
type Envelope struct {
	EventID        string          `json:"event_id"`        // UUIDv4
	RunID          string          `json:"run_id"`          // UUIDv4
	TenantID       string          `json:"tenant_id"`       // Phase 2: constant "local-dev"
	SequenceNumber int64           `json:"sequence_number"` // 0-based, gapless per run
	EventType      EventType       `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	OccurredAt     time.Time       `json:"occurred_at"`     // UTC, truncated to µs
	IdempotencyKey *string         `json:"idempotency_key"` // always null in Phase 2
}

// NewEnvelope builds an Envelope around p: it assigns a fresh event_id,
// marshals p as the payload, and truncates now to UTC microsecond
// precision (FR-4), so a round trip through Postgres's TIMESTAMPTZ leaves
// occurred_at unchanged (EC-12).
func NewEnvelope(runID, tenantID string, seq int64, p Payload, now time.Time) (Envelope, error) {
	payloadBytes, err := json.Marshal(p)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshaling %s payload: %w", p.EventType(), err)
	}
	return Envelope{
		EventID:        uuid.New().String(),
		RunID:          runID,
		TenantID:       tenantID,
		SequenceNumber: seq,
		EventType:      p.EventType(),
		Payload:        payloadBytes,
		OccurredAt:     now.UTC().Truncate(time.Microsecond),
		IdempotencyKey: nil,
	}, nil
}

// Encode marshals e as the JSON that goes on the wire as a run-events
// message value.
func Encode(e Envelope) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encoding envelope: %w", err)
	}
	return b, nil
}

// Decode parses b into an Envelope, validating the required envelope fields
// and the event_type. It distinguishes a field absent from the JSON from one
// present but zero-valued (e.g. a missing occurred_at vs. the zero
// time.Time) by checking field presence in a raw map pass before the typed
// unmarshal — a naive unmarshal-then-IsZero check cannot tell those apart.
func Decode(b []byte) (Envelope, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return Envelope{}, fmt.Errorf("decoding envelope: %w", err)
	}
	for _, field := range requiredEnvelopeFields {
		v, present := raw[field]
		if !present || string(v) == "null" {
			return Envelope{}, fmt.Errorf("decoding envelope: field %q: %w", field, ErrMissingField)
		}
	}

	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("decoding envelope: %w", err)
	}
	if !knownEventTypes[e.EventType] {
		return Envelope{}, fmt.Errorf("decoding envelope: %q: %w", e.EventType, ErrUnknownEventType)
	}
	return e, nil
}

// DecodePayload unmarshals e.Payload into its event type's struct, switching
// on e.EventType. It returns ErrInvalidPayload if the payload JSON doesn't
// match that type's shape, and ErrUnknownEventType if e.EventType isn't one
// of the 9 known constants (Decode should already have rejected that, but
// DecodePayload re-checks so it is safe to call on hand-built envelopes too).
func DecodePayload(e Envelope) (Payload, error) {
	switch e.EventType {
	case EventRunStarted:
		var p RunStartedPayload
		return p, unmarshalPayload(e.EventType, e.Payload, &p)
	case EventLLMResponded:
		var p LLMRespondedPayload
		return p, unmarshalPayload(e.EventType, e.Payload, &p)
	case EventToolInvoked:
		var p ToolInvokedPayload
		return p, unmarshalPayload(e.EventType, e.Payload, &p)
	case EventToolResulted:
		var p ToolResultedPayload
		return p, unmarshalPayload(e.EventType, e.Payload, &p)
	case EventAwaitingApproval:
		var p AwaitingApprovalPayload
		return p, unmarshalPayload(e.EventType, e.Payload, &p)
	case EventRunApproved:
		var p RunApprovedPayload
		return p, unmarshalPayload(e.EventType, e.Payload, &p)
	case EventRunCompleted:
		var p RunCompletedPayload
		return p, unmarshalPayload(e.EventType, e.Payload, &p)
	case EventRunFailed:
		var p RunFailedPayload
		return p, unmarshalPayload(e.EventType, e.Payload, &p)
	case EventRunCancelled:
		var p RunCancelledPayload
		return p, unmarshalPayload(e.EventType, e.Payload, &p)
	default:
		return nil, fmt.Errorf("decoding payload: %q: %w", e.EventType, ErrUnknownEventType)
	}
}

// unmarshalPayload is a helper so DecodePayload's switch stays one line per
// case. On unmarshal failure, it wraps the underlying json error together
// with ErrInvalidPayload so callers can errors.Is against the sentinel
// without losing the specific cause.
func unmarshalPayload(t EventType, raw json.RawMessage, dst Payload) error {
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decoding %s payload: %w", t, fmt.Errorf("%w: %v", ErrInvalidPayload, err))
	}
	return nil
}
