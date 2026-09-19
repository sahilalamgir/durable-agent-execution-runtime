package idempotency

import "time"

// RecordState is the value of IdemRecord.State. A key moves absent → claimed
// → resolved and never backwards.
type RecordState string

const (
	StateClaimed  RecordState = "claimed"
	StateResolved RecordState = "resolved"
)

// IdemRecord is the JSON value stored under an idempotency key.
type IdemRecord struct {
	State     RecordState `json:"state"`
	RunID     string      `json:"run_id"`
	Step      int         `json:"step"`
	ToolUseID string      `json:"tool_use_id"`
	ToolName  string      `json:"tool_name"`
	ArgsHash  string      `json:"args_hash"`
	// ClaimToken is a UUIDv4, fresh per claim/takeover attempt. Resolve and
	// TakeOver only succeed for the holder of the current token.
	ClaimToken string `json:"claim_token"`
	// ClaimedBy is "<hostname>-<pid>".
	ClaimedBy  string     `json:"claimed_by"`
	ClaimedAt  time.Time  `json:"claimed_at"` // UTC, µs
	ResolvedAt *time.Time `json:"resolved_at"`
	Result     *string    `json:"result"`     // set when resolved
	Status     *string    `json:"status"`     // "success" | "error", set when resolved
	Resolution *string    `json:"resolution"` // "executed" | "reconciled" | "reexecuted", set when resolved
}
