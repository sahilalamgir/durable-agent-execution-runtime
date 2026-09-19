// Package idempotency implements the Redis fence that guarantees a
// side-effecting tool is never executed twice for one invocation, even when
// the worker crashes and a recovering worker resumes the run (Phase 3).
package idempotency

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Key returns the Redis key for one tool invocation: "idem:{run_id}:{step}:
// {tool_use_id}". tool_use_id is fixed forever once LLMResponded is
// journaled, so a retry of the same invocation re-derives the same key while
// two identical tool_use blocks (two ids) get two keys (D-1). step is
// redundant with tool_use_id and kept for readability in redis-cli.
func Key(runID string, step int, toolUseID string) string {
	return fmt.Sprintf("idem:%s:%d:%s", runID, step, toolUseID)
}

// ArgsHash returns hex(sha256(toolName + "\x00" + canonicalJSON(args))). It
// is stored in the record and verified on read, not put in the key, so a
// future change to canonicalization is detected as a mismatch instead of
// silently yielding a different key and a duplicate execution (D-1).
func ArgsHash(toolName string, args json.RawMessage) (string, error) {
	canon, err := canonicalJSON(args)
	if err != nil {
		return "", fmt.Errorf("canonicalizing tool args: %w", err)
	}
	h := sha256.New()
	h.Write([]byte(toolName))
	h.Write([]byte{0})
	h.Write(canon)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// canonicalJSON re-encodes raw with sorted object keys and no insignificant
// whitespace. It decodes with UseNumber so integers never pass through
// float64 (2^53+1 would otherwise round to 2^53).
func canonicalJSON(raw json.RawMessage) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decoding json: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("decoding json: trailing data after the first value")
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	// encoding/json sorts map keys, and emits json.Number literals verbatim.
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("encoding json: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
