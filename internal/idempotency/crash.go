package idempotency

import (
	"fmt"
	"strings"
	"sync"
)

// CrashPoint is a spot around a side-effecting tool call where crash
// injection (DAE_CRASH_AT) can kill the worker.
type CrashPoint string

const (
	// CrashAfterClaim: claimed in Redis, Execute not yet called.
	CrashAfterClaim CrashPoint = "after_claim"
	// CrashAfterExecute: Execute returned, key not yet resolved.
	CrashAfterExecute CrashPoint = "after_execute"
	// CrashAfterResolve: key resolved, ToolResulted not yet published.
	CrashAfterResolve CrashPoint = "after_resolve"
)

// CrashHook is called by the Guard at each CrashPoint.
type CrashHook interface {
	At(toolName string, point CrashPoint)
}

type noCrash struct{}

func (noCrash) At(string, CrashPoint) {}

// exitCrashCode is the exit status of an injected crash (128+SIGKILL).
const exitCrashCode = 137

// exitCrash exits the first time toolName reaches point.
type exitCrash struct {
	tool  string
	point CrashPoint
	exit  func(code int)
	once  sync.Once
}

func (c *exitCrash) At(toolName string, point CrashPoint) {
	if toolName != c.tool || point != c.point {
		return
	}
	c.once.Do(func() { c.exit(exitCrashCode) })
}

// ParseCrashSpec parses DAE_CRASH_AT ("<tool>:<point>") into a CrashHook
// that calls exit(137) the first time that tool reaches that point in this
// process. An empty spec returns a hook that never crashes. exit is a
// parameter so tests don't kill the test binary; the worker passes os.Exit.
func ParseCrashSpec(spec string, exit func(code int)) (CrashHook, error) {
	if spec == "" {
		return noCrash{}, nil
	}
	tool, pointStr, ok := strings.Cut(spec, ":")
	point := CrashPoint(pointStr)
	if !ok || tool == "" {
		return nil, fmt.Errorf("parsing crash spec %q: want <tool>:<point>", spec)
	}
	switch point {
	case CrashAfterClaim, CrashAfterExecute, CrashAfterResolve:
	default:
		return nil, fmt.Errorf("parsing crash spec %q: unknown point %q (want after_claim, after_execute or after_resolve)", spec, pointStr)
	}
	return &exitCrash{tool: tool, point: point, exit: exit}, nil
}
