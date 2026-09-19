package replay

import (
	"reflect"
	"testing"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// TestFoldCrashPoints is AC-2: one row per named crash point, each checking
// Status, CurrentStep, NextSequence, NextAction, InFlightTool,
// PendingToolCalls, and Messages()'s role/block-type shape.
func TestFoldCrashPoints(t *testing.T) {
	tests := []struct {
		name             string
		buildEvents      func(t *testing.T) []events.Envelope
		wantStatus       RunStatus
		wantCurrentStep  int
		wantNextSeq      int64
		wantNextAction   NextAction
		wantPendingIDs   []string // tool_use_ids, nil means none
		wantInFlightID   string   // "" means nil
		wantMessageShape []string
	}{
		{
			name: "after RunStarted",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
				}
			},
			wantStatus:       StatusRunning,
			wantCurrentStep:  0,
			wantNextSeq:      1,
			wantNextAction:   ActionCallLLM,
			wantMessageShape: []string{"user:text"},
		},
		{
			name: "after LLMResponded with 1 tool",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("I'll clone it.", [3]string{"toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`}),
						false),
				}
			},
			wantStatus:       StatusRunning,
			wantCurrentStep:  1,
			wantNextSeq:      2,
			wantNextAction:   ActionExecuteTool,
			wantPendingIDs:   []string{"toolu_1"},
			wantMessageShape: []string{"user:text", "assistant:text,tool_use"},
		},
		{
			name: "after ToolInvoked",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("I'll clone it.", [3]string{"toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`}),
						false),
					fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`, false),
				}
			},
			wantStatus:       StatusRunning,
			wantCurrentStep:  1,
			wantNextSeq:      3,
			wantNextAction:   ActionResolveInFlightTool,
			wantInFlightID:   "toolu_1",
			wantMessageShape: []string{"user:text", "assistant:text,tool_use"},
		},
		{
			name: "mid-batch of 2 tools",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("",
							[3]string{"toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`},
							[3]string{"toolu_2", "run_tests", `{}`},
						),
						false),
					fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`, false),
					fxToolResulted(t, testRunID, 3, 1, "toolu_1", "clone_repo", "cloned ok", "success"),
				}
			},
			wantStatus:       StatusRunning,
			wantCurrentStep:  1,
			wantNextSeq:      4,
			wantNextAction:   ActionExecuteTool,
			wantPendingIDs:   []string{"toolu_2"},
			wantMessageShape: []string{"user:text", "assistant:tool_use,tool_use"},
		},
		{
			name: "after the last ToolResulted with CurrentStep < MaxSteps",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("", [3]string{"toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`}),
						false),
					fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`, false),
					fxToolResulted(t, testRunID, 3, 1, "toolu_1", "clone_repo", "cloned ok", "success"),
				}
			},
			wantStatus:       StatusRunning,
			wantCurrentStep:  1,
			wantNextSeq:      4,
			wantNextAction:   ActionCallLLM,
			wantMessageShape: []string{"user:text", "assistant:tool_use", "user:tool_result"},
		},
		{
			name: "after the last ToolResulted with CurrentStep == MaxSteps",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 1, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("", [3]string{"toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`}),
						false),
					fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`, false),
					fxToolResulted(t, testRunID, 3, 1, "toolu_1", "clone_repo", "cloned ok", "success"),
				}
			},
			wantStatus:       StatusRunning,
			wantCurrentStep:  1,
			wantNextSeq:      4,
			wantNextAction:   ActionFailMaxSteps,
			wantMessageShape: []string{"user:text", "assistant:tool_use", "user:tool_result"},
		},
		{
			name: "after a terminal LLMResponded",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "end_turn", fxTextContent("all done"), true),
				}
			},
			wantStatus:       StatusRunning,
			wantCurrentStep:  1,
			wantNextSeq:      2,
			wantNextAction:   ActionFinishRun,
			wantMessageShape: []string{"user:text", "assistant:text"},
		},
		{
			name: "after RunCompleted",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "end_turn", fxTextContent("all done"), true),
					fxRunCompleted(t, testRunID, 2, "all done", 1),
				}
			},
			wantStatus:       StatusCompleted,
			wantCurrentStep:  1,
			wantNextSeq:      3,
			wantNextAction:   ActionNone,
			wantMessageShape: []string{"user:text", "assistant:text"},
		},
		{
			name: "after RunFailed",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 1, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("", [3]string{"toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`}),
						false),
					fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`, false),
					fxToolResulted(t, testRunID, 3, 1, "toolu_1", "clone_repo", "cloned ok", "success"),
					fxRunFailed(t, testRunID, 4, 1, "max_steps_exceeded", "exceeded max_steps=1", false),
				}
			},
			wantStatus:       StatusFailed,
			wantCurrentStep:  1,
			wantNextSeq:      5,
			wantNextAction:   ActionNone,
			wantMessageShape: []string{"user:text", "assistant:tool_use", "user:tool_result"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Fold(tt.buildEvents(t))
			if err != nil {
				t.Fatalf("Fold() error = %v", err)
			}

			if s.Status != tt.wantStatus {
				t.Errorf("Status = %v, want %v", s.Status, tt.wantStatus)
			}
			if s.CurrentStep != tt.wantCurrentStep {
				t.Errorf("CurrentStep = %d, want %d", s.CurrentStep, tt.wantCurrentStep)
			}
			if s.NextSequence != tt.wantNextSeq {
				t.Errorf("NextSequence = %d, want %d", s.NextSequence, tt.wantNextSeq)
			}
			if s.NextAction != tt.wantNextAction {
				t.Errorf("NextAction = %v, want %v", s.NextAction, tt.wantNextAction)
			}

			var gotPendingIDs []string
			for _, tc := range s.PendingToolCalls {
				gotPendingIDs = append(gotPendingIDs, tc.ToolUseID)
			}
			if !reflect.DeepEqual(gotPendingIDs, tt.wantPendingIDs) {
				t.Errorf("PendingToolCalls tool_use_ids = %v, want %v", gotPendingIDs, tt.wantPendingIDs)
			}

			gotInFlightID := ""
			if s.InFlightTool != nil {
				gotInFlightID = s.InFlightTool.ToolUseID
			}
			if gotInFlightID != tt.wantInFlightID {
				t.Errorf("InFlightTool tool_use_id = %q, want %q", gotInFlightID, tt.wantInFlightID)
			}

			msgs, err := s.Messages()
			if err != nil {
				t.Fatalf("Messages() error = %v", err)
			}
			gotShape := messageShape(msgs)
			if !reflect.DeepEqual(gotShape, tt.wantMessageShape) {
				t.Errorf("Messages() shape = %v, want %v", gotShape, tt.wantMessageShape)
			}
		})
	}
}
