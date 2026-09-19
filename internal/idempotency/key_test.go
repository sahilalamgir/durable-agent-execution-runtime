package idempotency

import (
	"encoding/json"
	"testing"
)

func TestArgsHash(t *testing.T) {
	hash := func(tool, args string) string {
		t.Helper()
		h, err := ArgsHash(tool, json.RawMessage(args))
		if err != nil {
			t.Fatalf("ArgsHash(%q, %s) error = %v", tool, args, err)
		}
		return h
	}
	tests := []struct {
		name string
		a, b [2]string // tool, args
		same bool
	}{
		{"key order does not matter", [2]string{"t", `{"a":1,"b":2}`}, [2]string{"t", `{"b":2,"a":1}`}, true},
		{"whitespace does not matter", [2]string{"t", `{"a": 1,  "b": [1, 2]}`}, [2]string{"t", `{"a":1,"b":[1,2]}`}, true},
		{"nested key order does not matter", [2]string{"t", `{"o":{"x":1,"y":2}}`}, [2]string{"t", `{"o":{"y":2,"x":1}}`}, true},
		{"integers are not rounded through float64", [2]string{"t", `{"n": 9007199254740993}`}, [2]string{"t", `{"n": 9007199254740992}`}, false},
		{"tool name is part of the hash", [2]string{"open_pr", `{}`}, [2]string{"apply_fix", `{}`}, false},
		{"different values differ", [2]string{"t", `{"a":1}`}, [2]string{"t", `{"a":2}`}, false},
		{"html characters are not escaped differently", [2]string{"t", `{"a":"<b>&"}`}, [2]string{"t", `{"a":"\u003cb\u003e\u0026"}`}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hash(tt.a[0], tt.a[1]) == hash(tt.b[0], tt.b[1]); got != tt.same {
				t.Fatalf("hashes equal = %v, want %v", got, tt.same)
			}
		})
	}
}

func TestArgsHashRejectsBadJSON(t *testing.T) {
	for _, args := range []string{``, `{`, `{"a":1} {"b":2}`} {
		if _, err := ArgsHash("t", json.RawMessage(args)); err == nil {
			t.Errorf("ArgsHash(%q) returned nil error", args)
		}
	}
}

func TestKey(t *testing.T) {
	tests := []struct {
		name string
		run  string
		step int
		id   string
		want string
	}{
		{name: "format", run: "7f1c", step: 5, id: "toolu_05E", want: "idem:7f1c:5:toolu_05E"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Key(tt.run, tt.step, tt.id); got != tt.want {
				t.Fatalf("Key() = %q, want %q", got, tt.want)
			}
		})
	}
	if Key("r", 1, "toolu_a") == Key("r", 1, "toolu_b") {
		t.Fatal("different tool_use_ids must give different keys (two identical tool_use blocks)")
	}
	if Key("r", 1, "toolu_a") != Key("r", 1, "toolu_a") {
		t.Fatal("the same invocation must always derive the same key")
	}
}
