package acp

import (
	"encoding/json"
	"testing"
)

func TestCancelledPermissionResultShape(t *testing.T) {
	raw := cancelledPermissionResult()
	if !json.Valid(raw) {
		t.Fatalf("cancelledPermissionResult is not valid JSON: %q", string(raw))
	}
	var out struct {
		Outcome struct {
			Outcome string `json:"outcome"`
		} `json:"outcome"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode permission denial: %v", err)
	}
	if out.Outcome.Outcome != "cancelled" {
		t.Fatalf("outcome = %q, want cancelled", out.Outcome.Outcome)
	}
}

func TestWireMessageClassification(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		isResp  bool
		isReq   bool
		isNotif bool
	}{
		{"numeric id response", `{"jsonrpc":"2.0","id":1,"result":{}}`, true, false, false},
		{"string id error response", `{"jsonrpc":"2.0","id":"a","error":{"code":-32601,"message":"x"}}`, true, false, false},
		{"result null is still a response", `{"jsonrpc":"2.0","id":2,"result":null}`, true, false, false},
		{"request", `{"jsonrpc":"2.0","id":3,"method":"session/request_permission","params":{}}`, false, true, false},
		{"notification", `{"jsonrpc":"2.0","method":"session/update","params":{}}`, false, false, true},
		{"null id is a notification", `{"jsonrpc":"2.0","id":null,"method":"session/update"}`, false, false, true},
		{"id without method is neither", `{"jsonrpc":"2.0","id":4}`, false, false, false},
		{"method without id is a notification", `{"jsonrpc":"2.0","method":"x/test"}`, false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m wireMessage
			if err := json.Unmarshal([]byte(tc.line), &m); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got := m.isResponse(); got != tc.isResp {
				t.Errorf("isResponse = %v, want %v", got, tc.isResp)
			}
			if got := m.isRequest(); got != tc.isReq {
				t.Errorf("isRequest = %v, want %v", got, tc.isReq)
			}
			if got := m.isNotification(); got != tc.isNotif {
				t.Errorf("isNotification = %v, want %v", got, tc.isNotif)
			}
		})
	}
}

func TestInitializeRequestCarriesExplicitFalseCapabilities(t *testing.T) {
	raw, err := json.Marshal(initializeRequest{
		ProtocolVersion:    protocolVersion,
		ClientCapabilities: defaultClientCapabilities(),
		ClientInfo:         clientInfo{Name: "test", Version: "test"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	caps, ok := decoded["clientCapabilities"].(map[string]any)
	if !ok {
		t.Fatalf("clientCapabilities missing: %s", raw)
	}
	fs, ok := caps["fs"].(map[string]any)
	if !ok {
		t.Fatalf("clientCapabilities.fs missing: %s", raw)
	}
	// Explicit false must be present on the wire, not omitted.
	if v, ok := fs["readTextFile"]; !ok || v != false {
		t.Fatalf("readTextFile = %v (present=%v), want explicit false", v, ok)
	}
	if v, ok := fs["writeTextFile"]; !ok || v != false {
		t.Fatalf("writeTextFile = %v (present=%v), want explicit false", v, ok)
	}
	if v, ok := caps["terminal"]; !ok || v != false {
		t.Fatalf("terminal = %v (present=%v), want explicit false", v, ok)
	}
}
