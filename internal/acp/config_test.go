package acp

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestFlattenGroupedModelOptions(t *testing.T) {
	raw := json.RawMessage(`[
		{"name":"Gemini 3.8 Flash","value":"gemini-3.8-flash"},
		{"name":"Legacy","options":[
			{"name":"Gemini 3.7 Flash","value":"gemini-3.7-flash"},
			{"name":"Gemini 3.6 Flash","value":"gemini-3.6-flash"}
		]}
	]`)
	got, err := flattenSelectOptions(raw)
	if err != nil {
		t.Fatalf("flatten: %v", err)
	}
	wantValues := []string{"gemini-3.8-flash", "gemini-3.7-flash", "gemini-3.6-flash"}
	var values []string
	for _, o := range got {
		values = append(values, o.Value)
	}
	if !reflect.DeepEqual(values, wantValues) {
		t.Fatalf("values = %v, want %v", values, wantValues)
	}
}

func TestSessionConfigOptionDecodesModelSelector(t *testing.T) {
	raw := json.RawMessage(`{
		"id":"model","type":"select","category":"model","name":"Model",
		"currentValue":"gemini-3.7-flash",
		"options":[
			{"name":"Gemini 3.8 Flash","value":"gemini-3.8-flash"},
			{"name":"Legacy","options":[{"name":"Gemini 3.7 Flash","value":"gemini-3.7-flash"}]}
		]
	}`)
	var opt SessionConfigOption
	if err := json.Unmarshal(raw, &opt); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if opt.ID != "model" || opt.Type != "select" || opt.Category != "model" {
		t.Fatalf("identity = %+v", opt)
	}
	if opt.CurrentValue != "gemini-3.7-flash" {
		t.Fatalf("current = %q", opt.CurrentValue)
	}
	if got := ModelOptionValues([]SessionConfigOption{opt}); !reflect.DeepEqual(got, []string{"gemini-3.8-flash", "gemini-3.7-flash"}) {
		t.Fatalf("model values = %v", got)
	}
}

func TestBooleanOptionRequiresBool(t *testing.T) {
	var opt SessionConfigOption
	if err := json.Unmarshal([]byte(`{"id":"x","type":"boolean","value":true}`), &opt); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !opt.HasBoolValue || !opt.BoolValue {
		t.Fatalf("bool = %+v", opt)
	}
	var bad SessionConfigOption
	if err := json.Unmarshal([]byte(`{"id":"x","type":"boolean","value":"yes"}`), &bad); err == nil {
		t.Fatal("expected error for non-boolean value")
	}
}

func TestUnknownOptionShapeKeepsIdentity(t *testing.T) {
	var opt SessionConfigOption
	if err := json.Unmarshal([]byte(`{"id":"future","type":"slider","name":"F"}`), &opt); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if opt.ID != "future" || opt.Type != "slider" {
		t.Fatalf("identity = %+v", opt)
	}
}

func TestResolveModel(t *testing.T) {
	opts := []SessionConfigOption{{
		ID: "model", Type: "select", CurrentValue: "gemini-3.7-flash",
		Options: []SessionConfigSelectOption{
			{Value: "gemini-3.8-flash"},
			{Value: "gemini-3.7-flash"},
		},
	}}
	cases := []struct {
		name      string
		requested string
		def       string
		want      string
	}{
		{"explicit offered model wins", "gemini-3.8-flash", "gemini-3.8-flash", "gemini-3.8-flash"},
		{"unknown requested falls back to default", "nope", "gemini-3.8-flash", "gemini-3.8-flash"},
		{"unknown requested without default keeps current", "nope", "", "gemini-3.7-flash"},
		{"empty request uses default when offered", "", "gemini-3.8-flash", "gemini-3.8-flash"},
		{"empty request without default keeps current", "", "", "gemini-3.7-flash"},
		{"unoffered default keeps current", "", "gemini-9.9", "gemini-3.7-flash"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveModel(opts, tc.requested, tc.def); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	if got := ResolveModel(nil, "x", "y"); got != "" {
		t.Fatalf("no selector: got %q, want empty", got)
	}
}

func TestNewSessionCachesConfigOptions(t *testing.T) {
	toAgentR, toAgentW := io.Pipe()
	fromAgentR, fromAgentW := io.Pipe()
	c, _ := startFakeAgent(t, toAgentR, toAgentW, fromAgentR, fromAgentW)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := c.Initialize(ctx, "test", "test"); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sid, err := c.NewSession(ctx, "/tmp")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	opts := c.ConfigOptions(sid)
	if got := CurrentModel(opts); got != "gemini-3.7-flash" {
		t.Fatalf("current = %q, want gemini-3.7-flash", got)
	}
	if got := ModelOptionValues(opts); len(got) != 2 {
		t.Fatalf("model values = %v, want 2 flattened entries", got)
	}
	if got := ResolveModel(opts, "", "gemini-3.8-flash"); got != "gemini-3.8-flash" {
		t.Fatalf("default resolve = %q", got)
	}
	if got := ResolveModel(opts, "gemini-3.8-flash", ""); got != "gemini-3.8-flash" {
		t.Fatalf("explicit resolve = %q", got)
	}
	if got := c.ConfigOptions("unknown-session"); len(got) != 0 {
		t.Fatalf("unknown session: got %v, want empty", got)
	}
}
