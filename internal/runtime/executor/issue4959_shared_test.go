package executor

import (
	"encoding/base64"
	"testing"

	"github.com/tidwall/gjson"
)

// Shared #4959 fixtures: Responses history that starts with model turns
// needs a synthetic leading user turn. These helpers were formerly defined
// alongside the Antigravity REST executor tests and are still used by the
// Gemini/AIStudio executor tests.

func issue4959GeminiThoughtSignature() string {
	return "EjQKMgEMOdbHO0Gd+c9Mxk4ELwPGbpCEcp2mFfYYLix2UVtBH3fL8GECc4+JITVnHF4qZDsA"
}

// issue4959ResponsesModelFirstPayload is the #4959 Responses history: a
// next:function reasoning carrier, then function_call / function_call_output,
// then trailing assistant turns. Trailing model turns are left for a follow-up.
func issue4959ResponsesModelFirstPayload() []byte {
	carrier := "cpa-gemini-responses-carrier-v1:next:function:" + base64.RawStdEncoding.EncodeToString([]byte(issue4959GeminiThoughtSignature()))
	return []byte(`{"model":"gemini-3.7-flash-high","input":[` +
		`{"type":"reasoning","id":"rs_resp_test_detached_before_0","summary":[],"encrypted_content":"` + carrier + `"},` +
		`{"type":"function_call","call_id":"call_bash_1","name":"Bash","arguments":"{\"command\":\"true\"}"},` +
		`{"type":"function_call_output","call_id":"call_bash_1","output":"ok"},` +
		`{"role":"assistant","content":[{"type":"output_text","text":"first"}]},` +
		`{"role":"assistant","content":[{"type":"output_text","text":"second"}]}` +
		`]}`)
}

func contentHasNamedPart(content gjson.Result, partKind, name string) bool {
	for _, part := range content.Get("parts").Array() {
		if part.Get(partKind+".name").String() == name {
			return true
		}
	}
	return false
}

func assertIssue4959LeadingUserContents(t *testing.T, contents []gjson.Result) {
	t.Helper()
	if len(contents) < 3 {
		t.Fatalf("contents too short: %d", len(contents))
	}
	leadingText := contents[0].Get("parts.0.text")
	if contents[0].Get("role").String() != "user" || !leadingText.Exists() || leadingText.String() != "" {
		t.Fatalf("synthetic leading user missing: %s", contents[0].Raw)
	}
	if contents[1].Get("role").String() != "model" || !contentHasNamedPart(contents[1], "functionCall", "Bash") {
		t.Fatalf("function call is not immediately after the synthetic user: %s", contents[1].Raw)
	}
	if !contentHasNamedPart(contents[2], "functionResponse", "Bash") {
		t.Fatalf("function response missing or moved: %s", contents[2].Raw)
	}
}
