package auth

import (
	"context"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executionregistry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestHomeWebsocketReusesCanonicalModelSelection(t *testing.T) {
	dispatcher := &retainingHomeExecutionDispatcher{}
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{Home: internalconfig.HomeConfig{Enabled: true}})
	manager.PublishHomeDispatch(dispatcher, executionregistry.New(), 1)
	manager.RegisterExecutor(&retainingHomeExecutionExecutor{})
	t.Cleanup(func() { manager.CloseExecutionSession("canonical-model-session") })

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	opts := cliproxyexecutor.Options{Metadata: map[string]any{
		cliproxyexecutor.ExecutionSessionMetadataKey: "canonical-model-session",
		cliproxyexecutor.PinnedAuthMetadataKey:       "home-auth",
	}}
	for _, model := range []string{"model-a(high)", "model-a"} {
		if _, errExecute := manager.Execute(ctx, []string{"home-execution"}, cliproxyexecutor.Request{Model: model}, opts); errExecute != nil {
			t.Fatalf("Execute(%q) error = %v", model, errExecute)
		}
	}
	if got := dispatcher.calls.Load(); got != 1 {
		t.Fatalf("Home RPOP calls = %d, want 1 for one credential and canonical model", got)
	}
}
