package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestCodexBootstrapTimeoutRotatesAffinityAcrossPriorities(t *testing.T) {
	writeCompleted := func(w http.ResponseWriter, responseID string) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":%q,\"object\":\"response\",\"created_at\":1775555723,\"status\":\"completed\",\"model\":\"gpt-5.5\",\"output\":[],\"usage\":{\"input_tokens\":8,\"output_tokens\":4,\"total_tokens\":12}}}\n\n", responseID)
	}

	var stalledCalls atomic.Int32
	var stallEnabled atomic.Bool
	stalledCanceled := make(chan struct{})
	stalledServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stalledCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		if !stallEnabled.Load() {
			writeCompleted(w, "resp_affinity_bound")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(stalledCanceled)
	}))
	defer stalledServer.Close()

	var healthyCalls atomic.Int32
	healthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthyCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		writeCompleted(w, "resp_healthy")
	}))
	defer healthyServer.Close()

	cfg := &sdkconfig.Config{SDKConfig: sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{
		BootstrapTimeoutSeconds: 1,
	}}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewCodexExecutor(cfg))
	selector := coreauth.NewSessionAffinitySelector(&coreauth.RoundRobinSelector{})
	manager.SetSelector(selector)
	t.Cleanup(selector.Stop)

	auths := []*coreauth.Auth{
		{
			ID:       "z-codex-bootstrap-priority-10",
			Provider: "codex",
			Status:   coreauth.StatusActive,
			Attributes: map[string]string{
				"api_key":  "test-stalled",
				"base_url": stalledServer.URL,
				"priority": "0",
			},
		},
		{
			ID:       "a-codex-bootstrap-priority-5",
			Provider: "codex",
			Status:   coreauth.StatusActive,
			Attributes: map[string]string{
				"api_key":  "test-healthy",
				"base_url": healthyServer.URL,
				"priority": "0",
			},
		},
	}
	for _, auth := range auths {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("manager.Register(%s): %v", auth.ID, errRegister)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gpt-5.5"}})
		authID := auth.ID
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	}

	handler := NewBaseAPIHandlers(&cfg.SDKConfig, manager)
	warmupBody := []byte(`{"model":"gpt-5.5","input":"Warm up","stream":true,"conversation_id":"affinity-cursor-warmup"}`)
	requestBody := []byte(`{"model":"gpt-5.5","input":"Say ok","stream":true,"conversation_id":"sticky-bootstrap-timeout"}`)
	priorityProbeBody := []byte(`{"model":"gpt-5.5","input":"Probe priority","stream":true,"conversation_id":"priority-selection-probe"}`)
	executeSuccessfulRequest := func(label string, body []byte) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		data, _, errs := handler.ExecuteStreamWithAuthManager(ctx, "openai-response", "gpt-5.5", body, "")
		var response []byte
		for chunk := range data {
			response = append(response, chunk...)
		}
		for errMessage := range errs {
			if errMessage != nil {
				t.Fatalf("%s unexpected terminal error: status=%d err=%v", label, errMessage.StatusCode, errMessage.Error)
			}
		}
		if !bytes.Contains(response, []byte("response.completed")) {
			t.Fatalf("%s completion missing from body %q", label, response)
		}
	}

	executeSuccessfulRequest("warmup", warmupBody)
	executeSuccessfulRequest("affinity bind", requestBody)
	executeSuccessfulRequest("affinity hit", requestBody)
	if got := healthyCalls.Load(); got != 1 {
		t.Fatalf("healthy auth preflight calls = %d, want 1", got)
	}
	if got := stalledCalls.Load(); got != 2 {
		t.Fatalf("affinity-bound auth preflight calls = %d, want 2", got)
	}

	auths[0].Attributes["priority"] = "10"
	auths[1].Attributes["priority"] = "5"
	for _, auth := range auths {
		if _, errUpdate := manager.Update(context.Background(), auth); errUpdate != nil {
			t.Fatalf("manager.Update(%s): %v", auth.ID, errUpdate)
		}
	}
	stalledCalls.Store(0)
	healthyCalls.Store(0)
	executeSuccessfulRequest("priority probe", priorityProbeBody)
	if got := stalledCalls.Load(); got != 1 {
		t.Fatalf("higher-priority auth calls during priority probe = %d, want 1", got)
	}
	if got := healthyCalls.Load(); got != 0 {
		t.Fatalf("lexically earlier lower-priority auth calls during priority probe = %d, want 0", got)
	}

	stalledCalls.Store(0)
	healthyCalls.Store(0)
	stallEnabled.Store(true)

	requestCtx, cancelRequest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRequest()

	started := time.Now()
	data, _, errs := handler.ExecuteStreamWithAuthManager(
		requestCtx,
		"openai-response",
		"gpt-5.5",
		requestBody,
		"",
	)
	var body []byte
	for chunk := range data {
		body = append(body, chunk...)
	}
	for errMessage := range errs {
		if errMessage != nil {
			t.Fatalf("unexpected terminal error: status=%d err=%v", errMessage.StatusCode, errMessage.Error)
		}
	}
	elapsed := time.Since(started)

	if !bytes.Contains(body, []byte("response.completed")) {
		t.Fatalf("healthy completion missing from body %q", body)
	}
	if got := stalledCalls.Load(); got != 1 {
		t.Fatalf("stalled auth calls = %d, want 1", got)
	}
	if got := healthyCalls.Load(); got != 1 {
		t.Fatalf("healthy auth calls = %d, want 1", got)
	}
	select {
	case <-stalledCanceled:
	case <-time.After(time.Second):
		t.Fatal("stalled auth request context was not canceled")
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("request recovered before configured timeout: %s", elapsed)
	}
	if elapsed >= 4*time.Second {
		t.Fatalf("request recovery took too long: %s", elapsed)
	}
}
