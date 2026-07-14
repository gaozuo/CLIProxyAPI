package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func TestCodexBootstrapTimeoutBounds(t *testing.T) {
	if got := codexBootstrapTimeout(nil); got != 0 {
		t.Fatalf("nil config timeout = %s, want 0", got)
	}
	if got := codexBootstrapTimeout(&config.Config{SDKConfig: config.SDKConfig{Streaming: config.StreamingConfig{BootstrapTimeoutSeconds: 20}}}); got != 20*time.Second {
		t.Fatalf("timeout = %s, want 20s", got)
	}
	if got := codexBootstrapTimeout(&config.Config{SDKConfig: config.SDKConfig{Streaming: config.StreamingConfig{BootstrapTimeoutSeconds: 9999}}}); got != 10*time.Minute {
		t.Fatalf("capped timeout = %s, want 10m", got)
	}
}

func newCodexBootstrapTestExecutor(serverURL string) (*CodexExecutor, *cliproxyauth.Auth) {
	cfg := &config.Config{SDKConfig: config.SDKConfig{Streaming: config.StreamingConfig{BootstrapTimeoutSeconds: 1}}}
	return NewCodexExecutor(cfg), &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": serverURL,
		"api_key":  "test",
	}}
}

func executeCodexBootstrapTestStream(ctx context.Context, executor *CodexExecutor, auth *cliproxyauth.Auth) (*cliproxyexecutor.StreamResult, error) {
	return executor.ExecuteStream(ctx, auth, cliproxyexecutor.Request{
		Model:   "gpt-5.5",
		Payload: []byte(`{"model":"gpt-5.5","input":"Say ok"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Stream:       true,
	})
}

func TestCodexExecutorBootstrapTimeoutBeforeHeaders(t *testing.T) {
	canceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		close(canceled)
	}))
	defer server.Close()

	executor, auth := newCodexBootstrapTestExecutor(server.URL)
	_, err := executeCodexBootstrapTestStream(context.Background(), executor, auth)
	if err == nil {
		t.Fatal("expected bootstrap timeout")
	}
	statusErr, ok := err.(interface{ StatusCode() int })
	if !ok || statusErr.StatusCode() != http.StatusGatewayTimeout {
		t.Fatalf("error = %T %v, want HTTP 504", err, err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("upstream request was not canceled")
	}
}

func TestCodexExecutorBootstrapTimeoutAfterHeaders(t *testing.T) {
	canceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	}))
	defer server.Close()

	executor, auth := newCodexBootstrapTestExecutor(server.URL)
	result, err := executeCodexBootstrapTestStream(context.Background(), executor, auth)
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	select {
	case chunk := <-result.Chunks:
		statusErr, ok := chunk.Err.(interface{ StatusCode() int })
		if !ok || statusErr.StatusCode() != http.StatusGatewayTimeout {
			t.Fatalf("chunk error = %T %v, want HTTP 504", chunk.Err, chunk.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for bootstrap error")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("upstream request was not canceled")
	}
}

func TestCodexExecutorBootstrapTimeoutLeavesParentContextActive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer server.Close()

	executor, auth := newCodexBootstrapTestExecutor(server.URL)
	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	_, err := executeCodexBootstrapTestStream(parentCtx, executor, auth)
	if err == nil {
		t.Fatal("expected bootstrap timeout")
	}
	statusErr, ok := err.(interface{ StatusCode() int })
	if !ok || statusErr.StatusCode() != http.StatusGatewayTimeout {
		t.Fatalf("error = %T %v, want HTTP 504", err, err)
	}
	if errParent := parentCtx.Err(); errParent != nil {
		t.Fatalf("parent context error = %v, want nil", errParent)
	}
}

func TestCodexExecutorBootstrapNon2xxPreservesStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		w.(http.Flusher).Flush()
		time.Sleep(1500 * time.Millisecond)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
	}))
	defer server.Close()

	executor, auth := newCodexBootstrapTestExecutor(server.URL)
	_, err := executeCodexBootstrapTestStream(context.Background(), executor, auth)
	if err == nil {
		t.Fatal("expected non-2xx error")
	}
	statusErr, ok := err.(interface{ StatusCode() int })
	if !ok || statusErr.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("error = %T %v, want HTTP 429", err, err)
	}
}

func TestCodexExecutorBootstrapCleanEmptyStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
	}))
	defer server.Close()

	executor, auth := newCodexBootstrapTestExecutor(server.URL)
	result, err := executeCodexBootstrapTestStream(context.Background(), executor, auth)
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("unexpected stream error: %v", chunk.Err)
		}
		if len(chunk.Payload) > 0 {
			t.Fatalf("unexpected stream payload: %q", chunk.Payload)
		}
	}
}

func TestCodexExecutorBootstrapObservesRawDataBeforeIdentityTransform(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5.5\"}}\n\n"))
		w.(http.Flusher).Flush()
		time.Sleep(2200 * time.Millisecond)
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"output\":[]}}\n\n"))
	}))
	defer server.Close()

	cfg := &config.Config{SDKConfig: config.SDKConfig{Streaming: config.StreamingConfig{BootstrapTimeoutSeconds: 1}}}
	cfg.Codex.IdentityConfuse = true
	cfg.Routing.SessionAffinity = true
	executor := NewCodexExecutor(cfg)
	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.5",
		Payload: []byte(`{"model":"gpt-5.5","input":"Say ok","prompt_cache_key":"data:"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Stream:       true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	var payload []byte
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("unexpected stream error: %v", chunk.Err)
		}
		payload = append(payload, chunk.Payload...)
	}
	if !bytes.Contains(payload, []byte("response.completed")) {
		t.Fatalf("completed event missing from %q", payload)
	}
}

func TestCodexExecutorBootstrapTimeoutDisarmsAfterFirstEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5.5\"}}\n\n"))
		w.(http.Flusher).Flush()
		time.Sleep(2500 * time.Millisecond)
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"output\":[]}}\n\n"))
	}))
	defer server.Close()

	executor, auth := newCodexBootstrapTestExecutor(server.URL)
	result, err := executeCodexBootstrapTestStream(context.Background(), executor, auth)
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	var payload []byte
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("unexpected stream error: %v", chunk.Err)
		}
		payload = append(payload, chunk.Payload...)
	}
	if !bytes.Contains(payload, []byte("response.completed")) {
		t.Fatalf("completed event missing from %q", payload)
	}
}
