# Codex Bootstrap Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in Codex HTTP/SSE first-event timeout that cancels only the stalled credential attempt and reuses existing AuthManager failover.

**Architecture:** Add the PR #4303-compatible configuration field, but enforce it inside `CodexExecutor.ExecuteStream` rather than around `AuthManager.ExecuteStream`. Use `context.WithCancelCause`, a standard timer, and an atomic pending/observed/expired state so the timeout becomes a normal 504 credential failure handled by existing `MarkResult` and request-local `tried` logic.

**Tech Stack:** Go 1.26+, `context`, `sync/atomic`, `net/http`, `httptest`, existing CLIProxyAPI AuthManager and executor test infrastructure.

## Global Constraints

- Start from official CLIProxyAPI `main` commit `411d7d41eee0ff841b5badba5abacad6c11331ef`.
- Keep the timeout disabled when `streaming.bootstrap-timeout-seconds <= 0`.
- Cap the configured timeout at 600 seconds.
- Apply the timeout only to Codex HTTP/SSE before the first non-empty upstream `data:` event.
- Do not modify New API, WebSocket behavior, selectors, affinity cache code, or handler retry architecture.
- Use only standard-library concurrency primitives; do not add dependencies or a custom context type.
- Follow TDD: every production behavior change must be preceded by a failing test.
- Run `gofmt` after Go changes and build `./cmd/server` before completion.

---

### Task 1: Add the bounded configuration surface

**Files:**
- Modify: `internal/config/sdk_config.go`
- Modify: `config.example.yaml`
- Create: `internal/runtime/executor/codex_bootstrap_timeout_test.go`
- Modify: `internal/runtime/executor/codex_executor.go`

**Interfaces:**
- Produces: `StreamingConfig.BootstrapTimeoutSeconds int`
- Produces: `codexBootstrapTimeout(*config.Config) time.Duration`
- Produces: `codexBootstrapTimeoutError(time.Duration) statusErr`

- [ ] **Step 1: Write the failing bounds test**

```go
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
```

- [ ] **Step 2: Run the test and verify RED**

Run: `go test ./internal/runtime/executor -run '^TestCodexBootstrapTimeoutBounds$' -count=1`

Expected: compilation fails because `BootstrapTimeoutSeconds` and `codexBootstrapTimeout` do not exist.

- [ ] **Step 3: Add the minimal configuration and helpers**

Add to `StreamingConfig`:

```go
// BootstrapTimeoutSeconds limits how long a Codex HTTP/SSE stream may wait for
// its first non-empty upstream data event. <= 0 disables the timeout. Values
// above 600 are capped at 600 seconds.
BootstrapTimeoutSeconds int `yaml:"bootstrap-timeout-seconds,omitempty" json:"bootstrap-timeout-seconds,omitempty"`
```

Add to `codex_executor.go`:

```go
const maxCodexBootstrapTimeout = 10 * time.Minute

func codexBootstrapTimeout(cfg *config.Config) time.Duration {
	if cfg == nil || cfg.Streaming.BootstrapTimeoutSeconds <= 0 {
		return 0
	}
	seconds := cfg.Streaming.BootstrapTimeoutSeconds
	if seconds > int(maxCodexBootstrapTimeout/time.Second) {
		return maxCodexBootstrapTimeout
	}
	return time.Duration(seconds) * time.Second
}

func codexBootstrapTimeoutError(timeout time.Duration) statusErr {
	return statusErr{code: http.StatusGatewayTimeout, msg: fmt.Sprintf("codex upstream produced no payload within %s", timeout)}
}
```

Document `bootstrap-timeout-seconds` under `streaming` in `config.example.yaml`, explicitly noting Codex HTTP/SSE scope and the 600-second cap.

- [ ] **Step 4: Run the test and verify GREEN**

Run: `go test ./internal/runtime/executor -run '^TestCodexBootstrapTimeoutBounds$' -count=1`

Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
gofmt -w internal/config/sdk_config.go internal/runtime/executor/codex_executor.go internal/runtime/executor/codex_bootstrap_timeout_test.go
git add config.example.yaml internal/config/sdk_config.go internal/runtime/executor/codex_executor.go internal/runtime/executor/codex_bootstrap_timeout_test.go
git commit -m "feat(codex): add bootstrap timeout configuration"
```

### Task 2: Cancel silent Codex bootstrap attempts

**Files:**
- Modify: `internal/runtime/executor/codex_bootstrap_timeout_test.go`
- Modify: `internal/runtime/executor/codex_executor.go`

**Interfaces:**
- Consumes: `codexBootstrapTimeout` and `codexBootstrapTimeoutError`
- Produces: timeout before headers as a direct 504 error
- Produces: timeout after HTTP 200 as the first stream error chunk
- Produces: permanent timeout disarm after the first non-empty `data:` event

- [ ] **Step 1: Write failing executor tests**

Add these three `httptest.Server` tests, using the existing Codex OpenAI Responses translation path:

```go
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

func TestCodexExecutorBootstrapTimeoutDisarmsAfterFirstEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5.5\"}}\n\n"))
		w.(http.Flusher).Flush()
		time.Sleep(1100 * time.Millisecond)
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
```

Each timeout test must also assert that the upstream request context is canceled. The disarm test must delay its completion event beyond the configured timeout and assert that `response.completed` is still forwarded.

- [ ] **Step 2: Run the tests and verify RED**

Run: `go test ./internal/runtime/executor -run '^TestCodexExecutorBootstrapTimeout' -count=1 -v`

Expected: the first two tests exceed their guard deadline or return no 504 because the executor has no bootstrap timer.

- [ ] **Step 3: Implement the attempt-local timeout**

Add atomic states:

```go
const (
	codexBootstrapPending uint32 = iota
	codexBootstrapObserved
	codexBootstrapExpired
)
```

In `ExecuteStream`, create a child attempt context only when the timeout is enabled, attach it to `httpReq`, and start `time.AfterFunc`. The callback must win only through:

```go
if bootstrapState.CompareAndSwap(codexBootstrapPending, codexBootstrapExpired) {
	cancelAttempt(timeoutErr)
}
```

Use one local `observeBootstrap` closure that stops the timer only when it wins pending to observed. Call it for an explicit HTTP response, the first non-empty `data:` event, and clean stream termination. If expired already won, return or emit `timeoutErr` instead of the context-canceled scanner error. Defer cancellation of the child context until the stream goroutine exits.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `go test ./internal/runtime/executor -run '^TestCodexExecutorBootstrapTimeout' -count=1 -v`

Expected: all timeout and disarm tests pass.

- [ ] **Step 5: Run race detection and package tests**

```bash
go test -race ./internal/runtime/executor -run 'TestCodexBootstrapTimeout|TestCodexExecutorBootstrapTimeout' -count=1
go test ./internal/runtime/executor -count=1
```

Expected: PASS with no race reports.

- [ ] **Step 6: Format and commit**

```bash
gofmt -w internal/runtime/executor/codex_executor.go internal/runtime/executor/codex_bootstrap_timeout_test.go
git add internal/runtime/executor/codex_executor.go internal/runtime/executor/codex_bootstrap_timeout_test.go
git commit -m "fix(codex): timeout silent stream bootstrap"
```

### Task 3: Prove affinity and priority failover end to end

**Files:**
- Create: `sdk/api/handlers/codex_bootstrap_timeout_integration_test.go`

**Interfaces:**
- Consumes: the real `CodexExecutor`, AuthManager, session-affinity selector, and priority selection
- Produces: regression coverage proving one request moves from a stalled high-priority auth to a healthy lower-priority auth

- [ ] **Step 1: Write the real two-server integration test**

Create one stalled server that flushes HTTP 200 and waits for request cancellation, and one healthy server that returns a valid Codex `response.completed` SSE event. Register two Codex auth records:

```go
auth1.Attributes["priority"] = "10"
auth2.Attributes["priority"] = "5"
manager.SetSelector(coreauth.NewSessionAffinitySelector(&coreauth.RoundRobinSelector{}))
```

Send an OpenAI Responses request containing a stable `conversation_id`. Assert:

- the response completes successfully through auth2;
- both servers receive exactly one request;
- auth1's request context is canceled;
- the request does not return a terminal error;
- the elapsed time includes the configured timeout but stays below a fixed guard deadline.

- [ ] **Step 2: Run the integration test**

Run: `go test ./sdk/api/handlers -run '^TestCodexBootstrapTimeoutRotatesAffinityAcrossPriorities$' -count=1 -v`

Expected after Task 2: PASS. If it fails, change only the minimal executor error propagation needed to enter existing `MarkResult` and `tried` handling; do not add selector or handler special cases.

- [ ] **Step 3: Run handler and auth regression suites**

```bash
go test ./sdk/api/handlers ./sdk/cliproxy/auth -count=1
```

Expected: PASS.

- [ ] **Step 4: Format and commit**

```bash
gofmt -w sdk/api/handlers/codex_bootstrap_timeout_integration_test.go
git add sdk/api/handlers/codex_bootstrap_timeout_integration_test.go
git commit -m "test(codex): cover affinity timeout failover"
```

### Task 4: Complete verification and prepare the PR

**Files:**
- Modify only if verification exposes a defect in `internal/config/sdk_config.go`, `internal/runtime/executor/codex_executor.go`, `internal/runtime/executor/codex_bootstrap_timeout_test.go`, or `sdk/api/handlers/codex_bootstrap_timeout_integration_test.go`
- Read: `.github/PULL_REQUEST_TEMPLATE.md`

**Interfaces:**
- Produces: a clean branch based on official `main`
- Produces: a fork PR that documents AI assistance and the deliberate opt-in timeout exception

- [ ] **Step 1: Run fresh verification**

```bash
go test -race ./internal/runtime/executor ./sdk/api/handlers -count=1
go test -count=1 ./...
out=$(mktemp /tmp/cliproxyapi-codex-bootstrap.XXXXXX)
go build -o "$out" ./cmd/server
rm -f "$out"
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 2: Review the complete branch**

Request an independent code review against merge base `411d7d41eee0ff841b5badba5abacad6c11331ef`. Fix all Critical and Important findings and rerun their covering tests.

- [ ] **Step 3: Prepare the PR body**

Use `.github/PULL_REQUEST_TEMPLATE.md`. State that the change is AI-assisted because the configured git identity is not an upstream core developer. Include configuration, behavior, test commands, default-off rollout, duplicate-billing caveat, and that the implementation intentionally avoids #4303's handler-level retry/context wrapper.

- [ ] **Step 4: Push and create the PR**

```bash
git push -u origin codex/codex-bootstrap-timeout
gh pr create --repo gaozuo/CLIProxyAPI --base main --head codex/codex-bootstrap-timeout --title "fix(codex): timeout silent stream bootstrap" --body-file /tmp/cliproxyapi-codex-bootstrap-pr.md
```

Expected: a non-draft PR URL in the gaozuo fork.
