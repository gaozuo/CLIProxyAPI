# Codex Bootstrap Timeout Design

## Goal

Add an opt-in timeout for a Codex HTTP/SSE request that has not produced its first non-empty upstream `data:` event, then reuse CLIProxyAPI's existing credential failure and failover path so session affinity and credential priority cannot select the timed-out credential again within the same request.

## Scope

- Applies only to the Codex HTTP/SSE executor.
- Covers time waiting for response headers and time waiting for the first non-empty SSE `data:` event.
- Does not apply after the first non-empty SSE event.
- Does not change Codex WebSocket behavior.
- Does not change New API, downstream keep-alives, selectors, affinity caches, or retry policy.
- Remains disabled by default.

## Configuration

Reuse the configuration shape introduced by PR #4303:

```yaml
streaming:
  bootstrap-timeout-seconds: 20
```

`bootstrap-timeout-seconds <= 0` disables the timeout. Values above 600 seconds are capped at 600 seconds. The example configuration must state that the current implementation applies to Codex HTTP/SSE bootstrap only.

## Architecture

The timeout belongs inside `CodexExecutor.ExecuteStream`, around one credential's upstream HTTP/SSE attempt. A child context created with `context.WithCancelCause` is attached to the upstream HTTP request. A timer starts before `httpClient.Do` and races with observation of the first non-empty SSE `data:` event.

If the timer wins, only the child attempt context is canceled and the executor produces a typed HTTP 504 error. The parent request context remains active. The existing AuthManager bootstrap reader receives the error, calls `MarkResult`, adds the credential to the existing request-local `tried` set, and selects another credential. This preserves existing behavior for session affinity, fill-first routing, and priority fallback without adding any selector-specific logic.

If the first non-empty SSE `data:` event wins, the timer is stopped permanently. The child context remains linked only to the parent request and is canceled when the stream finishes, so healthy long-running streams are unaffected.

## Concurrency

A three-state value protected by one function-local `sync.Mutex` coordinates the timer callback and stream scanner:

- pending: no first event and no timeout;
- observed: first event or explicit HTTP result won;
- expired: timeout won and canceled the attempt.

Only a transition from pending while holding the mutex can win. The timer callback changes pending to expired, releases the mutex, and then cancels the attempt. The response path changes pending to observed and stops the timer while holding the same mutex. This prevents a boundary race from both forwarding a first event and reporting a timeout.

The feature must add no `sync/atomic` import or `atomic.*` operation in production or test code. It also must not introduce a custom context implementation or parent-watcher goroutine.

## Error Handling

Timeouts use a `statusErr` with HTTP status 504 and no `Retry-After`. Before response headers, `ExecuteStream` returns the error directly. After HTTP 200 but before the first event, the stream emits the error as its first chunk. Existing AuthManager logic handles both forms and records the failed credential.

An explicit non-2xx HTTP response stops the bootstrap timer and keeps its original status/error. A clean empty stream remains an `empty_stream` failure handled by existing code. Client cancellation keeps its original context error rather than being rewritten as a bootstrap timeout.

## Safety

- Default configuration remains unchanged.
- Only the current Codex credential attempt is canceled.
- No handler-level retry loop is added.
- No global affinity invalidation is added.
- No new background goroutine is created beyond the standard timer callback.
- Upstream cancellation cannot guarantee that a provider stops already-started computation, so duplicate billing risk cannot be eliminated; canceling the request context is the narrowest available mitigation.

## Testing

1. Configuration disabled, configured, and 600-second cap.
2. Timeout before response headers cancels the upstream request and returns 504.
3. HTTP 200 followed by silence cancels the upstream request and emits 504 before payload.
4. A first event before the deadline disarms the timeout and permits a longer healthy stream.
5. A real two-server integration test enables session affinity and different priorities; the high-priority credential stalls and the same request succeeds through the lower-priority credential.
6. Targeted race tests, package tests, full repository tests, and server build.
7. A branch-diff scan from the official base must find no newly added `sync/atomic` import or `atomic.*` operation.

## Isolated Real-Account Verification

Real-account verification runs on the production host without replacing, restarting, or reconfiguring the production CLIProxyAPI service.

- Build the candidate binary from the reviewed branch.
- Create a temporary directory on the production host and copy, rather than mount, selected Codex account files into it with restrictive permissions.
- Start the candidate on a loopback-only alternate port with its own temporary configuration, log path, and copied auth directory.
- Run a local black-hole HTTP proxy on another loopback-only port. The high-priority copied credential uses this proxy so its CONNECT operation stalls without reaching the provider.
- Keep a different copied real credential at lower priority with normal direct upstream access.
- Enable session affinity and a short bootstrap timeout in the isolated configuration, then issue a real Codex request with a stable conversation ID.
- Require logs and response timing to prove that the high-priority credential was selected first, canceled at the configured timeout, marked unavailable for the request, and replaced by the lower-priority real credential, which returns a successful first event and completion.
- Also run one real request with the timeout disabled and one healthy direct request with the timeout enabled to prove default-off behavior and healthy disarm behavior.
- Remove the isolated process, black-hole proxy, copied credentials, logs, configuration, and temporary directory after collecting redacted evidence.

No production credential contents, access tokens, refresh tokens, SSH passwords, or API keys may be printed, downloaded to the local workstation, committed, or included in the PR.

## Rollout

Ship the setting disabled. The isolated real-account test is validation only and does not enable the setting on the production service. A later production rollout should begin with 20 seconds on one controlled canary instance and monitor timeout counts, credential rotation, duplicate-request indicators, and successful recovery before enabling broadly.
