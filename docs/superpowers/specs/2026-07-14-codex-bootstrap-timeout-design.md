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

A three-state atomic value coordinates the timer callback and stream scanner:

- pending: no first event and no timeout;
- observed: first event or explicit HTTP result won;
- expired: timeout won and canceled the attempt.

Only a compare-and-swap from pending can win. This prevents a boundary race from both forwarding a first event and reporting a timeout. No custom context implementation or parent-watcher goroutine is introduced.

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

## Rollout

Ship the setting disabled. For production, begin with 20 seconds and one controlled canary instance. Monitor timeout counts, credential rotation, duplicate-request indicators, and successful recovery before enabling broadly.
