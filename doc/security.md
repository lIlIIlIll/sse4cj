# Security

Status: design requirements defined; server internals are scaffolded. stdx constraints documented in `design.md`.

## CRLF injection

- `SseEncoder.validateFrame` rejects `eventType` containing CR/LF and `id` containing CR/LF/NUL with `SseErrorKind.InvalidOutboundField`.
- `data` and `comment` newlines are split into separate `data:`/`:` lines by the encoder — no injection possible.
- Tests in `wire_protocol_test.cj` and `wire_property_test.cj`.

## NUL in id

- `id` containing NUL (U+0000) is rejected by `SseEncoder.validateFrame`.

## Bounded resources

- **Line/field/data/event/buffered bytes**: `SseDecoderLimits` with overflow-safe accounting. Default limits: 64 KiB line, 1 MiB data, 2 MiB event, 4 MiB buffer.
- **Per-connection queue**: bounded by both `maxQueuedFrames` (default 256) and `maxQueuedBytes` (default 1 MiB). Not just frame count.
- **Server-wide connections**: `maxConnections` (default 1024). New connections beyond limit get 503.
- **Callback dispatcher**: `BoundedAsyncSseCallbackDispatcher` with `maxPendingCallbacks` (default 64). Inline dispatcher has no queue.

## Slow-consumer isolation

- Backpressure decision at enqueue time, not at write time (see `design.md` — stdx `write()` is non-interruptible).
- Default strategy: `DisconnectSlowConsumer` — queue full → mark dead, remove from Hub, stop enqueuing.
- One slow connection does not block broadcast to other connections.
- **Known limitation**: writer task blocked in `write()` cannot be killed. Coroutine exits only when socket breaks. This is a stdx constraint, not an sse4cj design choice. Mitigated by `maxConnections` limit and dead-connection detection.

## Authentication ordering

- `SseEndpoint.decide()` runs admission handler before `SseHub.attach()`.
- Rejected connections never enter the Hub.
- Auth failures, admission exceptions, response init failures → connection not registered, no ghost connection.

## Callback cleanup

- User callback exceptions do not corrupt decoder state (client) or writer loop (server).
- Callback exception strategy configurable: `Continue`, `Close`, `ForwardToErrorHandler`.
- On `Close`: connection is deregistered and resources released.

## Server isolation

- No static/global connection registries. Each `SseServer` owns its own endpoint map; each `SseEndpoint` owns its own `SseHub`.
- Two `SseServer` instances with path `/events` are fully isolated.

## No callbacks after close

- `close()` sets `AtomicBool`, preventing further callbacks and reconnects.
- Writer task exits on next `write()` exception after socket break.
- Heartbeat timer cancelled on connection close.

## Response/task/timer cleanup

- Every terminal path (write error, auth failure, close) deregisters from Hub.
- Heartbeat timers are per-connection and cancelled on close — no permanent timer leak.
- Writer task is the sole owner of the connection writer; its exit ensures no concurrent write.

## Reconnect storm prevention

- `EventSourceClient` uses `ReconnectPolicy` with configurable `maxAttempts`, `maxDelay`, and jitter.
- Same client has at most one connection attempt and one read loop at a time.
- Reconnect sleep is cancellable via `SseCancellationToken` (closes underlying socket).
- Fake clock/sleeper in tests — no real sleeps.

## Sensitive header redaction

- `Authorization`, `Cookie`, `Proxy-Authorization` are never logged.
- Complete sensitive query strings are not logged.
- `SseServerRequest` exposes header lookup via function, not raw header map — allows selective redaction.

## Replay risk

- SSE + replay provides at-least-once semantics, not exactly-once.
- `ReplayGapPolicy`: `StartLive`, `Reject`, `Reset` — documented and configurable.
- Replay cannot race ahead of or be overtaken by live events (SyncCounter/flag ordering).

Security claims cite implementing code and deterministic tests in `doc/protocol-compliance.md`.
