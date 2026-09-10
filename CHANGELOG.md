# Changelog

All notable changes to `sse4cj` are documented here.

## Unreleased

### Wirestack network cutover (breaking)

- Replaced the stdx network adapters with Wirestack as the only default client and server backend. Removed the `Stdx*` transport surface and the `CANGJIE_STDX_PATH` network dependency.
- Added `WirestackSseClientTransport` as the public concrete client transport and made it the `EventSourceClient` default. Each request owns its client, cancellation handle, renewable timeout watchdog, response, and cleanup lifecycle.
- Renamed `connectTimeoutMillis` to `openTimeoutMillis`. The new timeout covers request sending through response headers; `readTimeoutMillis` is renewed for each active, nonempty body read.
- Changed `SseServerConfig` to `listenEndpoint: SocketEndpoint` and `maxConnections`. The default is loopback port zero. Removed address/port strings, server read/write/header timeouts, and `SseServerRequest.remoteAddress`.
- The server no longer offers a header-arrival deadline or source-IP admission data. Deployments that need slow-header defense, TLS termination, IP policy, or connection-rate controls must provide them at a trusted network boundary.
- The default server remains plaintext and does not auto-enable TLS. The default client verifies HTTPS with Wirestack system trust and hostname identity.
- Switched Wirestack server responses to the connection's bounded pull stream. Frame and byte charges include queued and in-flight data and are released only after the provider advances past the preceding write or closes the body.
- Added request-scoped forced cancellation, active-body cleanup, shared-deadline graceful shutdown, and force-close upgrade while graceful shutdown is in progress.

### Initial repository setup

- Repository initialized as an independent greenfield SSE implementation.
- Added normative implementation specification and agent guardrails.
- Added minimal Cangjie package manifest and package root.

### Historical: stdx transport constraint analysis and server design revisions

The entries below record the pre-Wirestack design and are retained as project history. They do not describe the current transport or deployment guidance.

- Verified stdx `HttpResponseWriter` had no public close/abort/interrupt/flush API.
- Documented the stdx `writeTimeout` one-shot timer behavior at the time: non-resetting for HTTP/1.1 and absent for HTTP/2.
- Added the original `§0 传输层约束` section to `SPEC.md` for the non-interruptible stdx write constraint.
- Revised the original backpressure design to make decisions at enqueue time.
- Revised the original close design so `closeNow` stopped enqueueing and closed the stdx HTTP server while the writer task waited for socket failure.
- Added the original completion conditions for stdx constraints, enqueue-stage backpressure, maximum connections, and dead-connection handling.
- Added `maxConnections` to the original `SseServerConfig` with a default of 1,024 and 503 rejection above the limit.
- Defined the initial server internals: `SseConnectionWriter`, `SseConnectionConfig`, backpressure and send result types, bounded connection queues, per-endpoint Hub, broadcast statistics, and replay interfaces.
- Added `sharedBytes()` to `EncodedSseFrame` for zero-copy broadcast.
- Removed a hardcoded `Transfer-Encoding: chunked` header from the original provider-neutral request handler.
- Removed a phantom `interrupt()` method from the old `StdxSseConnectionWriter`.
- Rewrote the original design and security documents around the stdx blocked-writer limitation. Current guidance is in `doc/security.md`.
