# Changelog

All notable changes to `sse4cj` will be documented here.

## Unreleased

- Repository initialized as an independent greenfield SSE implementation.
- Added normative implementation specification and agent guardrails.
- Added minimal Cangjie package manifest and package root.

### stdx transport constraint analysis and server design revisions

- Verified stdx `HttpResponseWriter` has no public close/abort/interrupt/flush API.
- Documented stdx writeTimeout one-shot timer bug (HTTP/1.1: non-resetting; HTTP/2: absent).
- Added `§0 传输层约束` section to `SPEC.md` documenting the non-interruptible write constraint.
- Revised SPEC backpressure section: all decisions at enqueue time, not at write time.
- Revised SPEC close section: `closeNow` stops enqueue + closes HTTP server; writer task exits on socket break.
- Added SPEC completion conditions 37-40 for stdx constraint, enqueue-stage backpressure, max connections, dead connection handling.
- Added `maxConnections` to `SseServerConfig` (default 1024) to prevent coroutine accumulation.
- Enforced `maxConnections` in `handleRequest` with 503 rejection.
- Defined server internals: `SseConnectionWriter` interface, `SseConnectionConfig`, `SseBackpressureStrategy`, `SseSendStatus`/`SseSendResult`, `SseConnection` (bounded queue + writer loop), `SseHub` (per-endpoint registry + broadcast), `SseBroadcastStats`, `ReplayGapPolicy`, `ReplayProvider`.
- Added `sharedBytes()` to `EncodedSseFrame` for zero-copy broadcast.
- Removed hardcoded `Transfer-Encoding: chunked` from `handleRequest` (SPEC violation).
- Removed phantom `interrupt()` method from `StdxSseConnectionWriter` (stdx has no such API).
- Rewrote `design.md` with full stdx constraint documentation, backpressure strategy, and writer loop design.
- Rewrote `security.md` with slow-consumer disconnect limitation and stdx coroutine leak mitigation.
