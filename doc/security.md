# Security

`sse4cj` uses Wirestack as its only default network backend. This page describes the controls enforced by the current implementation and the limits that remain the deployer's responsibility.

## Input and routing validation

- `SseEncoder.validateFrame` rejects carriage return or line feed in `eventType`, and rejects carriage return, line feed, or NUL in `id`.
- The encoder splits newlines in `data` and `comment` into separate SSE lines.
- `SseHeader` requires an HTTP token as the name and rejects carriage return, line feed, or NUL in the value.
- An endpoint path must start with `/` and cannot contain a query, fragment, carriage return, line feed, or NUL. Routing matches the path exactly and ignores the request query.

These checks prevent SSE field and HTTP header injection. Applications must still validate the meaning and authorization of user-provided event data.

## Bounded resources

- `SseDecoderLimits` bounds line, field, data, event, and total buffered bytes. Defaults are 64 KiB per line, 64 KiB per field, 1 MiB of data, 2 MiB per event, and 4 MiB total buffered data.
- `SseConnectionConfig.maxQueuedFrames` and `maxQueuedBytes` cover queued plus in-flight frames, despite their historical names. The defaults are 256 frames and 1 MiB per connection.
- `MemoryReplayBuffer` is bounded by frame count and encoded bytes. Its defaults are 1,024 frames and 16 MiB. It evicts the oldest entries to admit a frame and skips a single frame larger than `maxBytes`.
- `SseServerConfig.maxConnections` bounds active streaming responses. The default is 1,024, the accepted range is 1 through 65,536, and excess requests receive 503.
- Wirestack's unchanged HTTP defaults bound an HTTP header line to 8 KiB, all headers to 64 KiB, and the header count to 100.
- `BoundedAsyncSseCallbackDispatcher` has a bounded pending queue, with a default of 256 and a supported range of 1 through 1,000,000. `InlineSseCallbackDispatcher`, the `EventSourceClient` default, executes callbacks inline and has no queue.

## Header and admission limitations

The server enforces header byte and count limits, but it does not enforce a header-arrival deadline. `SseServerConfig` exposes only `listenEndpoint` and `maxConnections`; it has no read, write, or header timeout. Put an appropriate proxy or network boundary in front of the server when slow-header protection is required.

`SseServerRequest` exposes `method`, `lastEventId`, and `header(name)`. It does not expose a peer address, so an admission handler cannot implement source-IP policy. Enforce IP allowlists and connection-rate controls outside `sse4cj`.

An endpoint runs its admission handler before creating or attaching an SSE connection. A rejection does not enter the Hub, an admission exception produces an empty 500 response, and the server checks its closing state again after the handler returns. Shutdown closes admission gates, but it does not synchronously join arbitrary admission callbacks already running. Admission callbacks should therefore finish promptly and avoid unbounded work.

## Network exposure and HTTPS

The default `SseServer` listens on `127.0.0.1` with an ephemeral port and serves plaintext HTTP/1.1. It does not automatically enable TLS, and the public server configuration has no TLS option. Terminate TLS at a trusted reverse proxy or provide another explicitly reviewed deployment boundary.

The default `WirestackSseClientTransport` handles `https` with Wirestack's system trust, DNS reference-identity verification, and ALPN negotiation. An unavailable trust source, an untrusted certificate, or a hostname mismatch fails the request. The transport does not downgrade to HTTP. Its package-private TLS and resolver injection is for offline package tests, not a public mechanism for disabling verification.

The client also disables automatic redirects and configures a single HTTP attempt. EventSource reconnection remains an `EventSourceClient` policy and creates a new request.

## Cancellation, timeouts, and cleanup

Each `WirestackSseClientTransport.send` creates one `HttpClient`, one request cancellation handle, and one watchdog for that request.

- `openTimeoutMillis` covers the send operation through receipt of response headers, including DNS, TCP, TLS, and request transmission.
- `readTimeoutMillis` is armed separately for each active, nonempty body read. Time spent between reads, including event processing and callbacks, does not consume this budget.
- Caller cancellation and watchdog expiry cancel the request-scoped Wirestack handle. A watchdog timeout does not cancel the caller's `SseCancellationToken`, so `EventSourceClient` can treat it as a recoverable transport failure.
- Terminal cleanup cancels unfinished I/O, closes the response and per-request client, clears the token interrupt registration, and joins the watchdog. Response close is idempotent.

The Wirestack client adapter reports fixed messages for cancellation, open timeout, read timeout, request failure, body-read failure, and response-close failure. These messages omit the URL, query, headers, and provider exception text. This guarantee is scoped to the Wirestack adapter. A user callback exception forwarded by `ForwardToErrorHandler` includes that callback exception's message, so callback code should not place secrets in exception messages.

Server-side forced close uses Wirestack's request cancellation handle: it closes the affected connection for HTTP/1, but only the affected stream for HTTP/2. The server retains active response bodies until their real close path releases them. Graceful close gives Wirestack the same monotonic deadline after application queues drain; body close alone is not treated as proof that an HTTP/2 `END_STREAM` reached the peer. A concurrent force close can upgrade an in-progress graceful close.

## Slow consumers and frame accounting

The default `DisconnectSlowConsumer` strategy decides at enqueue time using both frame and byte limits. A connection that cannot accept a frame is logically closed, removed from its Hub, and physically aborted without blocking broadcasts to other connections.

The Wirestack pull body retains an in-flight frame, including its byte charge, until Wirestack asks for the next nonempty read. Wirestack performs that read only after the preceding HTTP/1.1 write or HTTP/2 write ticket completes. Logical eviction drops pending queued frames immediately, while partial or acknowledgment-pending frame accounting remains charged until the body closes or the next confirmed read releases it.

The generic push-writer seam still models `ServerWideOnly` and `Unsupported` abort capabilities. The built-in Wirestack server requires `Immediate`; registering an endpoint configured otherwise fails.

## Callback delivery and close

Callback exception handling is configurable as `Continue`, `Close`, or `ForwardToErrorHandler`. If the bounded asynchronous dispatcher is full or closed, `EventSourceClient` reports `SlowConsumer` and closes. `close()` stops new dispatch and discards accepted asynchronous callbacks that have not started.

A callback already executing is not synchronously joined by `close()`. Callback code must tolerate application shutdown and must not rely on `close()` waiting for arbitrary user work to return.

## Replay semantics

SSE replay provides at-least-once delivery, not exactly-once delivery. `ReplayGapPolicy` explicitly selects `StartLive`, `Reject`, or `Reset` when `Last-Event-ID` is outside retained history.

`SseHub` serializes replay lookup and connection attachment with replay-aware broadcast storage and connection snapshotting. An event is therefore either in the replay snapshot before attachment or delivered live after registration; live delivery cannot overtake the replay-to-live handoff. The connection must have enough frame and byte capacity for the initial replay or attachment fails. Empty IDs are not retained because they reset the SSE replay cursor.

Admission handlers receive selective header lookup rather than a copied raw header map. The library does not add request logging, but handler code can read requested headers and `lastEventId`; treat both as sensitive when adding application logs.

## Server isolation

Connection registries are instance-owned: each `SseServer` owns its endpoint map, each `SseEndpoint` owns its `SseHub`, and each Wirestack server adapter owns its active-body registry. Servers that use the same route path do not share these registries.
