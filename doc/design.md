# Design

Status: the SSE codec, reconnecting client, Wirestack client transport, server queue, and Wirestack server transport are implemented. Wirestack is the only built-in network backend.

## Module boundaries

1. **wire**: `SseDecoder`, `SseEncoder`, `EncodedSseFrame`, `SseFrame`, `SseEvent`, decoder limits, and SSE errors. This layer has no HTTP dependency.
2. **client**: `SseStreamReader`, `EventSourceClient`, `SseClientTransport`, `SseCancellationToken`, reconnect policy, and callback dispatch.
3. **server core**: `SseServer`, `SseEndpoint`, `SseHub`, `SseConnection`, replay, bounded buffering, heartbeat, and the generic `SseConnectionWriter` interface.
4. **Wirestack adapters**: `WirestackSseClientTransport` and the package-internal server transport. These translate between provider-neutral SSE values and Wirestack HTTP objects.

The decoder still consumes `std.io.InputStream`, and the server queue stores `EncodedSseFrame`. Wirestack types stop at the HTTP adapter boundary.

## Client transport ownership

`EventSourceClient` uses `WirestackSseClientTransport` by default. Each `send` creates one:

- `HttpClient`;
- `HttpRequestCancellationHandle`;
- request owner shared by the response and its `InputStream`;
- watchdog worker; and
- cancellation-token interrupt registration, when a token is supplied.

The client is configured with redirects disabled and one HTTP attempt. EventSource reconnect policy remains the only retry loop. A response owns its request resources until EOF, explicit `close`, cancellation, timeout, or failure. Cleanup cancels unfinished I/O, closes the response and client, clears the token registration, and joins the watchdog. Repeated response close is idempotent.

The public constructor uses Wirestack's default HTTPS configuration, including system trust, certificate validation, hostname verification, and ALPN. Package-internal TLS and resolver injection exists only for deterministic offline tests. The transport borrows those injected objects and does not close them.

### Open and read budgets

`SseHttpRequest` has two independent budgets:

- `openTimeoutMillis` covers the active send from DNS, TCP, and TLS through request transmission and receipt of response headers.
- `readTimeoutMillis` covers each active, nonempty response-body read.

The read budget is armed again for every nonempty read. Time spent between reads, including decoder callbacks and application processing, does not consume it. Reading into an empty destination returns `0` without arming a budget and without marking EOF.

One watchdog uses a monotonic absolute deadline and a generation number. Disarming one operation cannot let an expired deadline cancel a later read. Both watchdog expiry and caller cancellation act on the request-scoped Wirestack handle. A timeout does not cancel the `SseCancellationToken`, so `EventSourceClient` can report `TransportFailure` and reconnect. Caller cancellation wins classification as `Cancelled`.

After `send` returns, `EventSourceClient` checks its closed state and attempt token again before setting `OPEN` or invoking `onOpen`. A closed client cannot transition back to `OPEN` or `CONNECTING`.

## Server ownership

```text
SseServer
  owns endpoint routes and Wirestack HttpServer lifecycle
  owns the active-response reservation count

SseEndpoint
  owns one SseHub, endpoint configuration, and admission handler

SseHub
  owns that endpoint's connection map and optional replay provider

SseConnection
  owns one bounded queue and its queued/in-flight accounting
  is claimed by either a generic push writer or one pull stream

Wirestack server router
  owns a per-server map of handed-off ResponseBody values
```

There are no process-global route, connection, or response-body registries. Two servers using the same path remain isolated.

`SseServerConfig` contains a typed `SocketEndpoint` and `maxConnections`, whose valid range is `1..65536`. The default endpoint is IPv4 loopback on an ephemeral port, and the default server is cleartext HTTP/1.1. The package-internal server factory can inject TLS for protocol tests; there is no public server TLS configuration.

The server API does not expose a remote address, server read/write timeouts, or a header-arrival deadline. Wirestack's finite parser, header, chunk, body, and connection limits remain in effect, but they do not provide a time limit for a peer that stalls before completing request headers.

## Routing, admission, and response reservations

Routes are registered before serving and cannot change after serving begins. Routing uses the origin-form path before the first `?`, with no percent decoding or prefix matching. A malformed target returns 400, an unknown path returns 404, and a non-GET request to a known path returns 405.

For a matching GET request:

1. The router builds a provider-neutral `SseServerRequest` from the method, `Last-Event-ID`, and header lookup.
2. The endpoint admission handler runs before a connection enters the Hub. Rejection uses its configured `400..599` status; an admission exception returns 500.
3. The server rechecks its and the endpoint's closing gates. A close that won while admission ran returns 503.
4. The server acquires an active-response reservation. Exceeding `maxConnections` returns 503.
5. It creates an `SseConnection`, transfers the reservation to its pull stream, attaches the connection to the Hub, and returns the streaming response.

The reservation remains charged after the handler returns. It is released only when the actual response body closes. Every failure before ownership transfer releases it directly; every failure after transfer closes the stream and lets its release callback do so.

Admission handlers are arbitrary application callbacks. Shutdown closes the gates and rejects their later handoff, but it does not promise to synchronously join a callback that is already running.

## Replay and live ordering

`SseHub` uses its existing mutex to serialize attach with replay-aware broadcast:

- `attach` asks the replay provider for frames, enqueues the entire replay batch within connection limits, and then inserts the connection in the Hub, all while holding the Hub mutex.
- `broadcast(SseFrame)` encodes once, stores the frame by event ID, and snapshots current connections while holding the same mutex. It delivers to that snapshot after releasing the mutex.
- `broadcastEncoded(frame, id)` follows the same replay-aware path.
- `broadcast(EncodedSseFrame)` cannot infer an ID and therefore delivers without adding replay history.

A concurrent frame is consequently either stored before attach and returned as replay, or stored after the new connection is visible and delivered live. The Hub mutex itself is the handoff mechanism; there is no second queue or completion flag. Replay and live frames use the same connection queue and the same frame/byte limits.

The endpoint adds its optional initial comment after Hub attachment. Replay is already queued at that point; a concurrent live broadcast can race the initial comment because the connection is visible after `attach` returns.

## Queue and pull-body flow

Wirestack consumes an `SseConnectionStream` directly:

```text
producer -> Hub.broadcast -> bounded SseConnection queue
                                  |
                                  v
                         SseConnectionStream.read
                                  |
                                  v
                  Wirestack H1 write or H2 write ticket
```

The pull stream and generic push writer share `takeNext`, heartbeat selection, logical-close state, and in-flight accounting. They do not maintain duplicate queues. Only one of `openStream` or `start(writer)` can claim a connection.

A pull read has these rules:

1. Only one reader may be active.
2. An empty destination returns `0` without consuming or acknowledging a frame.
3. A frame can be copied in bounded slices from `sharedBytes`; the implementation does not clone the whole frame for each slice.
4. Finishing the last slice marks the frame as awaiting acknowledgement but keeps its full in-flight frame and byte charge.
5. The next nonempty read first acknowledges that frame, then checks for close, EOF, queued data, or heartbeat.
6. With no data and no terminal condition, the read waits. A temporary empty queue is not EOF.

Wirestack requests the next body segment only after the previous HTTP/1.1 write or HTTP/2 write ticket completes. Delaying acknowledgement until that next read keeps application buffer capacity charged through the provider's preceding network-write boundary. If the body closes before another read, close settles the retained frame exactly once.

The configured names `maxQueuedFrames` and `maxQueuedBytes` cover queued plus in-flight data:

```text
bufferedFrames = queuedFrames + inFlightFrames
bufferedBytes  = queuedBytes  + inFlightBytes
```

Backpressure is decided when producers enqueue. `DisconnectSlowConsumer` removes an over-budget connection from Hub fan-out and requests a transport abort. The other policies drop, reject, or wait for capacity according to `SseBackpressureStrategy`. These decisions use current buffer occupancy, not queue age. Encoded frame storage is shared across fan-out; each connection tracks only its queue entry and accounting.

## Generic push writer capability

`SseConnection.start(SseConnectionWriter)` remains a provider-neutral extension seam. Its single writer fiber removes one entry, calls `write` and `flush`, and releases the in-flight charge after the call completes or fails.

`SseTransportAbortCapability` describes what that writer can truthfully do:

- `Immediate`: `close()` is concurrent-safe, idempotent, and interrupts the affected response.
- `ServerWideOnly`: an individual response cannot be interrupted through the writer, but an owning transport can tear it down during server shutdown.
- `Unsupported`: neither request-scoped nor server-wide interruption is supplied by that writer contract.

The Wirestack-backed `SseServer` requires `Immediate` and rejects endpoints configured otherwise. Its pull abort calls `request.requestCancellation.cancel()`, which is request-scoped for HTTP/1.1 and stream-scoped for HTTP/2. `ServerWideOnly` and `Unsupported` remain meaningful only for direct/custom uses of the generic push interface; they are not alternate built-in network backends.

## Response-body registry and cancellation

The Wirestack router registers the operation-context cancellation callback before publishing a streaming response. That callback calls the pull stream's nonblocking `cancel`, which requests logical close and wakes a reader. It does not run the waiting body-close path inside the callback.

Publication and the router's closing check occur under the same mutex. A per-server registry holds each handed-off `ResponseBody`, including bodies whose connections have already been logically evicted from the Hub. The entry is removed by the body's final release path. Shutdown snapshots bodies under the registry lock and closes them outside it.

Logical close removes the connection from fan-out, rejects new sends, discards pending queue entries, and wakes waiters. A partial or acknowledgement-pending frame remains charged until the provider closes the body or requests the next segment. `awaitClosed` waits for that physical body-close boundary, not only Hub removal.

## HTTP framing

Successful SSE responses include:

- `Content-Type: text/event-stream; charset=utf-8`;
- `Cache-Control: no-cache, no-transform`; and
- optional `X-Accel-Buffering: no`.

The Wirestack adapter exposes an unknown-length `HttpBodyStream`.

- HTTP/1.1 adds `Transfer-Encoding: chunked` and `Connection: close`. Wirestack writes chunk boundaries and the terminal chunk.
- HTTP/2 adds neither connection-specific header. Wirestack writes DATA frames and the final END_STREAM.

sse4cj never writes raw HTTP framing bytes. Closing the application body is not proof that an HTTP/2 END_STREAM has reached the peer; provider shutdown completes the protocol-level drain.

## Shutdown

`closeGracefully(timeoutMillis)` computes one monotonic deadline, closes every endpoint admission gate, and drains endpoints against the remaining shared budget. It then always calls Wirestack `HttpServer.shutdown` with that same deadline. The adapter closes any remaining registered response bodies after shutdown returns.

`closeNow()` closes the gates and Hubs, requests each connection's request-scoped abort, invokes `HttpServer.close`, and closes registered bodies. Graceful and force transport close have separate one-time claims, so a concurrent `closeNow()` can upgrade an in-progress graceful shutdown.

Graceful queue completion does not invoke request abort. Forced reasons, including deadline expiry, queue overflow, explicit close, write failure, and server shutdown, do. Repeated close calls and release callbacks are idempotent.

## Implemented verification scope

Real loopback coverage verifies HTTP/1.1 data delivery before response close, empty-stream force close, bounded slow-consumer eviction, active-response slot reuse after physical close, replay `2, 3` followed by live `4` through a query-bearing route, admission/routing failures without ghost Hub entries, normal graceful EOF, deadline force, and force upgrade during graceful shutdown.

Offline TLS coverage verifies a trusted `example.com` certificate succeeds while hostname mismatch and an untrusted certificate fail. HTTP/2 coverage opens two streams on one client connection, aborts the first with either reset or EOF observable at the reader, and confirms the sibling continues receiving data with one resolver call. It also verifies the responses omit `Transfer-Encoding` and `Connection`, and that graceful shutdown delivers a fully consumed body with END_STREAM.

These results are limited to the tested Linux x86_64 toolchain and Wirestack source dependency. They do not establish other-platform support, release benchmarks, profiler results, or a public server TLS API.
