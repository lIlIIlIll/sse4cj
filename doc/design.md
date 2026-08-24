# Design

Status: wire codec, client transport, and server architecture defined. Server internals are scaffolded; stdx constraints documented below.

## Module boundaries

1. **wire** — SSE wire format: `SseDecoder`, `SseEncoder`, `EncodedSseFrame`, `SseFrame`, `SseEvent`, `SseDecodeSink`, `SseDecoderLimits`, `SseErrorKind`/`SseException`.
2. **client** — `SseStreamReader` (finite), `EventSourceClient` (reconnecting), `SseClientTransport` abstraction, `SseCancellationToken`, callback dispatcher.
3. **server** — `SseServer`, `SseEndpoint`, `SseHub`, `SseConnection`, `SseConnectionWriter` abstraction, backpressure, replay, heartbeat.
4. **observability** — metrics/diagnostic hooks (planned).

## stdx HTTP transport constraints

Verified against stdx source (`HttpResponseWriter`, `HttpContext`, `HttpEngineConn1`, `HttpEngineConn2`, `http_response.cj`, `http_request_context.cj`, `http_server1_1.cj`, `stream_server2_0.cj`):

### HttpResponseWriter public surface

```
public class HttpResponseWriter {
    public HttpResponseWriter(let ctx: HttpContext) {}
    public func write(buf: Array<Byte>): Unit { ... }
}
```

- **No `close`/`abort`/`interrupt`/`flush`/`end` API.**
- `write()` executes `synchronized(ctx.writerMtx) { ctx.httpConn.writeResponseByWriter(ctx, buf) }` — synchronous, blocking, holding `writerMtx`.
- External packages cannot access `writerMtx`, `responded`, `upgraded`, `httpConn`, or the underlying socket.

### HttpContext public surface

```
public class HttpContext {
    public func isClosed(): Bool       // read-only
    public prop request: HttpRequest
    public prop responseBuilder: HttpResponseBuilder
    public prop clientCertificate: ?Array<Certificate>
}
```

`writerMtx`, `responseFlushedByUser`, `responseFlushedWithChunked`, `upgraded`, `responded` are package-private. `HttpEngineConn` is `abstract class` (not `public`); `HttpEngineConn1`/`HttpEngineConn2` are `class` (not `public`).

### writeTimeout bug (HTTP/1.1)

`writeResponseByWriter` (`http_server1_1.cj:840-880`):

```
if (!ctx.responseFlushedByUser) {       // first flush only
    writeTimer = HttpTimer(start: writeTimeout, task: { close() })
    writeWithoutBody(response)
    ctx.responseFlushedByUser = true
}
// subsequent writes: no writeTimer.cancel(), no writeTimer restart
this.writeBodyByChunk(...)  // or conn.write(bodyData)
```

The timer is set once on first flush, never cancelled or reset. Consequences:

- **Active SSE connections are killed** after `writeTimeout` elapses, even while streaming normally.
- **Slow consumers blocking on the Nth write** cannot be reliably detected — the timer may have already fired (first-flush expiry) or been cancelled (if `writeResponse` path was taken).

### HTTP/2 path

`stream_server2_0.cj:969-982` — `writeResponseByWriter` has **no writeTimer at all**. Flow control via `waitIfWindowNegative()` can block indefinitely with no timeout.

### Design implications

1. **Cannot interrupt in-progress `write()`.** No public API to close/abort a single connection's writer.
2. **Cannot rely on writeTimeout.** It's a one-shot bug, not a per-write timeout. Will kill healthy connections.
3. **Backpressure must be pre-emptive.** Decide disconnect/drop at enqueue time, before `write()` is called.
4. **Writer task lifecycle is coupled to socket lifetime.** Writer task exits only when `write()` throws (socket closed by client, TCP timeout, or engine close).
5. **Max connections limit is mandatory.** Without it, slow-consumer attacks accumulate blocked writer coroutines.

## Backpressure strategy

```
producer → Hub.send(frame) → bounded queue → writer task → responseWriter.write(frame)
                ↑ decision point                         ↑ non-interruptible blocking zone
```

### Decision at enqueue

- Queue full → apply `SseBackpressureStrategy`:
  - `DisconnectSlowConsumer` (default): mark connection dead, remove from Hub, stop enqueuing.
  - `DropNewest`: discard the incoming frame.
  - `DropOldest`: discard the oldest queued frame, enqueue new.
  - `RejectSend`: return `SseSendStatus.Rejected` to caller.
  - `BlockWithTimeout`: wait up to `timeoutMillis` for queue space, then disconnect.

### Writer task blocked in write()

```
writer task blocked in write()
→ Hub-side queue fills up
→ subsequent send() returns Disconnected or Failed
→ Hub removes connection from fan-out list
→ other connections unaffected
→ writer task coroutine exits when socket breaks (write throws)
```

### Dead connection detection

- Track `lastDequeueTime` per connection.
- If `now - lastDequeueTime > staleThreshold` and queue is non-empty → mark dead.
- Dead connections: stop enqueuing, remove from Hub.
- Writer task will eventually exit when socket breaks.

### Coroutine leak protection

- `SseServerConfig.maxConnections` (default 1024).
- New connections beyond limit → 503 response.
- No unbounded coroutine growth.

## Server ownership

```
SseServer
  owns endpoint registry (HashMap<String, SseEndpoint>)
  owns HTTP Server lifecycle

SseEndpoint
  owns SseHub
  owns SseEndpointConfig
  owns admission handler

SseHub
  owns connection registry (per-endpoint, NOT global)
  owns replay provider

SseConnection
  owns outbound queue (bounded: frames + bytes)
  owns writer task/fiber
  owns SseConnectionWriter (transport abstraction)
```

Two `SseServer` instances with the same path `/events` are fully isolated — no shared static state.

## Connection lifecycle

```
parse request
→ extract Last-Event-ID
→ admission handler (authenticate/admit)
→ if rejected: send error response, return (connection never enters Hub)
→ if accepted: create SseConnection
→ register in Hub
→ replay (if ReplayProvider configured)
→ live stream (writer loop)
→ writer loop: queue.remove() → writer.write(frame) → on error: deregister + break
→ finally: deregister from Hub
```

### Writer loop

```
while (true) {
    let frame = queue.remove(timeout: heartbeatInterval)
    match (frame) {
        case Some(f) => writer.write(f)   // blocking, non-interruptible
        case None => writer.write(heartbeatFrame)  // idle heartbeat
    }
}
// exits on: write throws Exception, or queue closed
```

## Single-writer model

One writer task per connection. Producers enqueue `EncodedSseFrame`, never write sockets directly. Writer task owns the `SseConnectionWriter` and is the sole caller of `write()`.

## Replay/live ordering

Replay frames are enqueued before live frames. A `SyncCounter` or flag ensures replay completes before live events start. Both replay and live are subject to the same queue limits and backpressure.

## Cancellation

### Client-side

`SseCancellationToken` supports `registerInterrupt(action)` which wires a cancellation callback into the transport layer (closes the TCP socket / HTTP client). `cancel()` invokes the interrupt immediately if already registered, or marks cancelled for future registration.

### Server-side

- `closeNow()`: closes every endpoint admission gate, logically closes every connection, then closes the HTTP server (breaking listener + sockets). It can upgrade an in-progress graceful close and wake its drain waiters immediately.
- `closeGracefully(deadline)`: closes every endpoint admission gate before waiting on any writer, drains all endpoints against one shared monotonic deadline, then force-closes the stdx server transport.
- **Cannot interrupt in-progress `write()`** — see stdx constraints above.

## Graceful shutdown

```
stop accepting new connections
→ mark every endpoint and Hub closing
→ drain: writer tasks continue consuming queues
→ deadline: close HTTP server
→ sockets break → write() throws → writer tasks exit
→ deregister all connections
→ clear registry
```

Repeated calls are idempotent. `closeNow()` is a stronger operation: when it races with or follows `closeGracefully()`, it runs once, wakes the graceful drain through logical connection closure, and does not wait for the original graceful deadline.

The current daily stdx `Server.closeGracefully()` is first-close-wins: it sets its quit flag before draining, so a later `close()` cannot upgrade it to forced transport teardown. sse4cj therefore performs graceful behavior only at its bounded application queues and always finishes server shutdown with `Server.close()`.

## HTTP/1.1 and HTTP/2 behavior

- Framing (chunked vs stream frames) handled by stdx — sse4cj does not set `Transfer-Encoding`, `Connection`, or `Content-Length`.
- `HttpResponseWriter.write()` works on both HTTP/1.1 (chunked body) and HTTP/2 (DATA frames).
- HTTP/1.1 has the one-shot writeTimer bug; HTTP/2 has no write timer at all. Neither is used by sse4cj for slow-consumer detection.

## Memory bounds

- Per-connection queue: `maxQueuedFrames` (default 256) + `maxQueuedBytes` (default 1 MiB).
- Per-connection decoder limits: configurable via `SseDecoderLimits`.
- Server-wide: `maxConnections` (default 1024) bounds total coroutine + queue memory.
- Encoded frames shared across fan-out — no per-subscriber copy of large payloads.
