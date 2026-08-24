# Transport close contract

`SseConnection` separates **logical close** from **transport close**.

## Logical close

Logical close is controlled entirely by sse4cj and is immediate:

- remove the connection from its `SseHub`;
- reject future enqueue/send operations;
- discard pending queue entries;
- stop starting new heartbeat/replay/live writes;
- wake producers waiting for queue capacity;
- retain only an already in-flight frame until its write returns.

`SseBroadcastStats.disconnected` means the connection was logically evicted from fan-out. It does not claim that TCP/TLS/HTTP transport teardown has already completed.

## Transport close

Transport close means the writer fiber has returned from any in-flight `write`, finished its response lifecycle and released transport-owned resources.

The public state distinguishes:

```text
Open -> Draining -> LogicallyClosed -> TransportClosed
```

`awaitClosed(timeoutMillis)` waits for the transport-close boundary rather than merely waiting for logical eviction.

## Abort capability

`SseTransportAbortCapability` describes the real adapter capability:

- `Immediate`: `SseConnectionWriter.close()` is safe to call concurrently and interrupts an in-flight write.
- `ServerWideOnly`: one response cannot be aborted, but the owning HTTP server can tear transports down during server shutdown.
- `Unsupported`: neither per-response nor stronger server-wide abort is provided through this writer contract.

The current stdx HTTP server path uses `ServerWideOnly`. Its `HttpResponseWriter` public surface does not expose a per-response abort/interrupt operation, so sse4cj must not claim immediate physical teardown of an already-blocked write.

Server shutdown is ordered in two phases: all endpoint and Hub admission gates close first, then graceful draining begins against one shared monotonic deadline. A concurrent `SseServer.closeNow()` upgrades that drain exactly once. Logical connection closure wakes the graceful waiters immediately; physical termination of an in-flight stdx write still occurs only when the write returns/fails or the server-wide transport close takes effect.

In the current daily SDK, `ServerBuilder.build()` immediately creates and binds the server socket. If shutdown wins before start publication, sse4cj closes that unpublished server outside its registry lock. The same SDK's `Server.closeGracefully()` sets its quit flag before pool draining, and a later `Server.close()` then becomes a no-op. To preserve force-upgrade semantics, sse4cj never uses that transport-level graceful operation: after bounded SSE queue draining it calls `Server.close()` directly.

`SseServer.register()` rejects endpoint connection configs that claim another abort capability. This prevents a caller from configuring the stdx writer as `Immediate` when the adapter cannot provide that behavior.

## Resource accounting

The configured frame/byte limits cover both queued and in-flight application frames:

```text
bufferedFrames = queuedFrames + inFlightFrames
bufferedBytes  = queuedBytes  + inFlightBytes
```

Moving a frame from the queue into `write()` therefore does not free capacity. Capacity is released only after the write returns or fails.

This prevents a blocked writer from holding one unaccounted large frame while producers refill the whole queue budget behind it.

## Slow consumers

Slow-consumer policy is decided before starting a new write. With `DisconnectSlowConsumer`, exceeding the frame or byte budget logically evicts the connection immediately. Other healthy connections remain independent because Hub fan-out performs enqueue only and never network I/O.

An in-progress stdx write may continue after logical eviction. Its eventual physical termination depends on that write returning/failing, peer disconnect, or server-wide shutdown.

## stdx write-timeout behavior

The current uploaded stdx HTTP/1.1 server was verified with a real loopback connection. Enabling `ServerBuilder.writeTimeout(200 ms)` disconnected an otherwise healthy SSE stream at approximately the configured response age even though data was successfully transmitted every 50 ms. The runtime logged `write response timeout`.

Therefore `SseServerConfig.writeTimeoutMillis` is retained for source compatibility but is **not forwarded** to `ServerBuilder.writeTimeout` for SSE streaming responses. It is not a slow-consumer detector and it is not a per-write inactivity deadline.

Current slow-consumer protection instead comes from:

- bounded queued + in-flight frame count;
- bounded queued + in-flight bytes;
- enqueue-stage backpressure policy;
- logical Hub eviction;
- `maxConnections` admission control;
- server-wide shutdown as the stronger transport teardown boundary.

## stdx HTTP/1.1 streaming framing limitation

The current stdx public API does not expose an unknown-length/chunked streaming switch on `HttpResponseBuilder`.

A compiled and executed probe using:

```text
HttpResponseBuilder.body(InputStream)
```

with an unknown-length body and no framing header returned HTTP 500 with:

```text
Unknown body size for Content-Length.
```

The same streaming body succeeds when HTTP/1.1 chunked transfer coding is declared. `HttpResponseWriter` has the same practical requirement for an indefinite HTTP/1.1 SSE response.

For that reason the **stdx adapter only** applies the following compatibility workaround:

- HTTP/1.1: declare `Transfer-Encoding: chunked` and let stdx perform the actual chunk encoding;
- HTTP/2: never emit `Transfer-Encoding`;
- generic SSE core code remains unaware of HTTP framing.

This is an explicit current-SDK exception to the preferred transport-owned-framing rule in `SPEC.md`, not a claim that stdx exposes a proper framing API. Strict completion remains blocked until stdx can select indefinite streaming framing without the application declaring the HTTP/1.1 transfer coding.

## Real loopback regressions

The current adapter is covered by real 127.0.0.1 tests that verify:

- an SSE event reaches the client before response close;
- a client that does not read is logically evicted by buffered limits;
- an actively streaming connection survives beyond a tiny configured `writeTimeoutMillis`, proving the unsafe stdx timer is not installed;
- a slow-reading client is evicted by buffer pressure rather than the response-lifetime timer;
- server-wide shutdown closes the remaining transport path.

## Immediate-capable custom transports

A custom transport may set `transportAbortCapability: Immediate` only when its writer `close()` is:

- safe to invoke concurrently with `write()`;
- idempotent;
- guaranteed to request interruption of that response rather than closing unrelated multiplexed streams.

For HTTP/2 this normally requires a per-stream abort/reset primitive, not closing the shared underlying socket.
