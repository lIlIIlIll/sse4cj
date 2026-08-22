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

An in-progress stdx write may continue after logical eviction. Its eventual physical termination depends on that write returning/failing, peer disconnect, or server-wide shutdown. `writeTimeout` is not treated as the semantic slow-consumer detector.

## Immediate-capable custom transports

A custom transport may set `transportAbortCapability: Immediate` only when its writer `close()` is:

- safe to invoke concurrently with `write()`;
- idempotent;
- guaranteed to request interruption of that response rather than closing unrelated multiplexed streams.

For HTTP/2 this normally requires a per-stream abort/reset primitive, not closing the shared underlying socket.
