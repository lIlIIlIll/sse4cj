# Transport close contract

`SseConnection` separates removal from application fan-out, response-body release, and final HTTP protocol completion. Treating those boundaries as the same event can release buffer or admission capacity too early.

## Connection states

```text
Open -> Draining -> LogicallyClosed -> TransportClosed
  |                         ^
  +-------------------------+
```

A forced close can move directly from `Open` to `LogicallyClosed`.

### Logical close

Logical close is an sse4cj state transition. The first caller:

- stops accepting new frames;
- removes the connection from its endpoint Hub;
- discards queued entries;
- wakes blocked producers and the body reader;
- records the first close reason; and
- requests transport abort for a forced reason when the configured capability permits it.

`SseBroadcastStats.disconnected` reports this fan-out eviction. It does not mean that a response body, TCP connection, or HTTP/2 stream has finished closing.

A partial or completely copied pull frame can still be in flight after logical close. Its charge remains until Wirestack requests another segment or closes the response body.

### Transport close

`TransportClosed` means the owner has settled its in-flight state and marked the connection closed:

- a pull body has left any active read, settled its retained frame, and deregistered; or
- a generic push writer has left `write`/`flush`, closed its writer, and deregistered.

`awaitClosed()` and `awaitClosed(timeoutMillis)` wait for that mark. The pull reservation-release callback runs once outside the mutex immediately afterward, before `stream.close()` returns; `awaitClosed` alone is not a callback-join barrier. Pull cancellation does not satisfy this boundary because it deliberately avoids waiting inside a provider callback.

For HTTP/2, `TransportClosed` is still an application-body boundary. It does not prove that Wirestack has emitted END_STREAM or that the peer has observed it.

## Pull response ownership

The Wirestack server opens one `SseConnectionStream` for each accepted request. The stream and `SseConnection` share the same mutex, queue, in-flight accounting, close reason, abort callback, and release callback.

Only one transport owner can be claimed. `openStream` and `start(SseConnectionWriter)` race on the same claim, so a connection cannot have both a pull body and a push writer.

Only one pull read can be active. `SseConnectionStream.cancel()` requests logical close and wakes that reader. `SseConnectionStream.close()` performs the waiting finalization:

1. Mark body close requested and wake the reader.
2. If no earlier close reason exists, record `WriteFailure` and request abort.
3. Wait with the connection mutex released until the active read exits.
4. Settle any partial or acknowledgement-pending frame once.
5. Deregister the connection and mark it `TransportClosed`.
6. Invoke the response reservation release callback once.

Repeated or reentrant close calls do not repeat finalization.

## Delayed acknowledgement

Frame and byte budgets include queued and in-flight entries:

```text
bufferedFrames = queuedFrames + inFlightFrames
bufferedBytes  = queuedBytes  + inFlightBytes
```

Moving an entry from the queue into a body read transfers its charge to in-flight accounting. It does not free capacity.

The pull body may return one frame over several reads. Copying the last byte sets `ackPending` but retains the entire entry. At the beginning of the next nonempty read, the stream:

1. completes the previous in-flight entry;
2. updates the successful-write timestamps; and
3. then checks close state, EOF, queued frames, or heartbeat.

An empty destination neither consumes data nor acknowledges the preceding frame.

This order is tied to Wirestack's provider behavior: it asks for another HTTP/1.1 body segment only after writing the prior segment, and asks for another HTTP/2 segment only after the prior DATA write ticket completes. The next read is therefore the acknowledgement boundary available to sse4cj.

When graceful draining has no queued or in-flight entries, the reader claims `GracefulDrainComplete` and returns EOF without aborting the request. If the body closes before another read, close settles the retained entry instead.

## Close reasons and abort

`GracefulDrainComplete` is the only non-forced close reason. It stops admission and lets the body reach EOF.

These reasons request abort:

- `ExplicitCloseNow`;
- `GracefulDeadlineExceeded`;
- `QueueFrameLimitExceeded`;
- `QueueByteLimitExceeded`;
- `WriteFailure`; and
- `ServerShutdown`.

The abort callback is taken under the connection mutex and invoked after releasing it. This permits provider cancellation to call back into stream state without lock inversion.

For Wirestack, abort calls `request.requestCancellation.cancel()`. The handle affects the request's HTTP/1.1 connection or the specific HTTP/2 stream. It does not close a shared HTTP/2 client connection and its sibling streams.

The operation-context cancellation registration calls `stream.cancel()`. That path may run synchronously during registration, so the router checks `stream.isClosed()` while publishing. It does not call the waiting `stream.close()` from the cancellation callback.

## Generic push writer capability

The public `SseConnectionWriter` path remains independent of the Wirestack server adapter. One writer fiber calls `write` and `flush`; producers only enqueue.

`SseTransportAbortCapability` states what a generic writer supplies:

- `Immediate`: `close()` is idempotent, safe alongside `write`, and requests interruption of this response.
- `ServerWideOnly`: per-response interruption is unavailable, but the owning server may later tear down transports.
- `Unsupported`: the writer contract supplies neither form of interruption.

With `Immediate`, logical close calls the active writer's `close()` outside the connection mutex. With the other capabilities, an in-progress write can remain in flight until it returns or fails. Its frame stays charged and `awaitClosed` continues waiting.

`SseConnectionConfig` defaults to `Immediate`. The built-in Wirestack `SseServer` accepts only that setting because its request-scoped abort implements the contract. `ServerWideOnly` and `Unsupported` are for direct or custom uses of the generic push interface, not selectable built-in server backends.

## Slow consumers

Slow-consumer decisions occur at enqueue:

- the configured frame and byte limits count queued plus in-flight data;
- `DisconnectSlowConsumer` logically evicts an over-budget connection and requests its Wirestack request abort;
- other strategies drop, reject, or wait for capacity; and
- Hub fan-out never performs network I/O.

Backpressure uses buffer occupancy rather than queue age. Physical admission capacity is recovered through request abort followed by body close and its release callback.

## Server admission and response reservation

Admission runs before a connection enters the Hub or consumes an active-response reservation. A rejected request returns its configured status, and an admission exception returns 500.

After admission, `SseServer` checks the server and endpoint close gates, increments the active-response count, and checks the gates again. If close won during either interval, it returns 503 and releases any acquired count.

Once `openStream` succeeds, the stream owns the reservation. The handler returning does not release it. Closing the Wirestack body invokes the stream's release callback and decrements the count exactly once.

Shutdown does not promise to join an arbitrary admission callback already executing. It closes admission gates and rejects the callback's handoff when control returns.

## Active response-body registry

The Wirestack router keeps a response-body map scoped to that server. It includes handed-off bodies even after their connections have left the Hub.

Body publication and the router closing check share one mutex. If shutdown or synchronous cancellation wins, publication fails and the body is closed instead of returned. The body's release path unregisters its cancellation callback and removes the map entry.

Shutdown snapshots the map while holding its mutex and closes each body after releasing the mutex. Provider close, pull finalization, and reservation release can therefore reenter the registry safely.

## Graceful server shutdown

`SseServer.closeGracefully(timeoutMillis)`:

1. computes one absolute monotonic deadline;
2. closes every endpoint and Hub admission gate;
3. drains all endpoint queues against the remaining part of that shared deadline;
4. always calls `HttpServer.shutdown` with the same deadline; and
5. closes any response bodies still registered when Wirestack shutdown returns.

Application queue drain is not used as proof of network drain. Wirestack shutdown remains responsible for HTTP writers and protocol completion. A provider result that still has cooperatively exiting tasks is not, by itself, evidence of leaked SSE queue entries.

A normally drained pull stream reaches EOF without request abort. For HTTP/1.1, Wirestack writes the terminal chunk and the response uses `Connection: close`. For HTTP/2, Wirestack completes END_STREAM as part of its shutdown path.

## Forced server shutdown and upgrade

`SseServer.closeNow()`:

1. closes all admission gates;
2. logically closes Hub connections, triggering their request-scoped aborts;
3. invokes `HttpServer.close()`; and
4. closes the snapshot of remaining response bodies.

Graceful and forced transport shutdown use separate one-time claims. A `closeNow()` racing with an in-progress graceful shutdown can therefore invoke the real force close once, wake blocked reads, and let both shutdown callers finish without waiting for the original deadline.

All network and body close operations run outside endpoint, connection, and registry locks.

## HTTP framing boundary

The Wirestack body reports unknown content length. For a streaming HTTP/1.1 response, the adapter adds:

```text
Transfer-Encoding: chunked
Connection: close
```

Wirestack performs the chunk encoding and writes the terminal chunk. sse4cj does not write HTTP framing bytes.

HTTP/2 responses omit both headers. Wirestack converts body segments to DATA frames. A response-body close can precede the actual END_STREAM write, so server graceful shutdown must still run even when every SSE connection reports application-level drain.

## Client request close

Each `WirestackSseClientTransport.send` owns one client, request cancellation handle, response, token registration, and watchdog. The response wrapper and body stream share that owner.

`openTimeoutMillis` is armed around the Wirestack send through receipt of response headers. `readTimeoutMillis` is armed independently for each nonempty body read. No budget remains armed while the application is idle between reads.

Caller cancellation and watchdog expiry both cancel the request-scoped handle. Caller cancellation is reported as `Cancelled`. Open and read expiry are `TransportFailure`, and they do not cancel the EventSource token, so a later EventSource attempt gets a new client and handle.

EOF and explicit response close run the same one-time cleanup. Cleanup requests cancellation to unblock I/O, closes provider response and client resources, clears the SSE interrupt registration, and joins the watchdog outside its mutex. A close racing with a read interrupts the read before waiting for cleanup.

## Verified protocol behavior and limits

Real HTTP/1.1 scenarios cover streaming before response close, force-closing an empty body, slow-consumer eviction, admission-slot reuse after physical close, graceful EOF, deadline force, and `closeNow` upgrading an active graceful shutdown.

Offline TLS scenarios cover trusted-certificate success, hostname mismatch, and an untrusted certificate. A real HTTP/2 scenario opens two streams through one client and one resolver call. Closing the first stream is observed as reset or EOF, while the sibling remains open and receives another event. A separate graceful HTTP/2 scenario consumes the body fully and observes END_STREAM. HTTP/2 responses are also checked to omit `Transfer-Encoding` and `Connection`.

These observations do not imply that application body close alone proves HTTP/2 wire completion, that arbitrary admission callbacks are synchronously joined, or that server-side request/header timeouts exist. The current server API has no remote-address field, read/write timeout configuration, or header-arrival deadline.
