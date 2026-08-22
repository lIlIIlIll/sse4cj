# sse4cj

A greenfield, production-oriented Server-Sent Events (SSE) library for Cangjie.

## Status

Implementation is in progress and is compiled against the active Cangjie/stdx toolchain. The repository does **not** claim `COMPLETE` until every completion gate in [`SPEC.md`](SPEC.md) is satisfied.

The project is intentionally independent from `eventsource4cj`: no compatibility layer, copied implementation, inherited type design, or inherited error semantics are permitted.

The normative implementation requirements are in [`SPEC.md`](SPEC.md). Agent/Codex execution rules are in [`AGENTS.md`](AGENTS.md).

## Core separation

The library keeps these concerns distinct:

1. `SseDecoder` / `SseEncoder` — incremental SSE wire codec.
2. `SseStreamReader` — one finite HTTP SSE response; EOF is normal completion and never reconnects.
3. EventSource client semantics — long-lived reconnecting SSE with Last-Event-ID/retry/cancellation.
4. `SseServer` / `SseHub` / `SseConnection` — server ownership, single-writer delivery, bounded backpressure, heartbeat, replay and shutdown.

## Connection close semantics

Server connections use a capability-aware two-phase close model:

```text
logical close
    -> immediate Hub eviction
    -> reject future enqueue
    -> drop pending queue
    -> do not start another write

transport close
    -> current in-flight write has actually returned/failed
    -> writer response lifecycle is finished
```

Frame and byte limits include both queued and in-flight application frames. A blocked write therefore does not silently free the queue budget for another full payload behind it.

The current stdx HTTP server adapter does not expose a public per-response abort primitive. It is treated as `ServerWideOnly`: logical eviction is immediate, while a write already in progress may only physically terminate when the write returns/fails, the peer disconnects, or the owning server is closed.

See [`doc/transport-close-contract.md`](doc/transport-close-contract.md) for the precise contract and capability rules.

## Local development prerequisite

Use the real installed SDK/stdx environment:

```bash
pwd
git status --short
cjc -v
cjpm -v
echo "$CANGJIE_HOME"
echo "$CANGJIE_STDX_PATH"
```

Compile probes and tests against the current toolchain rather than assuming APIs from an older SDK or another repository.

## Quality bar

The project is not `COMPLETE` until it has real asserted protocol, property, integration and concurrency tests; bounded resource guarantees; cancellation/cleanup verification; release benchmarks with retained raw results; profiler-backed optimization; and complete protocol/security/performance documentation.

See [`SPEC.md`](SPEC.md) for the full completion checklist and final report format.
