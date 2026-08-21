# sse4cj

A greenfield, production-oriented Server-Sent Events (SSE) library for Cangjie.

## Status

Repository bootstrap only. The SSE implementation itself has **not** been claimed complete or SDK-verified yet.

The project is intentionally independent from `eventsource4cj`: no compatibility layer, copied implementation, inherited type design, or inherited error semantics are permitted.

The normative implementation requirements are in [`SPEC.md`](SPEC.md). Agent/Codex execution rules are in [`AGENTS.md`](AGENTS.md).

## Target capabilities

The completed library will keep four concerns separate:

1. `SseDecoder` / `SseEncoder` — incremental SSE wire codec.
2. `SseStreamReader` — one finite HTTP SSE response; EOF is normal completion and never reconnects.
3. `EventSourceClient` — long-lived EventSource semantics with Last-Event-ID, retry, reconnect, HTTP 204 stop and cancellation.
4. `SseServer` / `SseHub` — server connections, single-writer fan-out, heartbeat, bounded backpressure, replay and shutdown.

## Local implementation prerequisite

Before writing SDK-dependent code, run in the real development environment:

```bash
pwd
git status --short
cjc -v
cjpm -v
echo "$CANGJIE_HOME"
echo "$CANGJIE_STDX_PATH"
```

Then inspect the installed Cangjie SDK/stdx source and compile minimal probes for the actual HTTP client/server streaming API, flush behavior, response-body ownership, socket/cancellation primitives, Future/spawn, synchronization, bounded queues, monotonic clock, Timer and UTF-8 facilities.

Do not infer these APIs from an older SDK or another repository. If documentation and actual compilation differ, the current installed toolchain is authoritative.

## Bootstrap layout

```text
sse4cj/
├── cjpm.toml
├── README.md
├── SPEC.md
├── AGENTS.md
├── CHANGELOG.md
├── src/
│   └── package.cj
├── test/
│   └── README.md
├── benchmark/
│   └── README.md
├── examples/
│   └── README.md
└── doc/
    ├── design.md
    ├── protocol-compliance.md
    ├── performance.md
    └── security.md
```

The physical Cangjie source layout may be adjusted after inspecting current package rules, but wire/client/server/observability responsibilities must stay separated.

## Quality bar

The project is not `COMPLETE` until it has real asserted protocol, property, integration and concurrency tests; bounded resource guarantees; cancellation/cleanup verification; release benchmarks with retained raw results; profiler-backed optimization; and complete protocol/security/performance documentation.

See [`SPEC.md`](SPEC.md) for the full completion checklist and final report format.
