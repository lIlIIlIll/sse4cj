# sse4cj

A greenfield, production-oriented Server-Sent Events (SSE) library for Cangjie.

## Status

The default client and server use the adjacent `../Wirestack` source dependency. The migration is verified on Linux x86_64 with Cangjie `1.1.0-alpha.20260829040003` and CJPM `1.1.3`: 91 tests pass, including offline TLS and HTTP/2 cases. The repository does **not** claim `COMPLETE` until every completion gate in [`SPEC.md`](SPEC.md) is satisfied.

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

The Wirestack server adapter uses `Immediate` request-scoped abort: HTTP/1 cancels the request's connection; HTTP/2 cancels only that stream. Its pull body retains an entire frame's budget until the next nonempty provider read acknowledges the preceding network write, or actual body close releases it. Graceful server shutdown also waits on Wirestack's protocol drain; application body close alone does not prove HTTP/2 END_STREAM delivery.

See [`doc/transport-close-contract.md`](doc/transport-close-contract.md) for the precise contract and capability rules.

## Breaking network API

- `WirestackSseClientTransport()` is the sole built-in client backend and the EventSource default.
- `SseHttpRequest` and `EventSourceConfig` use `openTimeoutMillis` (default 10,000): DNS, TCP, TLS, request send, and response headers. `readTimeoutMillis` (default 120,000) applies separately to each active nonempty body read; callback/consumer idle time does not spend that budget.
- `SseServerConfig` accepts only `listenEndpoint: wirestack.http.SocketEndpoint` (default `127.0.0.1:0`) and `maxConnections` (default 1024, range `1..65536`).
- Server address/port string configuration, server read/write/header timeout settings, and `SseServerRequest.remoteAddress` are removed. There is **no finite header-arrival deadline** and no built-in peer-IP admission field. Wirestack's finite parser/header and connection limits remain enabled.
- Default HTTPS clients verify system trust and hostname. The default server is plaintext HTTP/1.1; the package-internal TLS injection used by tests is not a public server TLS configuration API.

There are no legacy aliases or backend switches.

## Local development prerequisite

Install a compatible Cangjie SDK and place the Wirestack checkout beside this repository. Record the actual environment:

```bash
pwd
git status --short
cjc -v
cjpm -v
echo "$CANGJIE_HOME"
```

Build Wirestack's native dependencies using its own scripts (run from `../Wirestack`):

```bash
python3 tools/build_linux_tls_provider.py --offline
python3 tools/build_linux_resolver.py --quiet
```

Then run from `sse4cj` with the SDK activated:

```bash
env -u CANGJIE_STDX_PATH cjpm build
env -u CANGJIE_STDX_PATH cjpm test --parallel 1 --timeout-each=30s
```

The migration run reports `cjpm build success` and `PASSED: 91, SKIPPED: 0, ERROR: 0, FAILED: 0`. An external executable using only the default SSE server/client also reports `wirestack-smoke: PASS`.

`cjpm build` uses this package's `-O2` option. CJPM 1.1.3 does not support `cjpm build --release`; that command is not an additional successful release gate. Native FFI/link configuration belongs to Wirestack's manifest, not this package.

Migration quality-gate results in that environment:

- `cjpm check`, `cjpm build`, the 91-test suite, formatting of migrated sources, and `git diff --check` succeed without the stdx environment variable.
- The production build reports three unused-function warnings in the Wirestack dependency; none are suppressed.
- `cjlint` runs successfully but is **not clean**: 189 suggestions and five mandatory public-URL diagnostics. All five flag the offline `example.com` TLS fixtures, whose injected resolvers map exclusively to loopback. The report is generated at `target/cjlint-report.json`; these findings are not suppressed.
- `cjpm test --coverage` also passes all 91 tests, and `cjcov` generates `target/coverage/index.html` and `coverage.json`. The compiler warns that coverage should run without optimization because this package uses `-O2`; these reports are not a precise unoptimized coverage certification.

These checks do not satisfy the separate release-benchmark and profiler-backed optimization completion requirements.

## Quality bar

The project is not `COMPLETE` until it has real asserted protocol, property, integration and concurrency tests; bounded resource guarantees; cancellation/cleanup verification; release benchmarks with retained raw results; profiler-backed optimization; and complete protocol/security/performance documentation.

See [`SPEC.md`](SPEC.md) for the full completion checklist and final report format.
