# AGENTS.md

## Mission

Build `sse4cj` as a greenfield, production-grade SSE implementation for Cangjie. `SPEC.md` is the normative project specification for this repository.

## Non-negotiable boundaries

- Do not modify, vendor, copy, or mechanically translate `eventsource4cj`.
- Do not preserve compatibility with any previous SSE API.
- Do not claim support for behavior that has not been compiled and tested in the active Cangjie SDK/stdx environment.
- Protocol correctness, bounded resource use, cancellation safety, and measurable performance take priority over API compatibility.
- Keep finite-response SSE (`SseStreamReader`) separate from reconnecting EventSource semantics (`EventSourceClient`).
- No process-global server/Hub registries.

## First action in every implementation session

Before writing SDK-dependent code, record the real environment:

```bash
pwd
git status --short
cjc -v
cjpm -v
echo "$CANGJIE_HOME"
echo "$CANGJIE_STDX_PATH"
```

Inspect the installed SDK/stdx implementation for the actual HTTP client/server streaming, flush, cancellation, timer, monotonic clock, synchronization, queue, socket, and UTF-8 APIs. If documentation and compilation disagree, compilation against the current SDK wins.

## Module boundaries

Keep these responsibilities separate even if physical files must be flattened for Cangjie package rules:

- `wire`: event/frame model, incremental UTF-8, decoder, encoder, limits, typed errors.
- `client`: finite stream reader, reconnecting EventSource state machine, dispatcher, transport abstraction.
- `server`: endpoint/server ownership, Hub, connection writer loop, backpressure, replay, transport abstraction.
- `observability`: metrics/diagnostic hooks only; never hidden println/stack traces.
- `test`: reference decoder, fixtures, property/differential, integration, concurrency.
- `benchmark`: decoder, encoder, fan-out, loopback and retained raw results.

## Wire implementation rules

- Decoder must be incremental and O(n).
- Do not implement parsing with whole-response `String`, `String.lines()`, generic `split(":")`, `startsWith("data")`, regexes, or searching for `"\n\n"`.
- LF, CR, and CRLF are equivalent line endings, including cross-chunk CRLF.
- UTF-8 code points may cross feed boundaries.
- Only one BOM at the start of the stream is ignored.
- Field names are exact and case-sensitive: `event`, `data`, `id`, `retry`.
- An `id:` block may update Last-Event-ID even when no business event is emitted.
- EOF does not substitute for the terminating blank line.
- All decoder buffers have explicit finite limits with overflow-safe accounting.

## Encoder rules

- Canonical output uses UTF-8 and LF.
- Preserve absent vs present-empty fields.
- Reject CR/LF injection in event type and CR/LF/NUL in id.
- Split logical data/comment lines deliberately.
- Encode a logical frame once and share immutable encoded bytes across fan-out.

## Concurrency and server rules

- One writer task/fiber owns each response writer.
- Producers enqueue immutable encoded frames; they do not write sockets.
- Queue limits cover both frame count and byte count.
- Default slow-consumer behavior is disconnect, unless the final public API documents another bounded policy.
- Never hold registry locks during network I/O, user callbacks, or user predicates.
- Authentication/admission completes before a connection becomes visible in a Hub.
- Every connection path must deregister and release timers/tasks/resources exactly once.
- Replay must not race ahead of, or be overtaken by, later live events.

## Client rules

- `SseStreamReader`: EOF is normal completion and never reconnects.
- `EventSourceClient`: recoverable EOF/network failure may reconnect; HTTP 204 closes permanently.
- Reconnect sleep must be cancellable and based on a monotonic clock.
- `close()` is idempotent and interrupts connect/read/sleep, prevents later callbacks/reconnects, and releases resources.
- User callbacks never execute while internal locks are held.
- Async callback dispatch, when provided, is bounded.

## Security rules

Do not leak Authorization, Cookie, Proxy-Authorization, or complete sensitive query strings in diagnostics. Protect against CRLF/NUL injection, unbounded lines/events/data, integer overflow, byte-unbounded queues, slow-consumer fan-out blocking, ghost connections, callback cleanup loss, reconnect storms, and replay gaps.

## Testing rules

- Tests must have assertions and deterministic termination.
- Do not access the public Internet in tests.
- Do not depend on fixed public IPs.
- Do not use arbitrary sleeps to guess readiness; use synchronization.
- Keep a simple reference decoder independent of optimized decoder internals.
- Differential and chunk-boundary tests are mandatory.
- Property tests use fixed/reported seeds.
- Use fake clock/sleeper for reconnect timing tests.
- Loopback tests must prove streaming flush behavior.

## Performance rules

- Measure before introducing complex pools/caches.
- Do not repeatedly scan accumulated decoder buffers.
- Do not allocate per input byte.
- Do not copy large payloads once per subscriber.
- Fan-out should enqueue, not write sockets.
- Benchmarks run optimized builds, warm up, use multiple samples, retain raw JSON/CSV, and report median/tail latency.
- At least one optimization must be justified by profiler evidence before claiming performance completion.

## Quality gates

Run every command that exists in the active environment and record real output:

```bash
cjpm check
cjpm build
cjpm test
cjpm build --release
cjfmt
cjlint
cjcov
git diff --check
```

Do not hide warnings globally. If a tool is unavailable, record that fact instead of inventing a pass.

## Completion

Do not report `COMPLETE` until all completion conditions in `SPEC.md` hold, including real tests, real release benchmarks, profiler-backed optimization, documentation, resource/cancellation guarantees, and verified SDK-dependent transports. Otherwise report `INCOMPLETE` with precise blockers and continue completing all independent work that remains possible.

## Code Review Rules

- Codex 审查必须以本仓库的 `SPEC.md`、当前 SDK 实测能力和源码为准。阻塞性意见应给出能到达的调用序列、确切违反的 SSE/资源/取消契约以及测试反例；不要把偏好或未证实推测伪装成缺陷。
- Decoder/encoder 修改时，对同一根因一次检查 LF/CR/CRLF 的跨 chunk 边界、UTF-8 拆分、开头 BOM、空字段与缺失字段、Last-Event-ID、EOF、NUL/换行注入、超限。核对参考 decoder 与优化 decoder 的等价性。
- Client/reconnect 修改时，覆盖正常 EOF 与可重连失败、HTTP 204、deadline、取消、close 幂等、无锁用户 callback、重连风暴；不能把完成和取消合并为一个表面成功状态。
- Server/Hub 修改时检查单 writer、队列 frame/byte 预算、in-flight accounting、replay-live 顺序、backpressure、资源清理、慢消费与锁外 I/O。无需将与本 PR 无关的所有组合列为阻塞。
- 发现同根因的正反、对称、空值和竞争情形应尽量一次检查并合并报告；可见历史 Review Thread 时先去重，复审聚焦修复及受影响调用链，同时保留新发现的真实阻塞缺陷。

## Review 反馈处理与收敛

- 按根因与不变量汇总 Review，核对现有 `SPEC.md` 与生产路径，再集中修复并补差分/参数化、可控时钟/同步测试；不要对每条建议独立 Push。
- 对仍阻塞、已修复、附证据不采纳、范围外独立改进分别记录证据；只声称确实运行的编译、fixture、真实网络/平台与 CI 结果。
- 检查最后一个 HEAD 的真实 Codex Review 完成证据；`AGENTS.md` 不会自动改变 Review Trigger。仍遵守原有 `SPEC.md` 的 COMPLETE/INCOMPLETE 要求，不因固定轮数或 Thread 清零降低门禁。
