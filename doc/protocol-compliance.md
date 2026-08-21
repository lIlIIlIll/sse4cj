# Protocol compliance

This matrix must be updated only from implemented, asserted, and locally verified behavior. `Planned` is not equivalent to supported.

| Requirement | Implementation | Tests | Status | Notes |
| --- | --- | --- | --- | --- |
| LF / CR / CRLF line endings | TBD | TBD | Planned | Includes cross-chunk CRLF |
| Incremental UTF-8 | TBD | TBD | Planned | Includes split code points and initial BOM |
| Exact field-name matching | TBD | TBD | Planned | `event`, `data`, `id`, `retry` only |
| Multi-line data | TBD | TBD | Planned | Preserve logical newlines |
| Last-Event-ID state | TBD | TBD | Planned | Includes empty-id reset and id-only blocks |
| retry parsing | TBD | TBD | Planned | ASCII digits, non-negative Int64 |
| EOF semantics | TBD | TBD | Planned | EOF does not dispatch unterminated event |
| Finite stream EOF | TBD | TBD | Planned | Normal completion, no reconnect |
| EventSource reconnect | TBD | TBD | Planned | Recoverable EOF/network errors |
| HTTP 204 stop | TBD | TBD | Planned | Permanent close |
| MIME essence parsing | TBD | TBD | Planned | Accept parameters such as charset=utf-8 |
| Server single writer | TBD | TBD | Planned | One writer per response |
| Bounded backpressure | TBD | TBD | Planned | Frames and bytes |
| Replay/live ordering | TBD | TBD | Planned | No gap/reordering at transition |
