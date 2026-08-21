# Security

Status: design requirements only; implementation is not yet verified.

The final implementation must explicitly cover:

- CR/LF injection in outbound event type and id;
- NUL handling in id;
- bounded line, field, data, event, and total buffered bytes;
- overflow-safe length accounting and retry parsing;
- bounded callback queues and per-connection frame+byte queues;
- slow-consumer isolation from broadcast fan-out;
- admission/authentication before Hub registration;
- cleanup when user callbacks fail;
- isolation between independent server instances;
- no callbacks or reconnects after close;
- response/task/timer cleanup on every terminal path;
- reconnect-storm prevention;
- redaction/avoidance of Authorization, Cookie, Proxy-Authorization, and complete sensitive query strings;
- replay-gap policy and at-least-once semantics.

Security claims must cite the implementing code and deterministic tests in `doc/protocol-compliance.md`.
