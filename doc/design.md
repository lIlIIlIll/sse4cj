# Design

Status: bootstrap only. No SDK-dependent architecture is considered verified yet.

The implementation must preserve the responsibility boundaries in `SPEC.md`:

1. wire codec;
2. finite SSE response reader;
3. reconnecting EventSource client;
4. SSE server/Hub.

Before filling this document, inspect the active Cangjie SDK/stdx APIs and record the actual primitives used for HTTP streaming, flush, cancellation, timers, monotonic time, synchronization, queues, sockets, and UTF-8 decoding.

Required final sections:

- wire state machine;
- incremental UTF-8;
- client state machine;
- server ownership;
- connection lifecycle;
- single-writer model;
- backpressure;
- replay/live ordering;
- cancellation;
- graceful shutdown;
- HTTP/1.1 and HTTP/2 behavior;
- explicit memory bounds.
