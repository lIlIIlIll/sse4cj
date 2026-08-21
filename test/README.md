# Test strategy

Tests are part of the implementation contract, not examples.

Required suites:

- `wire`: protocol fixtures for LF/CR/CRLF, BOM, UTF-8, fields, comments, data, id, retry and EOF.
- `client`: finite reader and reconnecting EventSource behavior using mock transport and loopback HTTP.
- `server`: ownership, admission, deregistration, single writer, backpressure, heartbeat and shutdown.
- `integration`: real local transport interoperability and immediate streaming/flush behavior.
- `concurrency`: send/close, broadcast/shutdown, slow-consumer and ordering races.
- `property`: fixed-seed random frames, chunking, invalid bytes and limit violations.

A simple correctness-first reference decoder must live in test code and must not share the optimized decoder's parsing core. Differential tests must feed identical inputs using chunk sizes `1, 2, 3, 7, 8, 31, 64, 255, 1024, 4096`, plus whole-input mode and all single split points for short fixtures.

No test may access the public Internet, depend on a fixed public IP, or use arbitrary sleeps to guess readiness. Reconnect timing tests should use a fake clock/sleeper.
