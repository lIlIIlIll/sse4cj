# Performance

No performance claims are valid yet.

The finished project must retain reproducible release-mode results for decoder, encoder, fan-out, and loopback benchmarks. Record SDK/compiler version, OS, CPU/hardware context, benchmark inputs, warm-up, sample count, and raw JSON/CSV output.

Required evidence:

- decoder: MB/s, events/s, ns/event, median and P95 across specified event/chunk sizes;
- encoder: small, multiline, UTF-8, 64 KiB and 1 MiB payload cases;
- fan-out: 1/10/100/1000 clients, P50/P95/P99 enqueue latency, delivery rate, drop/disconnect counts;
- instrumentation proving one logical broadcast frame is encoded once;
- a permanently slow consumer that does not stall unrelated consumers;
- loopback: sustained stream, 100-client broadcast, first-event latency and P50/P95/P99;
- at least one profiler-backed optimization with before/after numbers.

Do not report fabricated allocation counts or extrapolate from a single run.
