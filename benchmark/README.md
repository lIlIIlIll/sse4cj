# Benchmarks

Benchmarks must run against an optimized/release build, use deterministic inputs, warm up before measurement, collect multiple samples, and retain raw JSON or CSV in `benchmark/results/`.

Required benchmark programs:

- `decoder_bench.cj`: payloads 32 B, 256 B, 1 KiB, 64 KiB, 1 MiB; ASCII, Chinese UTF-8, multiline data, comment-heavy, id/retry-heavy; chunk sizes 1, 7, 64, 1024, 4096, 16384 and whole input.
- `encoder_bench.cj`: small, multiline, Chinese UTF-8, 64 KiB and 1 MiB data.
- `fanout_bench.cj`: 1, 10, 100 and 1000 fake-writer clients; broadcast/s, deliveries/s, P50/P95/P99 enqueue latency, encoded-frame count and drop/disconnect count.
- `loopback_bench.cj`: sustained single connection, 100-client broadcast and first-event latency.

The fan-out benchmark must instrument encoding so it proves one logical broadcast is encoded once, and it must include a permanently slow consumer to prove isolation.

Do not add object pools or similar complexity without profiler evidence. At least one final optimization must be backed by a before/after profile and benchmark result.
