# Examples

This directory currently records example goals; it does not contain runnable CJPM example projects.

## Prerequisites

- Use a Cangjie 1.1.0-compatible SDK. The Wirestack cutover has been exercised with `cjc 1.1.0-alpha.20260829040003` and `cjpm 1.1.3` on Linux x86_64.
- Keep a Wirestack source checkout next to `sse4cj`. The package manifest resolves it through `wirestack = { path = "../Wirestack" }`.
- Build Wirestack's native TLS and resolver dependencies using the commands in the repository README. No stdx binary dependency is required for networking.

Other platforms and toolchain versions are not established by the current project evidence.

## Planned examples

Future runnable examples should cover:

- finite SSE response consumption for streaming APIs without provider-specific code;
- reconnecting `EventSourceClient`;
- a broadcast SSE server;
- a Last-Event-ID replay server.

Examples must use the current `SocketEndpoint`, `openTimeoutMillis`, `WirestackSseClientTransport`, and default Wirestack-backed `SseServer` APIs. A server example must state that the default listener is plaintext loopback and must not imply that TLS is enabled automatically. Examples supplement API and behavior checks; they do not replace them.
