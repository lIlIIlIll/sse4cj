package sse4cj

import std.collection.*
import std.sync.*
import std.time.*
import wirestack.http.*

private class RealLoopbackSink <: SseDecodeSink {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private let events = ArrayList<SseEvent>()

    public func onEvent(event: SseEvent): Unit {
        synchronized(mutex) {
            events.add(event)
            condition.notifyAll()
        }
    }

    public func onLastEventIdChanged(_id: String): Unit {
        let _ = _id
    }
    public func onRetryChanged(_retryMillis: Int64): Unit {
        let _ = _retryMillis
    }

    func awaitCount(expected: Int64, timeoutMillis!: Int64 = 2000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (events.size < expected) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                condition.wait(timeout: remaining)
            }
            events.size >= expected
        }
    }

    func eventAt(index: Int64): SseEvent {
        synchronized(mutex) {
            events[index]
        }
    }
}

private class CompletionLatch {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private var completed = false

    func complete(): Unit {
        synchronized(mutex) {
            completed = true
            condition.notifyAll()
        }
    }

    func await(timeoutMillis: Int64): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!completed) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                condition.wait(timeout: remaining)
            }
            completed
        }
    }
}

private class FixedAdmission <: SseAdmissionHandler {
    private let status: UInt16

    init(status: UInt16) {
        this.status = status
    }

    public func admit(_request: SseServerRequest): SseAdmissionResult {
        let _ = _request
        SseAdmissionResult.reject(status: status)
    }
}

private class ThrowingAdmission <: SseAdmissionHandler {
    public func admit(_request: SseServerRequest): SseAdmissionResult {
        let _ = _request
        throw Exception("deliberate admission failure")
    }
}

private func functionalDelay(delayMillis: Int64): Unit {
    let mutex = Mutex()
    let condition = synchronized(mutex) {
        mutex.condition()
    }
    synchronized(mutex) {
        condition.wait(timeout: Duration.millisecond * delayMillis)
    }
}

private func loopbackEndpoint(port: UInt16): SocketEndpoint {
    SocketEndpoint(IpAddress(IpAddressFamily.Ipv4, [127u8, 0u8, 0u8, 1u8]), port)
}

private func serverUrl(server: SseServer, target: String): String {
    "http://127.0.0.1:${server.boundPort}${target}"
}

private func startServer(endpoints: Array<SseEndpoint>, maxConnections!: Int64 = 1024): SseServer {
    let server = SseServer(config: SseServerConfig(listenEndpoint: loopbackEndpoint(0), maxConnections: maxConnections))
    for (endpoint in endpoints) {
        server.register(endpoint)
    }
    server.start()
    server.awaitBound()
    server
}

private func largeAsciiData(size: Int64): String {
    String.fromUtf8(Array<Byte>(size, repeat: b'x'))
}

private func awaitEndpointClosing(endpoint: SseEndpoint, timeoutMillis!: Int64 = 2000): Bool {
    let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
    synchronized(endpoint.closeMutex) {
        while (!endpoint.isClosing) {
            let remaining = deadline - MonoTime.now()
            if (remaining <= Duration.Zero) {
                break
            }
            endpoint.closeChanged.wait(timeout: remaining)
        }
        endpoint.isClosing
    }
}

private func rawStatus(client: HttpClient, method: String, url: String): Int64 {
    let response = client.send(HttpRequest(HttpMethod(method), HttpUrl.parse(url), retrySafety: HttpRetrySafety.Disabled))
    var status: Int64 = 0
    try {
        status = response.status
    } finally {
        response.close()
    }
    status
}

private func awaitBodyTerminal(response: SseTransportResponse): Bool {
    let buffer = Array<Byte>(4096, repeat: 0u8)
    var terminal = false
    try {
        while (!terminal) {
            terminal = response.body().read(buffer) == 0
        }
    } catch (_: Exception) {
        terminal = true
    }
    terminal
}

@Test
func wirestackRealLoopbackStreamsEventBeforeResponseClose() {
    let endpoint = SseEndpoint("/events", config: SseEndpointConfig(initialComment: Some("open")))
    let server = startServer([endpoint])
    try {
        let response = WirestackSseClientTransport().send(
            SseHttpRequest(serverUrl(server, "/events"), openTimeoutMillis: 2000, readTimeoutMillis: 5000),
            None<SseCancellationToken>
        )
        try {
            @Assert(response.status(), 200)
            @Assert(endpoint.hub.connectionCount, 1)

            let sink = RealLoopbackSink()
            let readerDone = CompletionLatch()
            let reader: Future<Unit> = spawn {
                => try {
                    SseStreamReader().read(response.body(), sink)
                } catch (_: SseException) {
                // Explicit shutdown may surface as EOF or transport cancellation.
                } finally {
                    readerDone.complete()
                }
            }

            let stats = endpoint.hub.broadcast(SseFrame(data: Some("live")))
            @Assert(stats.accepted, 1)
            @Assert(sink.awaitCount(1))
            @Assert(sink.eventAt(0).data, "live")
            @Assert(endpoint.hub.connectionCount, 1)

            server.closeNow()
            @Assert(readerDone.await(2000))
            reader.get(Duration.second)
        } finally {
            response.close()
        }
    } finally {
        server.closeNow()
    }
}

@Test
func wirestackEmptyStreamWithoutHeartbeatForceClosesPromptly() {
    let endpoint = SseEndpoint("/empty", config: SseEndpointConfig(initialComment: None<String>))
    let server = startServer([endpoint])
    try {
        let response = WirestackSseClientTransport().send(
            SseHttpRequest(serverUrl(server, "/empty"), openTimeoutMillis: 2000, readTimeoutMillis: 5000),
            None<SseCancellationToken>
        )
        try {
            @Assert(response.status(), 200)
            @Assert(endpoint.hub.connectionCount, 1)
            let readStarted = CompletionLatch()
            let readDone = CompletionLatch()
            let readTask: Future<Bool> = spawn {
                =>
                    readStarted.complete()
                    try {
                        awaitBodyTerminal(response)
                    } finally {
                        readDone.complete()
                    }
            }
            @Assert(readStarted.await(1000))
            @Assert(readTask.tryGet().isNone())

            server.closeNow()
            @Assert(readDone.await(2000))
            @Assert(readTask.get(Duration.second))
            @Assert(endpoint.hub.connectionCount, 0)
        } finally {
            response.close()
        }
    } finally {
        server.closeNow()
    }
}

@Test
func wirestackEvictedConnectionReleasesPhysicalAdmissionSlot() {
    let endpoint = SseEndpoint(
        "/events",
        config: SseEndpointConfig(
            connection: SseConnectionConfig(
                maxQueuedFrames: 8,
                maxQueuedBytes: 2 * 1024 * 1024,
                backpressure: SseBackpressureStrategy.DisconnectSlowConsumer
            ),
            initialComment: Some("open")
        )
    )
    let server = startServer([endpoint], maxConnections: 1)
    try {
        let first = WirestackSseClientTransport().send(
            SseHttpRequest(serverUrl(server, "/events"), openTimeoutMillis: 2000, readTimeoutMillis: 5000),
            None<SseCancellationToken>
        )
        try {
            @Assert(first.status(), 200)
            @Assert(endpoint.hub.connectionCount, 1)

            let frame = SseEncoder.encode(SseFrame(data: Some(largeAsciiData(256 * 1024))))
            var disconnected = false
            for (_ in 0..512) {
                let stats = endpoint.hub.broadcast(frame)
                if (stats.disconnected > 0 || endpoint.hub.connectionCount == 0) {
                    disconnected = true
                    break
                }
            }
            @Assert(disconnected)
            @Assert(endpoint.hub.connectionCount, 0)

            // The client deliberately does not read until logical eviction. Seeing
            // the terminal body state proves request-scoped abort reached the wire.
            let firstTerminal: Future<Bool> = spawn {=> awaitBodyTerminal(first)}
            @Assert(firstTerminal.get(2 * Duration.second))

            let second = WirestackSseClientTransport().send(
                SseHttpRequest(serverUrl(server, "/events"), openTimeoutMillis: 2000, readTimeoutMillis: 5000),
                None<SseCancellationToken>
            )
            try {
                @Assert(second.status(), 200)
                @Assert(endpoint.hub.connectionCount, 1)
            } finally {
                second.close()
            }
        } finally {
            first.close()
        }
    } finally {
        server.closeNow()
    }
}

@Test
func wirestackReplayIsOrderedAcrossQueryRoutedHandoff() {
    let replay = MemoryReplayBuffer(maxFrames: 8, maxBytes: 1024 * 1024)
    let endpoint = SseEndpoint(
        "/events",
        config: SseEndpointConfig(initialComment: None<String>),
        replay: Some<ReplayProvider>(replay)
    )
    let _ = endpoint.hub.broadcast(SseFrame(id: Some("1"), data: Some("one")))
    let _ = endpoint.hub.broadcast(SseFrame(id: Some("2"), data: Some("two")))
    let _ = endpoint.hub.broadcast(SseFrame(id: Some("3"), data: Some("three")))

    let server = startServer([endpoint])
    try {
        let response = WirestackSseClientTransport().send(
            SseHttpRequest(
                serverUrl(server, "/events?cursor=1"),
                headers: [SseHeader("Last-Event-ID", "1")],
                openTimeoutMillis: 2000,
                readTimeoutMillis: 5000
            ),
            None<SseCancellationToken>
        )
        try {
            @Assert(response.status(), 200)
            let sink = RealLoopbackSink()
            let reader: Future<Unit> = spawn {
                => try {
                    SseStreamReader().read(response.body(), sink)
                } catch (_: SseException) {}
            }
            @Assert(sink.awaitCount(2))
            let stats = endpoint.hub.broadcast(SseFrame(id: Some("4"), data: Some("four")))
            @Assert(stats.accepted, 1)
            @Assert(sink.awaitCount(3))
            @Assert(sink.eventAt(0).lastEventId, "2")
            @Assert(sink.eventAt(0).data, "two")
            @Assert(sink.eventAt(1).lastEventId, "3")
            @Assert(sink.eventAt(1).data, "three")
            @Assert(sink.eventAt(2).lastEventId, "4")
            @Assert(sink.eventAt(2).data, "four")
            response.close()
            reader.get(2 * Duration.second)
        } finally {
            response.close()
        }
    } finally {
        server.closeNow()
    }
}

@Test
func wirestackRoutingAndAdmissionFailuresCreateNoGhostConnections() {
    let accepted = SseEndpoint("/events", config: SseEndpointConfig(initialComment: None<String>))
    let rejected = SseEndpoint(
        "/rejected",
        config: SseEndpointConfig(initialComment: None<String>),
        admission: Some<SseAdmissionHandler>(FixedAdmission(429u16))
    )
    let failed = SseEndpoint(
        "/failed",
        config: SseEndpointConfig(initialComment: None<String>),
        admission: Some<SseAdmissionHandler>(ThrowingAdmission())
    )
    let server = startServer([accepted, rejected, failed])
    let client = HttpClient
        .builder()
        .redirectPolicy(HttpRedirectPolicy.noFollow())
        .retryPolicy(HttpRetryPolicy(maximumAttempts: 1))
        .requestTimeout(2 * Duration.second)
        .build()
    try {
        @Assert(rawStatus(client, "GET", serverUrl(server, "/missing")), 404)
        @Assert(rawStatus(client, "POST", serverUrl(server, "/events")), 405)
        @Assert(rawStatus(client, "GET", serverUrl(server, "/rejected")), 429)
        @Assert(rawStatus(client, "GET", serverUrl(server, "/failed")), 500)
        @Assert(accepted.hub.connectionCount, 0)
        @Assert(rejected.hub.connectionCount, 0)
        @Assert(failed.hub.connectionCount, 0)
    } finally {
        client.close()
        server.closeNow()
    }
}

@Test
func wirestackGracefulCloseDeliversNormalEof() {
    let endpoint = SseEndpoint("/events", config: SseEndpointConfig(initialComment: Some("open")))
    let server = startServer([endpoint])
    try {
        let response = WirestackSseClientTransport().send(
            SseHttpRequest(serverUrl(server, "/events"), openTimeoutMillis: 2000, readTimeoutMillis: 5000),
            None<SseCancellationToken>
        )
        try {
            let readStarted = CompletionLatch()
            let reader: Future<Bool> = spawn {
                =>
                    readStarted.complete()
                    try {
                        SseStreamReader().read(response.body(), RealLoopbackSink())
                        true
                    } catch (_: SseException) {
                        false
                    }
            }
            @Assert(readStarted.await(1000))
            let graceful: Future<Unit> = spawn {=> server.closeGracefully(2000)}
            graceful.get(3 * Duration.second)
            @Assert(reader.get(2 * Duration.second))
            @Assert(endpoint.hub.connectionCount, 0)
        } finally {
            response.close()
        }
    } finally {
        server.closeNow()
    }
}

@Test
func wirestackGracefulDrainForceClosesBlockedTransport() {
    let endpoint = SseEndpoint(
        "/events",
        config: SseEndpointConfig(
            connection: SseConnectionConfig(maxQueuedFrames: 128, maxQueuedBytes: 64 * 1024 * 1024),
            initialComment: None<String>
        )
    )
    let server = startServer([endpoint])
    try {
        let response = WirestackSseClientTransport().send(
            SseHttpRequest(serverUrl(server, "/events"), openTimeoutMillis: 2000, readTimeoutMillis: 5000),
            None<SseCancellationToken>
        )
        try {
            let frame = SseEncoder.encode(SseFrame(data: Some(largeAsciiData(256 * 1024))))
            for (_ in 0..64) {
                let stats = endpoint.hub.broadcast(frame)
                @Assert(stats.accepted, 1)
            }

            let closeTask: Future<Unit> = spawn {=> server.closeGracefully(10)}
            closeTask.get(2 * Duration.second)
            @Assert(endpoint.hub.connectionCount, 0)

            let terminal: Future<Bool> = spawn {=> awaitBodyTerminal(response)}
            @Assert(terminal.get(2 * Duration.second))
        } finally {
            response.close()
        }
    } finally {
        server.closeNow()
    }
}

@Test
func wirestackCloseNowInterruptsInProgressGracefulShutdown() {
    let endpoint = SseEndpoint(
        "/events",
        config: SseEndpointConfig(
            connection: SseConnectionConfig(maxQueuedFrames: 256, maxQueuedBytes: 64 * 1024 * 1024),
            initialComment: None<String>
        )
    )
    let server = startServer([endpoint])
    try {
        let response = WirestackSseClientTransport().send(
            SseHttpRequest(serverUrl(server, "/events"), openTimeoutMillis: 2000, readTimeoutMillis: 5000),
            None<SseCancellationToken>
        )
        try {
            let frame = SseEncoder.encode(SseFrame(data: Some(largeAsciiData(256 * 1024))))
            for (_ in 0..128) {
                let stats = endpoint.hub.broadcast(frame)
                @Assert(stats.accepted, 1)
            }

            let graceful: Future<Unit> = spawn {=> server.closeGracefully(5000)}
            @Assert(awaitEndpointClosing(endpoint))
            @Assert(graceful.tryGet().isNone())

            let force: Future<Unit> = spawn {=> server.closeNow()}
            force.get(2 * Duration.second)
            graceful.get(2 * Duration.second)
            @Assert(endpoint.hub.connectionCount, 0)
        } finally {
            response.close()
        }
    } finally {
        server.closeNow()
    }
}

@Test
func wirestackSlowReadingClientIsEvictedByBufferPressure() {
    let endpoint = SseEndpoint(
        "/events",
        config: SseEndpointConfig(
            connection: SseConnectionConfig(
                maxQueuedFrames: 8,
                maxQueuedBytes: 2 * 1024 * 1024,
                backpressure: SseBackpressureStrategy.DisconnectSlowConsumer
            ),
            initialComment: Some("open")
        )
    )
    let server = startServer([endpoint])
    try {
        let response = WirestackSseClientTransport().send(
            SseHttpRequest(serverUrl(server, "/events"), openTimeoutMillis: 2000, readTimeoutMillis: 5000),
            None<SseCancellationToken>
        )
        try {
            @Assert(response.status(), 200)
            let firstRead = CompletionLatch()
            let bytesRead = AtomicInt64(0)
            let slowReader: Future<Unit> = spawn {
                =>
                    let buffer = Array<Byte>(1024, repeat: 0u8)
                    try {
                        while (true) {
                            let count = response.body().read(buffer)
                            if (count <= 0) {
                                break
                            }
                            bytesRead.fetchAdd(count)
                            firstRead.complete()
                            // Deliberate consumer throttling, not a readiness heuristic.
                            functionalDelay(5)
                        }
                    } catch (_: Exception) {
                        // Queue eviction aborts the request-scoped streaming response.
                    }
            }

            @Assert(firstRead.await(1000))
            let frame = SseEncoder.encode(SseFrame(data: Some(largeAsciiData(256 * 1024))))
            var disconnected = false
            for (_ in 0..512) {
                let stats = endpoint.hub.broadcast(frame)
                if (stats.disconnected > 0 || endpoint.hub.connectionCount == 0) {
                    disconnected = true
                    break
                }
            }
            @Assert(bytesRead.load() > 0)
            @Assert(disconnected)
            @Assert(endpoint.hub.connectionCount, 0)
            response.close()
            slowReader.get(2 * Duration.second)
        } finally {
            response.close()
        }
    } finally {
        server.closeNow()
    }
}
