package sse4cj

import std.collection.*
import std.sync.*
import std.time.*

private class RealLoopbackSink <: SseDecodeSink {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) { mutex.condition() }
    private let events = ArrayList<SseEvent>()

    public func onEvent(event: SseEvent): Unit {
        synchronized(mutex) {
            events.add(event)
            condition.notifyAll()
        }
    }

    public func onLastEventIdChanged(_id: String): Unit { let _ = _id }
    public func onRetryChanged(_retryMillis: Int64): Unit { let _ = _retryMillis }

    func awaitEvent(timeoutMillis!: Int64 = 2000): Option<SseEvent> {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (events.size == 0) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                condition.wait(timeout: remaining)
            }
            if (events.size == 0) { None<SseEvent> } else { Some(events[0]) }
        }
    }
}

private class CompletionLatch {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) { mutex.condition() }
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
                if (remaining <= Duration.Zero) { break }
                condition.wait(timeout: remaining)
            }
            completed
        }
    }
}

private func functionalDelay(delayMillis: Int64): Unit {
    let mutex = Mutex()
    let condition = synchronized(mutex) { mutex.condition() }
    synchronized(mutex) { condition.wait(timeout: Duration.millisecond * delayMillis) }
}

private func largeAsciiData(size: Int64): String {
    String.fromUtf8(Array<Byte>(size, repeat: b'x'))
}

@Test
func stdxRealLoopbackStreamsEventBeforeResponseClose() {
    let endpoint = SseEndpoint("/events", config: SseEndpointConfig(initialComment: Some("open")))
    let server = SseServer(config: SseServerConfig(address: "127.0.0.1", port: 0, writeTimeoutMillis: 5000))
    server.register(endpoint)
    server.start()
    server.awaitBound()

    let transport = StdxSseClientTransport()
    let response = transport.send(
        SseHttpRequest("http://127.0.0.1:${server.boundPort}/events", readTimeoutMillis: 5000),
        None<SseCancellationToken>
    )
    @Assert(response.status(), 200)
    @Assert(endpoint.hub.connectionCount, 1)

    let sink = RealLoopbackSink()
    let readerDone = CompletionLatch()
    let reader: Future<Unit> = spawn {
        => try {
            SseStreamReader().read(response.body(), sink)
        } catch (_: SseException) {
            // Deliberate server shutdown may surface as EOF or transport failure.
        } finally { readerDone.complete() }
    }

    let stats = endpoint.hub.broadcast(SseEncoder.encode(SseFrame(data: Some("live"))))
    @Assert(stats.accepted, 1)
    match (sink.awaitEvent()) {
        case Some(event) => @Assert(event.data, "live")
        case None => @Fail("real loopback event was not streamed")
    }
    @Assert(endpoint.hub.connectionCount, 1)

    server.closeNow()
    @Assert(readerDone.await(2000))
    reader.get()
    response.close()
    @Assert(endpoint.hub.connectionCount, 0)
}

@Test
func nonReadingClientIsLogicallyEvictedByBufferedLimit() {
    let connectionConfig = SseConnectionConfig(
        maxQueuedFrames: 8,
        maxQueuedBytes: 2 * 1024 * 1024,
        backpressure: SseBackpressureStrategy.DisconnectSlowConsumer,
        transportAbortCapability: SseTransportAbortCapability.ServerWideOnly
    )
    let endpoint = SseEndpoint(
        "/events",
        config: SseEndpointConfig(connection: connectionConfig, initialComment: Some("open"))
    )
    let server = SseServer(config: SseServerConfig(address: "127.0.0.1", port: 0, writeTimeoutMillis: 60_000))
    server.register(endpoint)
    server.start()
    server.awaitBound()

    let response = StdxSseClientTransport().send(
        SseHttpRequest("http://127.0.0.1:${server.boundPort}/events", readTimeoutMillis: 60_000),
        None<SseCancellationToken>
    )
    @Assert(response.status(), 200)
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

    // Logical eviction must not require the client to read or close first.
    let closeDone = CompletionLatch()
    let closeTask: Future<Unit> = spawn { => try { server.closeNow() } finally { closeDone.complete() } }
    let serverClosedPromptly = closeDone.await(2000)
    if (!serverClosedPromptly) {
        // Cleanup path for an SDK that cannot interrupt the server-wide connection.
        response.close()
        let _ = closeDone.await(2000)
    }
    @Assert(serverClosedPromptly)
    closeTask.get()
    response.close()
}

@Test
func activeSseConnectionSurvivesConfiguredWriteTimeoutWindow() {
    let endpoint = SseEndpoint(
        "/events",
        config: SseEndpointConfig(
            connection: SseConnectionConfig(
                maxQueuedFrames: 64,
                maxQueuedBytes: 1024 * 1024,
                transportAbortCapability: SseTransportAbortCapability.ServerWideOnly
            ),
            initialComment: Some("open")
        )
    )
    let server = SseServer(config: SseServerConfig(address: "127.0.0.1", port: 0, writeTimeoutMillis: 200))
    server.register(endpoint)
    server.start()
    server.awaitBound()

    let response = StdxSseClientTransport().send(
        SseHttpRequest("http://127.0.0.1:${server.boundPort}/events", readTimeoutMillis: 2000),
        None<SseCancellationToken>
    )
    @Assert(response.status(), 200)
    let sink = RealLoopbackSink()
    let reader: Future<Unit> = spawn {
        => try { SseStreamReader().read(response.body(), sink) }
        catch (_: SseException) {
            // Deliberate server shutdown may surface as EOF or transport failure.
        }
    }

    // Keep the connection actively flowing well beyond the configured 200 ms.
    for (index in 0..8) {
        let stats = endpoint.hub.broadcast(SseEncoder.encode(SseFrame(data: Some("tick-${index}"))))
        @Assert(stats.accepted, 1)
        functionalDelay(50)
    }

    @Assert(endpoint.hub.connectionCount, 1)
    server.closeNow()
    reader.get()
    response.close()
}

@Test
func slowReadingClientIsEvictedByBufferPressureNotResponseTimer() {
    let connectionConfig = SseConnectionConfig(
        maxQueuedFrames: 8,
        maxQueuedBytes: 1024 * 1024,
        backpressure: SseBackpressureStrategy.DisconnectSlowConsumer,
        transportAbortCapability: SseTransportAbortCapability.ServerWideOnly
    )
    let endpoint = SseEndpoint(
        "/events",
        config: SseEndpointConfig(connection: connectionConfig, initialComment: Some("open"))
    )
    // A deliberately tiny configured timeout is retained only to prove that the
    // SSE adapter does not install stdx's unsafe response-lifetime write timer.
    let server = SseServer(config: SseServerConfig(address: "127.0.0.1", port: 0, writeTimeoutMillis: 100))
    server.register(endpoint)
    server.start()
    server.awaitBound()

    let response = StdxSseClientTransport().send(
        SseHttpRequest("http://127.0.0.1:${server.boundPort}/events", readTimeoutMillis: 5000),
        None<SseCancellationToken>
    )
    @Assert(response.status(), 200)

    let firstRead = CompletionLatch()
    let bytesRead = AtomicInt64(0)
    let slowReader: Future<Unit> = spawn {
        => let buffer = Array<Byte>(1024, repeat: 0)
        try {
            while (true) {
                let count = response.body().read(buffer)
                if (count <= 0) { break }
                bytesRead.fetchAdd(count)
                firstRead.complete()
                functionalDelay(5)
            }
        } catch (_: Exception) {
            // Expected when server-wide shutdown closes the streaming response.
        }
    }

    @Assert(firstRead.await(1000))
    functionalDelay(150)
    @Assert(endpoint.hub.connectionCount, 1)

    let frame = SseEncoder.encode(SseFrame(data: Some(largeAsciiData(128 * 1024))))
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

    server.closeNow()
    response.close()
    slowReader.get()
}
