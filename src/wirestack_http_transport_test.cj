package sse4cj

import std.collection.*
import std.io.InputStream
import std.sync.*
import std.time.*
import wirestack.http.*

private const WIRESTACK_TEST_QUERY_SECRET: String = "query-secret-7349"
private const WIRESTACK_TEST_HEADER_SECRET: String = "header-secret-8821"

private func wirestackTestLoopback(port: UInt16): SocketEndpoint {
    SocketEndpoint(IpAddress(IpAddressFamily.Ipv4, [127u8, 0u8, 0u8, 1u8]), port)
}

private class WirestackCountingResolver <: Resolver {
    private let delegate: StaticResolver
    let calls = AtomicInt64(0)

    init(address: IpAddress) {
        delegate = StaticResolver(
            ResolveResult(
                [address],
                HostName("example.com"),
                ResolverSource.Static
            )
        )
    }

    public func resolve(
        host: HostName,
        service: ?String,
        options: ResolveOptions,
        context: OperationContext
    ): ResolveResult {
        calls.fetchAdd(1)
        delegate.resolve(host, service, options, context)
    }
}

private class WirestackTestLatch {
    private let mutex = Mutex()
    private let changed = synchronized(mutex) {
        mutex.condition()
    }
    private var signalled = false

    func signal(): Unit {
        synchronized(mutex) {
            signalled = true
            changed.notifyAll()
        }
    }

    func await(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!signalled) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                changed.wait(timeout: remaining)
            }
            signalled
        }
    }
}

private class WirestackOpeningGate {
    private let mutex = Mutex()
    private let changed = synchronized(mutex) {
        mutex.condition()
    }
    private var entered = false
    private var released = false

    func block(context: OperationContext): Unit {
        let registration = context.cancellation.register {
            => synchronized(mutex) {
                changed.notifyAll()
            }
        }
        try {
            let deadline = MonoTime.now() + Duration.second * 3
            synchronized(mutex) {
                entered = true
                changed.notifyAll()
                while (!released && !context.isCancellationRequested()) {
                    let remaining = deadline - MonoTime.now()
                    if (remaining <= Duration.Zero) {
                        break
                    }
                    changed.wait(timeout: remaining)
                }
            }
        } finally {
            registration.unregister()
        }
    }

    func awaitEntered(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!entered) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                changed.wait(timeout: remaining)
            }
            entered
        }
    }

    func release(): Unit {
        synchronized(mutex) {
            released = true
            changed.notifyAll()
        }
    }
}

private class WirestackStalledBody <: HttpBodyStream {
    private let mutex = Mutex()
    private let changed = synchronized(mutex) {
        mutex.condition()
    }
    private var entered = false
    private var released = false
    private var closed = false

    public prop contentLength: ?Int64 {
        get() {
            None<Int64>
        }
    }

    public func read(destination: Array<Byte>): Int64 {
        if (destination.isEmpty()) {
            return 0
        }
        synchronized(mutex) {
            if (closed) {
                throw HttpBodyException(HttpBodyErrorCode.Closed, "stalled test body closed")
            }
            entered = true
            changed.notifyAll()
            while (!released && !closed) {
                changed.wait()
            }
            if (closed) {
                throw HttpBodyException(HttpBodyErrorCode.Closed, "stalled test body closed")
            }
            0
        }
    }

    func awaitEntered(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!entered) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                changed.wait(timeout: remaining)
            }
            entered
        }
    }

    func release(): Unit {
        synchronized(mutex) {
            released = true
            changed.notifyAll()
        }
    }

    public func close(): Unit {
        synchronized(mutex) {
            closed = true
            changed.notifyAll()
        }
    }

    public func isClosed(): Bool {
        synchronized(mutex) {
            closed
        }
    }
}

private class WirestackIdleBody <: HttpBodyStream {
    private let mutex = Mutex()
    private let changed = synchronized(mutex) {
        mutex.condition()
    }
    private let initial = ": open\n\n".toArray()
    private var initialOffset: Int64 = 0
    private var next = Array<Byte>()
    private var nextOffset: Int64 = 0
    private var initialCompleted = false
    private var closed = false

    public prop contentLength: ?Int64 {
        get() {
            None<Int64>
        }
    }

    public func read(destination: Array<Byte>): Int64 {
        if (destination.isEmpty()) {
            return 0
        }
        synchronized(mutex) {
            if (closed) {
                throw HttpBodyException(HttpBodyErrorCode.Closed, "idle test body closed")
            }
            if (initialOffset < initial.size) {
                let count = min(destination.size, initial.size - initialOffset)
                initial.copyTo(destination, initialOffset, 0, count)
                initialOffset += count
                if (initialOffset == initial.size) {
                    initialCompleted = true
                    changed.notifyAll()
                }
                return count
            }
            while (nextOffset == next.size && !closed) {
                changed.wait()
            }
            if (closed) {
                throw HttpBodyException(HttpBodyErrorCode.Closed, "idle test body closed")
            }
            let count = min(destination.size, next.size - nextOffset)
            next.copyTo(destination, nextOffset, 0, count)
            nextOffset += count
            count
        }
    }

    func awaitInitialRead(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!initialCompleted) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                changed.wait(timeout: remaining)
            }
            initialCompleted
        }
    }

    func publish(value: String): Unit {
        synchronized(mutex) {
            next = value.toArray()
            nextOffset = 0
            changed.notifyAll()
        }
    }

    public func close(): Unit {
        synchronized(mutex) {
            closed = true
            changed.notifyAll()
        }
    }

    public func isClosed(): Bool {
        synchronized(mutex) {
            closed
        }
    }
}

private func wirestackStreamingHeaders(version: HttpVersion): HttpHeaders {
    let builder = HttpHeaders
        .builder()
        .add("Content-Type", "text/event-stream; charset=utf-8")
        .add("Cache-Control", "no-cache")
    if (version == HttpVersion.Http11) {
        let _ = builder.add("Transfer-Encoding", "chunked").add("Connection", "close")
    }
    builder.build()
}

private func wirestackFiniteResponse(body!: String = "id:1\ndata:finite\n\n"): HttpServerResponse {
    let bytes = body.toArray()
    HttpServerResponse(
        HttpResponse(
            200,
            "OK",
            ResponseBody(RequestBody.bytes(bytes).open()),
            headers: HttpHeaders
                .builder()
                .add("Content-Type", "text/event-stream; charset=utf-8")
                .add("Content-Length", bytes.size.toString())
                .build()
        )
    )
}

private class WirestackClientNetworkHandler <: HttpServerHandler {
    let allCalls = AtomicInt64(0)
    let finiteCalls = AtomicInt64(0)
    let redirectCalls = AtomicInt64(0)
    let redirectTargetCalls = AtomicInt64(0)
    let recoverCalls = AtomicInt64(0)
    let openingGate = WirestackOpeningGate()
    let stalledBody = WirestackStalledBody()
    let recoverBody = WirestackStalledBody()
    let idleBody = WirestackIdleBody()

    public func handle(request: HttpServerRequest, context: OperationContext): HttpServerResponse {
        let _ = request.body.read(Array<Byte>(1, repeat: 0u8))
        allCalls.fetchAdd(1)
        if (request.target.startsWith("/finite")) {
            finiteCalls.fetchAdd(1)
            return wirestackFiniteResponse()
        }
        if (request.target.startsWith("/open-stall")) {
            openingGate.block(context)
            return wirestackFiniteResponse()
        }
        if (request.target.startsWith("/read-stall")) {
            return HttpServerResponse(
                HttpResponse(
                    200,
                    "OK",
                    ResponseBody(stalledBody),
                    headers: wirestackStreamingHeaders(request.version)
                )
            )
        }
        if (request.target.startsWith("/idle")) {
            return HttpServerResponse(
                HttpResponse(
                    200,
                    "OK",
                    ResponseBody(idleBody),
                    headers: wirestackStreamingHeaders(request.version)
                )
            )
        }
        if (request.target.startsWith("/recover")) {
            let index = recoverCalls.fetchAdd(1)
            if (index == 0) {
                return HttpServerResponse(
                    HttpResponse(
                        200,
                        "OK",
                        ResponseBody(recoverBody),
                        headers: wirestackStreamingHeaders(request.version)
                    )
                )
            }
            return HttpServerResponse(HttpResponse(204, "No Content", ResponseBody.empty()))
        }
        if (request.target.startsWith("/redirect-target")) {
            redirectTargetCalls.fetchAdd(1)
            return wirestackFiniteResponse(body: "data:followed\n\n")
        }
        if (request.target.startsWith("/redirect")) {
            redirectCalls.fetchAdd(1)
            return HttpServerResponse(
                HttpResponse(
                    302,
                    "Found",
                    ResponseBody.empty(),
                    headers: HttpHeaders.builder().add("Location", "/redirect-target").add("Content-Length", "0")
                        .build()
                )
            )
        }
        HttpServerResponse(HttpResponse(404, "Not Found", ResponseBody.empty()))
    }

    func releaseAll(): Unit {
        openingGate.release()
        stalledBody.release()
        recoverBody.release()
        idleBody.close()
    }
}

private class WirestackRawTestServer {
    let server: HttpServer
    private let serving: Future<Unit>

    init(handler: HttpServerHandler) {
        let built = HttpServer.builder().listen(wirestackTestLoopback(0)).handler(handler).connectionLimit(16).build()
        server = built
        serving = spawn {=> built.serve()}
    }

    prop port: UInt16 {
        get() {
            server.localEndpoint.port
        }
    }

    func close(): Unit {
        let _ = server.shutdown(context: OperationContext(deadline: Some(Deadline.after(Duration.second * 2))))
        serving.get(Duration.second * 2)
    }
}

private class WirestackNetworkSink <: SseDecodeSink {
    let events = ArrayList<SseEvent>()

    public func onEvent(event: SseEvent): Unit {
        events.add(event)
    }
    public func onLastEventIdChanged(_id: String): Unit {
        let _ = _id
    }
    public func onRetryChanged(_retryMillis: Int64): Unit {
        let _ = _retryMillis
    }
}

private class WirestackRecordingTransport <: SseClientTransport {
    private let delegate: SseClientTransport
    let calls = AtomicInt64(0)
    private let seenTokens = ArrayList<SseCancellationToken>()

    init(delegate: SseClientTransport) {
        this.delegate = delegate
    }

    public func send(
        request: SseHttpRequest,
        cancellation: Option<SseCancellationToken>
    ): SseTransportResponse {
        calls.fetchAdd(1)
        match (cancellation) {
            case Some(value) => seenTokens.add(value)
            case None => ()
        }
        delegate.send(request, cancellation)
    }

    func token(index: Int64): SseCancellationToken {
        seenTokens[index]
    }
    func tokenCount(): Int64 {
        seenTokens.size
    }
}

private class WirestackImmediateSleeper <: SseReconnectSleeper {
    public func sleep(_delayMillis: Int64): Bool {
        let _ = _delayMillis;
        true
    }
    public func interrupt(): Unit {}
}

private class WirestackZeroJitter <: SseJitterSource {
    public func nextInt64(upperExclusive: Int64): Int64 {
        upperExclusive / 2
    }
}

private class WirestackNetworkCallbacks <: EventSourceCallbacks {
    var opens: Int64 = 0
    var closed: Int64 = 0
    let errors = ArrayList<SseException>()

    public override func onOpen(): Unit {
        opens++
    }
    public override func onError(error: SseException): Unit {
        errors.add(error)
    }
    public override func onClosed(): Unit {
        closed++
    }
}

private func wirestackSensitiveRequest(
    url: String,
    openTimeoutMillis!: Int64 = 1000,
    readTimeoutMillis!: Int64 = 1000
): SseHttpRequest {
    SseHttpRequest(
        url,
        headers: [
            SseHeader("Authorization", "Bearer ${WIRESTACK_TEST_HEADER_SECRET}"),
            SseHeader("Cookie", "session=${WIRESTACK_TEST_HEADER_SECRET}")
        ],
        openTimeoutMillis: openTimeoutMillis,
        readTimeoutMillis: readTimeoutMillis
    )
}

private func wirestackExpectSseFailure(action: () -> Unit): SseException {
    try {
        action()
    } catch (error: SseException) {
        return error
    }
    throw Exception("expected SseException")
}

private func wirestackAssertRedacted(error: SseException): Unit {
    @Assert(!error.message.contains(WIRESTACK_TEST_QUERY_SECRET))
    @Assert(!error.message.contains(WIRESTACK_TEST_HEADER_SECRET))
}

private func wirestackReadUntil(input: InputStream, marker: String): String {
    let output = ArrayList<Byte>()
    let buffer = Array<Byte>(64, repeat: 0u8)
    while (output.size < 64 * 1024) {
        let count = input.read(buffer)
        if (count == 0) {
            break
        }
        for (index in 0..count) {
            output.add(buffer[index])
        }
        let value = String.fromUtf8(output.toArray())
        if (value.contains(marker)) {
            return value
        }
    }
    throw Exception("SSE marker was not received")
}

private func wirestackReadHttpUntil(body: ResponseBody, marker: String): String {
    let output = ArrayList<Byte>()
    let buffer = Array<Byte>(64, repeat: 0u8)
    while (output.size < 64 * 1024) {
        let count = body.read(buffer)
        if (count == 0) {
            break
        }
        for (index in 0..count) {
            output.add(buffer[index])
        }
        let value = String.fromUtf8(output.toArray())
        if (value.contains(marker)) {
            return value
        }
    }
    throw Exception("HTTP body marker was not received")
}

private func wirestackDrainHttpBody(body: ResponseBody): String {
    let output = ArrayList<Byte>()
    let buffer = Array<Byte>(128, repeat: 0u8)
    while (true) {
        let count = body.read(buffer)
        if (count == 0) {
            break
        }
        for (index in 0..count) {
            output.add(buffer[index])
        }
    }
    String.fromUtf8(output.toArray())
}

private func wirestackWaitMillis(value: Int64): Unit {
    let mutex = Mutex()
    let changed = synchronized(mutex) {
        mutex.condition()
    }
    let deadline = MonoTime.now() + Duration.millisecond * value
    synchronized(mutex) {
        while (true) {
            let remaining = deadline - MonoTime.now()
            if (remaining <= Duration.Zero) {
                break
            }
            changed.wait(timeout: remaining)
        }
    }
}

private func wirestackOperationContext(): OperationContext {
    OperationContext(deadline: Some(Deadline.after(Duration.second * 5)))
}

private func wirestackAssertHttp2Headers(response: HttpResponse): Unit {
    @Assert(response.status, 200)
    @Assert(response.version, HttpVersion.Http2)
    @Assert(!response.headers.contains("Transfer-Encoding"))
    @Assert(!response.headers.contains("Connection"))
}

@Test
func wirestackFiniteResponseEndsWithoutReconnect() {
    let handler = WirestackClientNetworkHandler()
    let fixture = WirestackRawTestServer(handler)
    try {
        let transport = WirestackSseClientTransport()
        let sink = WirestackNetworkSink()
        let url = "http://127.0.0.1:${fixture.port}/finite?token=${WIRESTACK_TEST_QUERY_SECRET}"
        readSseHttpResponse(
            SseStreamReader(),
            wirestackSensitiveRequest(url),
            transport,
            sink
        )
        @Assert(handler.finiteCalls.load(), 1)
        @Assert(handler.allCalls.load(), 1)
        @Assert(sink.events.size, 1)
        @Assert(sink.events[0].data, "finite")
        @Assert(sink.events[0].lastEventId, "1")
        @Assert(sink.events[0].origin, Some("http://127.0.0.1:${fixture.port}"))
    } finally {
        handler.releaseAll()
        fixture.close()
    }
}

@Test
func wirestackOpenTimeoutCancelsStalledHeaders() {
    let handler = WirestackClientNetworkHandler()
    let fixture = WirestackRawTestServer(handler)
    let transport = WirestackSseClientTransport()
    let url = "http://127.0.0.1:${fixture.port}/open-stall?token=${WIRESTACK_TEST_QUERY_SECRET}"
    let opening: Future<SseException> = spawn {
        => wirestackExpectSseFailure {
            =>
                let unexpected = transport.send(
                    wirestackSensitiveRequest(url, openTimeoutMillis: 100),
                    None<SseCancellationToken>
                )
                unexpected.close()
        }
    }
    try {
        @Assert(handler.openingGate.awaitEntered())
        let failure = opening.get(Duration.second * 2)
        @Assert(failure.kind, SseErrorKind.TransportFailure)
        wirestackAssertRedacted(failure)
        handler.openingGate.release()

        let next = transport.send(
            wirestackSensitiveRequest("http://127.0.0.1:${fixture.port}/finite?token=${WIRESTACK_TEST_QUERY_SECRET}"),
            None<SseCancellationToken>
        )
        try {
            @Assert(next.status(), 200)
        } finally {
            next.close()
        }
        @Assert(handler.finiteCalls.load(), 1)
        @Assert(handler.allCalls.load(), 2)
    } finally {
        handler.releaseAll()
        try {
            opening.get(Duration.second * 2)
        } catch (_: Exception) {}
        fixture.close()
    }
}

@Test
func wirestackCallerCancellationWinsDuringOpen() {
    let handler = WirestackClientNetworkHandler()
    let fixture = WirestackRawTestServer(handler)
    let transport = WirestackSseClientTransport()
    let token = SseCancellationToken()
    let url = "http://127.0.0.1:${fixture.port}/open-stall?token=${WIRESTACK_TEST_QUERY_SECRET}"
    let opening: Future<SseException> = spawn {
        => wirestackExpectSseFailure {
            =>
                let unexpected = transport.send(
                    wirestackSensitiveRequest(url, openTimeoutMillis: 2000),
                    Some(token)
                )
                unexpected.close()
        }
    }
    try {
        @Assert(handler.openingGate.awaitEntered())
        token.cancel()
        handler.openingGate.release()
        let failure = opening.get(Duration.second * 2)
        @Assert(failure.kind, SseErrorKind.Cancelled)
        wirestackAssertRedacted(failure)

        let preCancelled = SseCancellationToken()
        preCancelled.cancel()
        let preFailure = wirestackExpectSseFailure {
            =>
                let unexpected = transport.send(
                    wirestackSensitiveRequest(
                        "http://127.0.0.1:${fixture.port}/finite?token=${WIRESTACK_TEST_QUERY_SECRET}"),
                    Some(preCancelled)
                )
                unexpected.close()
        }
        @Assert(preFailure.kind, SseErrorKind.Cancelled)
        wirestackAssertRedacted(preFailure)
        @Assert(handler.finiteCalls.load(), 0)
        @Assert(handler.allCalls.load(), 1)
    } finally {
        handler.releaseAll()
        try {
            opening.get(Duration.second * 2)
        } catch (_: Exception) {}
        fixture.close()
    }
}

@Test
func wirestackReadTimeoutRemainsRecoverable() {
    let handler = WirestackClientNetworkHandler()
    let fixture = WirestackRawTestServer(handler)
    let recording = WirestackRecordingTransport(WirestackSseClientTransport())
    let callbacks = WirestackNetworkCallbacks()
    let client = EventSourceClient(
        EventSourceConfig(
            "http://127.0.0.1:${fixture.port}/recover?token=${WIRESTACK_TEST_QUERY_SECRET}",
            headers: [SseHeader("Authorization", "Bearer ${WIRESTACK_TEST_HEADER_SECRET}")],
            openTimeoutMillis: 1000,
            readTimeoutMillis: 100,
            reconnectPolicy: SseReconnectPolicy(initialDelayMillis: 0, maxDelayMillis: 1, maxAttempts: Some(3))
        ),
        callbacks: callbacks,
        transport: recording,
        sleeper: WirestackImmediateSleeper(),
        jitterSource: WirestackZeroJitter()
    )
    let running: Future<Unit> = spawn {=> client.run()}
    try {
        @Assert(handler.recoverBody.awaitEntered())
        running.get(Duration.second * 2)
        @Assert(handler.recoverCalls.load(), 2)
        @Assert(recording.calls.load(), 2)
        @Assert(recording.tokenCount(), 2)
        @Assert(!recording.token(0).isCancelled)
        @Assert(callbacks.opens, 1)
        @Assert(callbacks.errors.size, 1)
        @Assert(callbacks.errors[0].kind, SseErrorKind.TransportFailure)
        wirestackAssertRedacted(callbacks.errors[0])
        @Assert(callbacks.closed, 1)
        @Assert(client.readyState, EventSourceReadyState.CLOSED)
    } finally {
        client.close()
        handler.releaseAll()
        try {
            running.get(Duration.second * 2)
        } catch (_: Exception) {}
        fixture.close()
    }
}

@Test
func wirestackInactiveReaderDoesNotConsumeReadBudget() {
    let handler = WirestackClientNetworkHandler()
    let fixture = WirestackRawTestServer(handler)
    let transport = WirestackSseClientTransport()
    let response = transport.send(
        wirestackSensitiveRequest(
            "http://127.0.0.1:${fixture.port}/idle?token=${WIRESTACK_TEST_QUERY_SECRET}",
            readTimeoutMillis: 100
        ),
        None<SseCancellationToken>
    )
    var reading = None<Future<String>>
    try {
        @Assert(response.status(), 200)
        let initial = wirestackReadUntil(response.body(), "\n\n")
        @Assert(initial.contains("open"))
        @Assert(handler.idleBody.awaitInitialRead())
        wirestackWaitMillis(250)
        handler.idleBody.publish("id:2\ndata:after-idle\n\n")
        let activeRead: Future<String> = spawn {=> wirestackReadUntil(response.body(), "after-idle")}
        reading = Some(activeRead)
        @Assert(activeRead.get(Duration.second * 2).contains("after-idle"))
        @Assert(handler.allCalls.load(), 1)
    } finally {
        response.close()
        if (let Some(value) <- reading) {
            try {
                value.get(Duration.second * 2)
            } catch (_: Exception) {}
        }
        handler.releaseAll()
        fixture.close()
    }
}

@Test
func wirestackResponseCloseInterruptsActiveRead() {
    let handler = WirestackClientNetworkHandler()
    let fixture = WirestackRawTestServer(handler)
    let transport = WirestackSseClientTransport()
    let token = SseCancellationToken()
    let response = transport.send(
        wirestackSensitiveRequest(
            "http://127.0.0.1:${fixture.port}/read-stall?token=${WIRESTACK_TEST_QUERY_SECRET}",
            readTimeoutMillis: 2000
        ),
        Some(token)
    )
    let started = WirestackTestLatch()
    let reading: Future<SseException> = spawn {
        =>
            started.signal()
            wirestackExpectSseFailure {=> let _ = response.body().read(Array<Byte>(64, repeat: 0u8))}
    }
    try {
        @Assert(handler.stalledBody.awaitEntered())
        @Assert(started.await())
        response.close()
        let failure = reading.get(Duration.second * 2)
        @Assert(failure.kind, SseErrorKind.TransportFailure)
        wirestackAssertRedacted(failure)
        response.close()
        @Assert(!token.isCancelled)

        let reusable = transport.send(
            wirestackSensitiveRequest("http://127.0.0.1:${fixture.port}/finite?token=${WIRESTACK_TEST_QUERY_SECRET}"),
            Some(token)
        )
        try {
            @Assert(reusable.status(), 200)
        } finally {
            reusable.close()
        }
        @Assert(handler.allCalls.load(), 2)
    } finally {
        response.close()
        handler.releaseAll()
        try {
            reading.get(Duration.second * 2)
        } catch (_: Exception) {}
        fixture.close()
    }
}

@Test
func wirestackRedirectResponseIsNotFollowed() {
    let handler = WirestackClientNetworkHandler()
    let fixture = WirestackRawTestServer(handler)
    try {
        let response = WirestackSseClientTransport().send(
            wirestackSensitiveRequest("http://127.0.0.1:${fixture.port}/redirect?token=${WIRESTACK_TEST_QUERY_SECRET}"),
            None<SseCancellationToken>
        )
        try {
            @Assert(response.status(), 302)
            @Assert(response.header("Location"), Some("/redirect-target"))
            @Assert(handler.redirectCalls.load(), 1)
            @Assert(handler.redirectTargetCalls.load(), 0)
            @Assert(handler.allCalls.load(), 1)
        } finally {
            response.close()
        }
    } finally {
        handler.releaseAll()
        fixture.close()
    }
}

private func wirestackTestCertificateDer(): Array<Byte> {
    [
        48u8,
        130u8,
        3u8,
        67u8,
        48u8,
        130u8,
        2u8,
        43u8,
        160u8,
        3u8,
        2u8,
        1u8,
        2u8,
        2u8,
        20u8,
        13u8,
        158u8,
        169u8,
        62u8,
        101u8,
        122u8,
        129u8,
        200u8,
        194u8,
        246u8,
        212u8,
        232u8,
        190u8,
        2u8,
        19u8,
        68u8,
        230u8,
        67u8,
        90u8,
        46u8,
        48u8,
        13u8,
        6u8,
        9u8,
        42u8,
        134u8,
        72u8,
        134u8,
        247u8,
        13u8,
        1u8,
        1u8,
        11u8,
        5u8,
        0u8,
        48u8,
        26u8,
        49u8,
        24u8,
        48u8,
        22u8,
        6u8,
        3u8,
        85u8,
        4u8,
        3u8,
        12u8,
        15u8,
        105u8,
        103u8,
        110u8,
        111u8,
        114u8,
        101u8,
        100u8,
        46u8,
        101u8,
        120u8,
        97u8,
        109u8,
        112u8,
        108u8,
        101u8,
        48u8,
        30u8,
        23u8,
        13u8,
        50u8,
        54u8,
        48u8,
        56u8,
        50u8,
        51u8,
        50u8,
        48u8,
        49u8,
        57u8,
        50u8,
        56u8,
        90u8,
        23u8,
        13u8,
        51u8,
        54u8,
        48u8,
        56u8,
        50u8,
        48u8,
        50u8,
        48u8,
        49u8,
        57u8,
        50u8,
        56u8,
        90u8,
        48u8,
        26u8,
        49u8,
        24u8,
        48u8,
        22u8,
        6u8,
        3u8,
        85u8,
        4u8,
        3u8,
        12u8,
        15u8,
        105u8,
        103u8,
        110u8,
        111u8,
        114u8,
        101u8,
        100u8,
        46u8,
        101u8,
        120u8,
        97u8,
        109u8,
        112u8,
        108u8,
        101u8,
        48u8,
        130u8,
        1u8,
        34u8,
        48u8,
        13u8,
        6u8,
        9u8,
        42u8,
        134u8,
        72u8,
        134u8,
        247u8,
        13u8,
        1u8,
        1u8,
        1u8,
        5u8,
        0u8,
        3u8,
        130u8,
        1u8,
        15u8,
        0u8,
        48u8,
        130u8,
        1u8,
        10u8,
        2u8,
        130u8,
        1u8,
        1u8,
        0u8,
        223u8,
        226u8,
        2u8,
        205u8,
        81u8,
        218u8,
        67u8,
        207u8,
        156u8,
        207u8,
        23u8,
        149u8,
        253u8,
        167u8,
        160u8,
        254u8,
        32u8,
        8u8,
        131u8,
        8u8,
        106u8,
        99u8,
        43u8,
        81u8,
        85u8,
        205u8,
        123u8,
        252u8,
        22u8,
        55u8,
        93u8,
        210u8,
        237u8,
        97u8,
        38u8,
        134u8,
        244u8,
        238u8,
        176u8,
        99u8,
        27u8,
        106u8,
        155u8,
        156u8,
        217u8,
        189u8,
        203u8,
        163u8,
        114u8,
        93u8,
        249u8,
        189u8,
        43u8,
        68u8,
        16u8,
        135u8,
        58u8,
        70u8,
        197u8,
        233u8,
        67u8,
        16u8,
        20u8,
        204u8,
        106u8,
        122u8,
        26u8,
        173u8,
        118u8,
        244u8,
        196u8,
        195u8,
        205u8,
        220u8,
        150u8,
        229u8,
        174u8,
        241u8,
        235u8,
        118u8,
        214u8,
        11u8,
        166u8,
        101u8,
        135u8,
        164u8,
        169u8,
        11u8,
        140u8,
        197u8,
        118u8,
        223u8,
        241u8,
        24u8,
        152u8,
        172u8,
        39u8,
        15u8,
        199u8,
        75u8,
        130u8,
        199u8,
        45u8,
        114u8,
        179u8,
        66u8,
        36u8,
        176u8,
        29u8,
        147u8,
        240u8,
        20u8,
        222u8,
        243u8,
        57u8,
        157u8,
        9u8,
        48u8,
        8u8,
        116u8,
        228u8,
        18u8,
        197u8,
        227u8,
        33u8,
        14u8,
        194u8,
        70u8,
        226u8,
        169u8,
        133u8,
        17u8,
        222u8,
        46u8,
        243u8,
        211u8,
        5u8,
        109u8,
        189u8,
        65u8,
        110u8,
        47u8,
        115u8,
        64u8,
        25u8,
        246u8,
        87u8,
        205u8,
        118u8,
        53u8,
        29u8,
        24u8,
        190u8,
        65u8,
        249u8,
        191u8,
        135u8,
        217u8,
        91u8,
        172u8,
        201u8,
        144u8,
        182u8,
        109u8,
        228u8,
        4u8,
        96u8,
        102u8,
        101u8,
        72u8,
        124u8,
        1u8,
        42u8,
        174u8,
        129u8,
        199u8,
        226u8,
        8u8,
        204u8,
        226u8,
        97u8,
        134u8,
        53u8,
        202u8,
        6u8,
        112u8,
        212u8,
        192u8,
        2u8,
        196u8,
        158u8,
        180u8,
        157u8,
        23u8,
        239u8,
        171u8,
        92u8,
        24u8,
        85u8,
        151u8,
        84u8,
        154u8,
        32u8,
        246u8,
        88u8,
        64u8,
        248u8,
        52u8,
        93u8,
        21u8,
        43u8,
        159u8,
        45u8,
        63u8,
        250u8,
        137u8,
        208u8,
        169u8,
        202u8,
        207u8,
        242u8,
        54u8,
        131u8,
        129u8,
        156u8,
        87u8,
        149u8,
        238u8,
        214u8,
        209u8,
        255u8,
        216u8,
        214u8,
        26u8,
        35u8,
        198u8,
        19u8,
        217u8,
        232u8,
        200u8,
        66u8,
        126u8,
        147u8,
        131u8,
        6u8,
        22u8,
        55u8,
        237u8,
        199u8,
        105u8,
        119u8,
        183u8,
        67u8,
        205u8,
        195u8,
        67u8,
        2u8,
        3u8,
        1u8,
        0u8,
        1u8,
        163u8,
        129u8,
        128u8,
        48u8,
        126u8,
        48u8,
        29u8,
        6u8,
        3u8,
        85u8,
        29u8,
        14u8,
        4u8,
        22u8,
        4u8,
        20u8,
        231u8,
        84u8,
        206u8,
        159u8,
        171u8,
        134u8,
        130u8,
        56u8,
        16u8,
        224u8,
        254u8,
        147u8,
        241u8,
        226u8,
        101u8,
        225u8,
        224u8,
        77u8,
        215u8,
        4u8,
        48u8,
        31u8,
        6u8,
        3u8,
        85u8,
        29u8,
        35u8,
        4u8,
        24u8,
        48u8,
        22u8,
        128u8,
        20u8,
        231u8,
        84u8,
        206u8,
        159u8,
        171u8,
        134u8,
        130u8,
        56u8,
        16u8,
        224u8,
        254u8,
        147u8,
        241u8,
        226u8,
        101u8,
        225u8,
        224u8,
        77u8,
        215u8,
        4u8,
        48u8,
        15u8,
        6u8,
        3u8,
        85u8,
        29u8,
        19u8,
        1u8,
        1u8,
        255u8,
        4u8,
        5u8,
        48u8,
        3u8,
        1u8,
        1u8,
        255u8,
        48u8,
        43u8,
        6u8,
        3u8,
        85u8,
        29u8,
        17u8,
        4u8,
        36u8,
        48u8,
        34u8,
        130u8,
        11u8,
        101u8,
        120u8,
        97u8,
        109u8,
        112u8,
        108u8,
        101u8,
        46u8,
        99u8,
        111u8,
        109u8,
        130u8,
        13u8,
        42u8,
        46u8,
        101u8,
        120u8,
        97u8,
        109u8,
        112u8,
        108u8,
        101u8,
        46u8,
        111u8,
        114u8,
        103u8,
        135u8,
        4u8,
        127u8,
        0u8,
        0u8,
        1u8,
        48u8,
        13u8,
        6u8,
        9u8,
        42u8,
        134u8,
        72u8,
        134u8,
        247u8,
        13u8,
        1u8,
        1u8,
        11u8,
        5u8,
        0u8,
        3u8,
        130u8,
        1u8,
        1u8,
        0u8,
        177u8,
        226u8,
        104u8,
        89u8,
        222u8,
        253u8,
        50u8,
        95u8,
        87u8,
        60u8,
        231u8,
        174u8,
        226u8,
        214u8,
        76u8,
        242u8,
        169u8,
        65u8,
        95u8,
        254u8,
        117u8,
        146u8,
        161u8,
        63u8,
        15u8,
        23u8,
        218u8,
        233u8,
        153u8,
        213u8,
        242u8,
        221u8,
        235u8,
        251u8,
        3u8,
        219u8,
        27u8,
        104u8,
        120u8,
        58u8,
        31u8,
        216u8,
        131u8,
        177u8,
        248u8,
        122u8,
        144u8,
        38u8,
        228u8,
        13u8,
        20u8,
        214u8,
        60u8,
        148u8,
        1u8,
        236u8,
        9u8,
        190u8,
        73u8,
        239u8,
        111u8,
        206u8,
        230u8,
        181u8,
        193u8,
        132u8,
        113u8,
        219u8,
        99u8,
        28u8,
        19u8,
        47u8,
        242u8,
        238u8,
        2u8,
        143u8,
        95u8,
        41u8,
        176u8,
        130u8,
        195u8,
        26u8,
        117u8,
        209u8,
        142u8,
        96u8,
        192u8,
        250u8,
        196u8,
        114u8,
        94u8,
        83u8,
        213u8,
        153u8,
        108u8,
        2u8,
        3u8,
        211u8,
        42u8,
        173u8,
        116u8,
        56u8,
        220u8,
        13u8,
        71u8,
        77u8,
        240u8,
        121u8,
        241u8,
        58u8,
        122u8,
        151u8,
        163u8,
        33u8,
        59u8,
        215u8,
        218u8,
        112u8,
        141u8,
        37u8,
        72u8,
        154u8,
        127u8,
        112u8,
        86u8,
        204u8,
        88u8,
        170u8,
        231u8,
        27u8,
        25u8,
        93u8,
        245u8,
        7u8,
        162u8,
        41u8,
        127u8,
        2u8,
        160u8,
        9u8,
        126u8,
        169u8,
        192u8,
        130u8,
        32u8,
        1u8,
        160u8,
        235u8,
        63u8,
        103u8,
        17u8,
        43u8,
        127u8,
        74u8,
        181u8,
        232u8,
        10u8,
        107u8,
        226u8,
        184u8,
        197u8,
        190u8,
        183u8,
        128u8,
        194u8,
        200u8,
        152u8,
        175u8,
        86u8,
        32u8,
        39u8,
        134u8,
        139u8,
        247u8,
        184u8,
        247u8,
        140u8,
        43u8,
        201u8,
        80u8,
        218u8,
        222u8,
        26u8,
        191u8,
        19u8,
        53u8,
        64u8,
        90u8,
        114u8,
        21u8,
        18u8,
        173u8,
        27u8,
        214u8,
        112u8,
        227u8,
        147u8,
        107u8,
        15u8,
        116u8,
        121u8,
        236u8,
        127u8,
        232u8,
        57u8,
        239u8,
        83u8,
        81u8,
        7u8,
        14u8,
        73u8,
        229u8,
        153u8,
        203u8,
        174u8,
        207u8,
        44u8,
        26u8,
        193u8,
        83u8,
        115u8,
        173u8,
        55u8,
        117u8,
        208u8,
        75u8,
        135u8,
        138u8,
        101u8,
        95u8,
        38u8,
        10u8,
        53u8,
        133u8,
        20u8,
        179u8,
        93u8,
        222u8,
        29u8,
        120u8,
        33u8,
        202u8,
        74u8,
        217u8,
        88u8,
        144u8,
        242u8,
        153u8,
        167u8,
        5u8,
        63u8,
        51u8,
        224u8,
        37u8,
        240u8,
        93u8
    ]
}

private func wirestackTestPrivateKeyDer(): Array<Byte> {
    [
        48u8,
        130u8,
        4u8,
        191u8,
        2u8,
        1u8,
        0u8,
        48u8,
        13u8,
        6u8,
        9u8,
        42u8,
        134u8,
        72u8,
        134u8,
        247u8,
        13u8,
        1u8,
        1u8,
        1u8,
        5u8,
        0u8,
        4u8,
        130u8,
        4u8,
        169u8,
        48u8,
        130u8,
        4u8,
        165u8,
        2u8,
        1u8,
        0u8,
        2u8,
        130u8,
        1u8,
        1u8,
        0u8,
        223u8,
        226u8,
        2u8,
        205u8,
        81u8,
        218u8,
        67u8,
        207u8,
        156u8,
        207u8,
        23u8,
        149u8,
        253u8,
        167u8,
        160u8,
        254u8,
        32u8,
        8u8,
        131u8,
        8u8,
        106u8,
        99u8,
        43u8,
        81u8,
        85u8,
        205u8,
        123u8,
        252u8,
        22u8,
        55u8,
        93u8,
        210u8,
        237u8,
        97u8,
        38u8,
        134u8,
        244u8,
        238u8,
        176u8,
        99u8,
        27u8,
        106u8,
        155u8,
        156u8,
        217u8,
        189u8,
        203u8,
        163u8,
        114u8,
        93u8,
        249u8,
        189u8,
        43u8,
        68u8,
        16u8,
        135u8,
        58u8,
        70u8,
        197u8,
        233u8,
        67u8,
        16u8,
        20u8,
        204u8,
        106u8,
        122u8,
        26u8,
        173u8,
        118u8,
        244u8,
        196u8,
        195u8,
        205u8,
        220u8,
        150u8,
        229u8,
        174u8,
        241u8,
        235u8,
        118u8,
        214u8,
        11u8,
        166u8,
        101u8,
        135u8,
        164u8,
        169u8,
        11u8,
        140u8,
        197u8,
        118u8,
        223u8,
        241u8,
        24u8,
        152u8,
        172u8,
        39u8,
        15u8,
        199u8,
        75u8,
        130u8,
        199u8,
        45u8,
        114u8,
        179u8,
        66u8,
        36u8,
        176u8,
        29u8,
        147u8,
        240u8,
        20u8,
        222u8,
        243u8,
        57u8,
        157u8,
        9u8,
        48u8,
        8u8,
        116u8,
        228u8,
        18u8,
        197u8,
        227u8,
        33u8,
        14u8,
        194u8,
        70u8,
        226u8,
        169u8,
        133u8,
        17u8,
        222u8,
        46u8,
        243u8,
        211u8,
        5u8,
        109u8,
        189u8,
        65u8,
        110u8,
        47u8,
        115u8,
        64u8,
        25u8,
        246u8,
        87u8,
        205u8,
        118u8,
        53u8,
        29u8,
        24u8,
        190u8,
        65u8,
        249u8,
        191u8,
        135u8,
        217u8,
        91u8,
        172u8,
        201u8,
        144u8,
        182u8,
        109u8,
        228u8,
        4u8,
        96u8,
        102u8,
        101u8,
        72u8,
        124u8,
        1u8,
        42u8,
        174u8,
        129u8,
        199u8,
        226u8,
        8u8,
        204u8,
        226u8,
        97u8,
        134u8,
        53u8,
        202u8,
        6u8,
        112u8,
        212u8,
        192u8,
        2u8,
        196u8,
        158u8,
        180u8,
        157u8,
        23u8,
        239u8,
        171u8,
        92u8,
        24u8,
        85u8,
        151u8,
        84u8,
        154u8,
        32u8,
        246u8,
        88u8,
        64u8,
        248u8,
        52u8,
        93u8,
        21u8,
        43u8,
        159u8,
        45u8,
        63u8,
        250u8,
        137u8,
        208u8,
        169u8,
        202u8,
        207u8,
        242u8,
        54u8,
        131u8,
        129u8,
        156u8,
        87u8,
        149u8,
        238u8,
        214u8,
        209u8,
        255u8,
        216u8,
        214u8,
        26u8,
        35u8,
        198u8,
        19u8,
        217u8,
        232u8,
        200u8,
        66u8,
        126u8,
        147u8,
        131u8,
        6u8,
        22u8,
        55u8,
        237u8,
        199u8,
        105u8,
        119u8,
        183u8,
        67u8,
        205u8,
        195u8,
        67u8,
        2u8,
        3u8,
        1u8,
        0u8,
        1u8,
        2u8,
        130u8,
        1u8,
        0u8,
        43u8,
        63u8,
        249u8,
        163u8,
        153u8,
        118u8,
        237u8,
        87u8,
        171u8,
        166u8,
        212u8,
        27u8,
        29u8,
        51u8,
        231u8,
        101u8,
        128u8,
        97u8,
        180u8,
        146u8,
        137u8,
        30u8,
        220u8,
        76u8,
        235u8,
        162u8,
        245u8,
        27u8,
        55u8,
        93u8,
        125u8,
        202u8,
        20u8,
        34u8,
        138u8,
        243u8,
        82u8,
        88u8,
        212u8,
        9u8,
        105u8,
        79u8,
        248u8,
        124u8,
        222u8,
        17u8,
        140u8,
        199u8,
        231u8,
        253u8,
        160u8,
        147u8,
        187u8,
        74u8,
        124u8,
        88u8,
        235u8,
        88u8,
        165u8,
        190u8,
        16u8,
        191u8,
        8u8,
        13u8,
        143u8,
        90u8,
        68u8,
        82u8,
        19u8,
        99u8,
        104u8,
        169u8,
        156u8,
        216u8,
        202u8,
        235u8,
        215u8,
        22u8,
        148u8,
        18u8,
        210u8,
        121u8,
        208u8,
        172u8,
        215u8,
        125u8,
        231u8,
        76u8,
        60u8,
        103u8,
        31u8,
        54u8,
        86u8,
        216u8,
        150u8,
        17u8,
        215u8,
        191u8,
        244u8,
        19u8,
        177u8,
        209u8,
        113u8,
        133u8,
        177u8,
        124u8,
        139u8,
        34u8,
        220u8,
        171u8,
        234u8,
        26u8,
        214u8,
        17u8,
        45u8,
        165u8,
        153u8,
        19u8,
        70u8,
        230u8,
        241u8,
        242u8,
        112u8,
        121u8,
        22u8,
        183u8,
        71u8,
        175u8,
        86u8,
        93u8,
        139u8,
        25u8,
        57u8,
        128u8,
        202u8,
        212u8,
        32u8,
        204u8,
        154u8,
        177u8,
        153u8,
        231u8,
        189u8,
        190u8,
        230u8,
        216u8,
        67u8,
        77u8,
        86u8,
        162u8,
        64u8,
        163u8,
        142u8,
        80u8,
        19u8,
        205u8,
        130u8,
        231u8,
        39u8,
        7u8,
        207u8,
        120u8,
        173u8,
        139u8,
        75u8,
        98u8,
        207u8,
        124u8,
        177u8,
        191u8,
        182u8,
        135u8,
        129u8,
        20u8,
        29u8,
        157u8,
        28u8,
        64u8,
        34u8,
        1u8,
        81u8,
        87u8,
        112u8,
        187u8,
        160u8,
        64u8,
        20u8,
        204u8,
        34u8,
        0u8,
        75u8,
        115u8,
        216u8,
        32u8,
        165u8,
        192u8,
        13u8,
        70u8,
        217u8,
        250u8,
        185u8,
        244u8,
        15u8,
        102u8,
        169u8,
        5u8,
        210u8,
        146u8,
        63u8,
        22u8,
        105u8,
        65u8,
        212u8,
        130u8,
        143u8,
        86u8,
        112u8,
        76u8,
        109u8,
        234u8,
        144u8,
        68u8,
        111u8,
        147u8,
        15u8,
        113u8,
        175u8,
        129u8,
        29u8,
        108u8,
        255u8,
        106u8,
        167u8,
        66u8,
        244u8,
        116u8,
        74u8,
        64u8,
        80u8,
        101u8,
        222u8,
        73u8,
        248u8,
        135u8,
        74u8,
        132u8,
        26u8,
        66u8,
        210u8,
        203u8,
        69u8,
        171u8,
        48u8,
        178u8,
        166u8,
        237u8,
        2u8,
        129u8,
        129u8,
        0u8,
        250u8,
        126u8,
        103u8,
        117u8,
        4u8,
        58u8,
        229u8,
        178u8,
        6u8,
        203u8,
        161u8,
        168u8,
        183u8,
        54u8,
        122u8,
        231u8,
        168u8,
        165u8,
        91u8,
        159u8,
        207u8,
        180u8,
        190u8,
        114u8,
        99u8,
        153u8,
        52u8,
        242u8,
        147u8,
        23u8,
        152u8,
        29u8,
        5u8,
        241u8,
        6u8,
        221u8,
        223u8,
        204u8,
        127u8,
        173u8,
        227u8,
        103u8,
        43u8,
        38u8,
        140u8,
        16u8,
        110u8,
        226u8,
        222u8,
        160u8,
        237u8,
        180u8,
        161u8,
        70u8,
        52u8,
        85u8,
        79u8,
        30u8,
        65u8,
        88u8,
        123u8,
        109u8,
        107u8,
        221u8,
        186u8,
        231u8,
        31u8,
        22u8,
        170u8,
        222u8,
        112u8,
        176u8,
        139u8,
        232u8,
        104u8,
        97u8,
        0u8,
        92u8,
        105u8,
        217u8,
        211u8,
        35u8,
        128u8,
        231u8,
        111u8,
        34u8,
        41u8,
        183u8,
        64u8,
        223u8,
        100u8,
        189u8,
        52u8,
        148u8,
        151u8,
        3u8,
        152u8,
        114u8,
        16u8,
        56u8,
        221u8,
        20u8,
        192u8,
        239u8,
        5u8,
        146u8,
        205u8,
        71u8,
        245u8,
        249u8,
        178u8,
        245u8,
        93u8,
        107u8,
        57u8,
        109u8,
        67u8,
        96u8,
        229u8,
        172u8,
        233u8,
        55u8,
        64u8,
        109u8,
        146u8,
        102u8,
        185u8,
        197u8,
        2u8,
        129u8,
        129u8,
        0u8,
        228u8,
        205u8,
        220u8,
        44u8,
        149u8,
        248u8,
        220u8,
        102u8,
        3u8,
        169u8,
        213u8,
        5u8,
        246u8,
        161u8,
        35u8,
        206u8,
        104u8,
        13u8,
        85u8,
        184u8,
        225u8,
        9u8,
        158u8,
        189u8,
        254u8,
        35u8,
        100u8,
        145u8,
        240u8,
        83u8,
        71u8,
        249u8,
        181u8,
        112u8,
        125u8,
        186u8,
        226u8,
        148u8,
        94u8,
        76u8,
        155u8,
        79u8,
        35u8,
        28u8,
        113u8,
        224u8,
        162u8,
        168u8,
        39u8,
        25u8,
        146u8,
        198u8,
        236u8,
        98u8,
        200u8,
        129u8,
        0u8,
        20u8,
        212u8,
        249u8,
        136u8,
        175u8,
        206u8,
        33u8,
        7u8,
        176u8,
        132u8,
        220u8,
        147u8,
        61u8,
        197u8,
        114u8,
        119u8,
        183u8,
        72u8,
        217u8,
        236u8,
        25u8,
        45u8,
        124u8,
        145u8,
        7u8,
        47u8,
        58u8,
        32u8,
        216u8,
        58u8,
        250u8,
        191u8,
        53u8,
        198u8,
        79u8,
        236u8,
        131u8,
        113u8,
        221u8,
        170u8,
        212u8,
        186u8,
        247u8,
        24u8,
        190u8,
        177u8,
        215u8,
        198u8,
        62u8,
        146u8,
        11u8,
        250u8,
        137u8,
        226u8,
        239u8,
        211u8,
        142u8,
        210u8,
        56u8,
        18u8,
        218u8,
        151u8,
        138u8,
        60u8,
        136u8,
        72u8,
        227u8,
        244u8,
        246u8,
        65u8,
        103u8,
        2u8,
        129u8,
        129u8,
        0u8,
        132u8,
        207u8,
        30u8,
        21u8,
        41u8,
        159u8,
        252u8,
        134u8,
        174u8,
        25u8,
        170u8,
        229u8,
        99u8,
        246u8,
        43u8,
        165u8,
        197u8,
        129u8,
        158u8,
        153u8,
        149u8,
        171u8,
        115u8,
        201u8,
        252u8,
        74u8,
        143u8,
        139u8,
        9u8,
        55u8,
        218u8,
        59u8,
        145u8,
        25u8,
        224u8,
        149u8,
        36u8,
        130u8,
        45u8,
        51u8,
        201u8,
        188u8,
        186u8,
        97u8,
        30u8,
        163u8,
        219u8,
        214u8,
        199u8,
        72u8,
        181u8,
        159u8,
        46u8,
        94u8,
        165u8,
        130u8,
        145u8,
        135u8,
        46u8,
        8u8,
        54u8,
        176u8,
        166u8,
        31u8,
        18u8,
        104u8,
        85u8,
        208u8,
        83u8,
        217u8,
        202u8,
        15u8,
        160u8,
        174u8,
        33u8,
        33u8,
        68u8,
        234u8,
        186u8,
        104u8,
        118u8,
        12u8,
        125u8,
        220u8,
        182u8,
        204u8,
        123u8,
        39u8,
        160u8,
        234u8,
        144u8,
        99u8,
        204u8,
        139u8,
        247u8,
        161u8,
        61u8,
        99u8,
        68u8,
        124u8,
        145u8,
        162u8,
        222u8,
        68u8,
        28u8,
        69u8,
        96u8,
        55u8,
        134u8,
        15u8,
        212u8,
        181u8,
        72u8,
        201u8,
        127u8,
        210u8,
        17u8,
        134u8,
        63u8,
        95u8,
        180u8,
        234u8,
        201u8,
        107u8,
        14u8,
        211u8,
        14u8,
        45u8,
        2u8,
        129u8,
        129u8,
        0u8,
        134u8,
        2u8,
        176u8,
        239u8,
        136u8,
        83u8,
        127u8,
        59u8,
        92u8,
        239u8,
        238u8,
        238u8,
        20u8,
        58u8,
        227u8,
        78u8,
        244u8,
        144u8,
        194u8,
        225u8,
        139u8,
        36u8,
        232u8,
        78u8,
        29u8,
        120u8,
        187u8,
        97u8,
        226u8,
        225u8,
        212u8,
        105u8,
        68u8,
        69u8,
        3u8,
        207u8,
        31u8,
        0u8,
        137u8,
        142u8,
        210u8,
        244u8,
        113u8,
        251u8,
        42u8,
        19u8,
        201u8,
        17u8,
        53u8,
        253u8,
        123u8,
        131u8,
        121u8,
        86u8,
        142u8,
        91u8,
        72u8,
        33u8,
        235u8,
        111u8,
        208u8,
        8u8,
        48u8,
        114u8,
        22u8,
        31u8,
        228u8,
        97u8,
        144u8,
        248u8,
        0u8,
        26u8,
        128u8,
        85u8,
        224u8,
        156u8,
        138u8,
        245u8,
        96u8,
        55u8,
        242u8,
        117u8,
        195u8,
        202u8,
        135u8,
        12u8,
        62u8,
        215u8,
        56u8,
        129u8,
        21u8,
        139u8,
        32u8,
        91u8,
        8u8,
        169u8,
        45u8,
        114u8,
        117u8,
        163u8,
        94u8,
        97u8,
        234u8,
        166u8,
        23u8,
        221u8,
        244u8,
        10u8,
        130u8,
        14u8,
        154u8,
        213u8,
        159u8,
        18u8,
        170u8,
        187u8,
        223u8,
        7u8,
        26u8,
        245u8,
        3u8,
        105u8,
        75u8,
        80u8,
        0u8,
        220u8,
        239u8,
        7u8,
        2u8,
        129u8,
        129u8,
        0u8,
        201u8,
        146u8,
        224u8,
        14u8,
        102u8,
        135u8,
        23u8,
        78u8,
        94u8,
        251u8,
        61u8,
        252u8,
        67u8,
        89u8,
        251u8,
        15u8,
        231u8,
        180u8,
        173u8,
        174u8,
        200u8,
        174u8,
        47u8,
        224u8,
        47u8,
        160u8,
        125u8,
        133u8,
        177u8,
        196u8,
        92u8,
        79u8,
        95u8,
        94u8,
        245u8,
        61u8,
        148u8,
        225u8,
        139u8,
        130u8,
        179u8,
        37u8,
        114u8,
        117u8,
        19u8,
        62u8,
        130u8,
        53u8,
        40u8,
        37u8,
        126u8,
        85u8,
        61u8,
        231u8,
        72u8,
        163u8,
        239u8,
        224u8,
        114u8,
        242u8,
        107u8,
        65u8,
        194u8,
        72u8,
        127u8,
        12u8,
        217u8,
        150u8,
        163u8,
        12u8,
        16u8,
        153u8,
        216u8,
        113u8,
        104u8,
        232u8,
        27u8,
        193u8,
        152u8,
        86u8,
        219u8,
        155u8,
        247u8,
        252u8,
        113u8,
        72u8,
        249u8,
        159u8,
        187u8,
        40u8,
        247u8,
        24u8,
        54u8,
        215u8,
        17u8,
        200u8,
        171u8,
        42u8,
        73u8,
        134u8,
        58u8,
        98u8,
        57u8,
        132u8,
        241u8,
        15u8,
        217u8,
        66u8,
        26u8,
        177u8,
        53u8,
        30u8,
        150u8,
        57u8,
        188u8,
        176u8,
        116u8,
        143u8,
        216u8,
        30u8,
        118u8,
        147u8,
        34u8,
        166u8,
        117u8,
        81u8,
        174u8,
        82u8
    ]
}

@Test
func wirestackTlsTrustAndHostnameValidation() {
    let certificate = wirestackTestCertificateDer()
    let serverTls = HttpServerTlsConfig([certificate], wirestackTestPrivateKeyDer())
    let endpoint = SseEndpoint("/tls", config: SseEndpointConfig(initialComment: Some("tls-open")))
    let server = SseServer(config: SseServerConfig(listenEndpoint: wirestackTestLoopback(0)))
    server.transportFactory = WirestackSseServerTransportFactory(tls: Some(serverTls))
    server.register(endpoint)
    server.start()
    server.awaitBound()

    let trustedTls = HttpClientTlsConfig([certificate])
    let trustedResolver = StaticResolver(
        ResolveResult(
            [wirestackTestLoopback(0).address],
            HostName("example.com"),
            ResolverSource.Static
        )
    )
    try {
        let trustedTransport = WirestackSseClientTransport(trustedTls, trustedResolver)
        let opened = trustedTransport.send(
            wirestackSensitiveRequest(
                "https://example.com:${server.boundPort}/tls?token=${WIRESTACK_TEST_QUERY_SECRET}"),
            None<SseCancellationToken>
        )
        try {
            @Assert(opened.status(), 200)
            @Assert(opened.header("Content-Type"), Some("text/event-stream; charset=utf-8"))
            let stats = endpoint.hub.broadcast(SseFrame(id: Some("1"), data: Some("trusted")))
            @Assert(stats.accepted, 1)
            @Assert(wirestackReadUntil(opened.body(), "trusted").contains("trusted"))
        } finally {
            opened.close()
        }
        @Assert(!trustedTls.isClosed())

        let wrongTls = HttpClientTlsConfig([certificate])
        try {
            let wrongResolver = StaticResolver(
                ResolveResult(
                    [wirestackTestLoopback(0).address],
                    HostName("wrong.test"),
                    ResolverSource.Static
                )
            )
            let hostnameFailure = wirestackExpectSseFailure {
                =>
                    let unexpected = WirestackSseClientTransport(wrongTls, wrongResolver).send(
                        wirestackSensitiveRequest(
                            "https://wrong.test:${server.boundPort}/tls?token=${WIRESTACK_TEST_QUERY_SECRET}"),
                        None<SseCancellationToken>
                    )
                    unexpected.close()
            }
            @Assert(hostnameFailure.kind, SseErrorKind.TransportFailure)
            wirestackAssertRedacted(hostnameFailure)
            @Assert(!wrongTls.isClosed())
        } finally {
            wrongTls.close()
        }

        let untrustedTls = HttpClientTlsConfig(Array<Array<Byte>>())
        try {
            let untrustedFailure = wirestackExpectSseFailure {
                =>
                    let unexpected = WirestackSseClientTransport(untrustedTls, trustedResolver).send(
                        wirestackSensitiveRequest(
                            "https://example.com:${server.boundPort}/tls?token=${WIRESTACK_TEST_QUERY_SECRET}"),
                        None<SseCancellationToken>
                    )
                    unexpected.close()
            }
            @Assert(untrustedFailure.kind, SseErrorKind.TransportFailure)
            wirestackAssertRedacted(untrustedFailure)
            @Assert(!untrustedTls.isClosed())
        } finally {
            untrustedTls.close()
        }
    } finally {
        server.closeNow()
        trustedTls.close()
        serverTls.close()
    }
}

private func wirestackHttp2Client(certificate: Array<Byte>): (HttpClient, HttpClientTlsConfig, WirestackCountingResolver) {
    let tls = HttpClientTlsConfig([certificate])
    let resolver = WirestackCountingResolver(wirestackTestLoopback(0).address)
    let client = HttpClient
        .builder()
        .resolver(resolver)
        .tls(tls)
        .redirectPolicy(HttpRedirectPolicy.noFollow())
        .retryPolicy(HttpRetryPolicy(maximumAttempts: 1))
        .build()
    (client, tls, resolver)
}

@Test
func wirestackHttp2RequestAbortPreservesSiblingStream() {
    let certificate = wirestackTestCertificateDer()
    let serverTls = HttpServerTlsConfig([certificate], wirestackTestPrivateKeyDer())
    let firstEndpoint = SseEndpoint("/first", config: SseEndpointConfig(initialComment: Some("first-open")))
    let secondEndpoint = SseEndpoint("/second", config: SseEndpointConfig(initialComment: Some("second-open")))
    let server = SseServer(
        config: SseServerConfig(
            listenEndpoint: wirestackTestLoopback(0),
            maxConnections: 2
        )
    )
    server.transportFactory = WirestackSseServerTransportFactory(tls: Some(serverTls))
    server.register(firstEndpoint)
    server.register(secondEndpoint)
    server.start()
    server.awaitBound()
    let (client, clientTls, resolver) = wirestackHttp2Client(certificate)
    try {
        let first = client.get(
            "https://example.com:${server.boundPort}/first?case=h2",
            context: wirestackOperationContext()
        )
        try {
            wirestackAssertHttp2Headers(first)
            @Assert(wirestackReadHttpUntil(first.body, "first-open").contains("first-open"))
            @Assert(resolver.calls.load(), 1)
            let second = client.get(
                "https://example.com:${server.boundPort}/second?case=h2",
                context: wirestackOperationContext()
            )
            try {
                wirestackAssertHttp2Headers(second)
                @Assert(wirestackReadHttpUntil(second.body, "second-open").contains("second-open"))
                @Assert(resolver.calls.load(), 1)
                @Assert(firstEndpoint.hub.connectionCount, 1)
                @Assert(secondEndpoint.hub.connectionCount, 1)
                let readStarted = WirestackTestLatch()
                let terminated: Future<Unit> = spawn {
                    =>
                        readStarted.signal()
                        try {
                            let buffer = Array<Byte>(64, repeat: 0u8)
                            while (first.body.read(buffer) > 0) {}
                        } catch (_: Exception) {
                            // Request abort can race the awakened body's EOF.
                        }
                }
                try {
                    @Assert(readStarted.await())
                    firstEndpoint.closeNow()
                    terminated.get(Duration.second * 2)
                    @Assert(firstEndpoint.hub.connectionCount, 0)
                    let stats = secondEndpoint.hub.broadcast(SseFrame(id: Some("2"), data: Some("sibling-survived")))
                    @Assert(stats.accepted, 1)
                    @Assert(wirestackReadHttpUntil(second.body, "sibling-survived").contains("sibling-survived"))
                    @Assert(secondEndpoint.hub.connectionCount, 1)
                    @Assert(resolver.calls.load(), 1)
                } finally {
                    first.close()
                    try {
                        terminated.get(Duration.second * 2)
                    } catch (_: Exception) {}
                }
            } finally {
                second.close()
            }
        } finally {
            first.close()
        }
    } finally {
        client.close()
        server.closeNow()
        clientTls.close()
        serverTls.close()
    }
}

@Test
func wirestackHttp2GracefulCloseDeliversEndStream() {
    let certificate = wirestackTestCertificateDer()
    let serverTls = HttpServerTlsConfig([certificate], wirestackTestPrivateKeyDer())
    let endpoint = SseEndpoint("/graceful", config: SseEndpointConfig(initialComment: Some("graceful-open")))
    let server = SseServer(
        config: SseServerConfig(
            listenEndpoint: wirestackTestLoopback(0),
            maxConnections: 1
        )
    )
    server.transportFactory = WirestackSseServerTransportFactory(tls: Some(serverTls))
    server.register(endpoint)
    server.start()
    server.awaitBound()
    let (client, clientTls, resolver) = wirestackHttp2Client(certificate)
    try {
        let opened = client.get(
            "https://example.com:${server.boundPort}/graceful?case=end-stream",
            context: wirestackOperationContext()
        )
        try {
            wirestackAssertHttp2Headers(opened)
            @Assert(resolver.calls.load(), 1)
            let stats = endpoint.hub.broadcast(SseFrame(id: Some("3"), data: Some("before-graceful")))
            @Assert(stats.accepted, 1)
            let closing: Future<Unit> = spawn {=> server.closeGracefully(2000)}
            try {
                let payload = wirestackDrainHttpBody(opened.body)
                @Assert(payload.contains("graceful-open"))
                @Assert(payload.contains("before-graceful"))
                @Assert(opened.body.isFullyConsumed())
                closing.get(Duration.second * 3)
            } finally {
                opened.close()
                server.closeNow()
                try {
                    closing.get(Duration.second * 2)
                } catch (_: Exception) {}
            }
        } finally {
            opened.close()
        }
    } finally {
        server.closeNow()
        client.close()
        clientTls.close()
        serverTls.close()
    }
}
