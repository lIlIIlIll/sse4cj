package sse4cj

import std.collection.*
import std.io.InputStream

private func eventSourceBytes(text: String): Array<Byte> {
    let bytes = Array<Byte>(text.size, repeat: 0)
    unsafe { text.rawData().copyTo(bytes, 0, 0, text.size) }
    bytes
}

private class EventSourceInput <: InputStream {
    private let bytes: Array<Byte>
    private var offset: Int64 = 0

    init(text: String) {
        bytes = eventSourceBytes(text)
    }

    public func read(buffer: Array<Byte>): Int64 {
        if (offset >= bytes.size) {
            return 0
        }
        let remaining = bytes.size - offset
        let count = if (remaining < buffer.size) {
            remaining
        } else {
            buffer.size
        }
        bytes.copyTo(buffer, offset, 0, count)
        offset += count
        count
    }
}

class EventSourceResponse <: SseTransportResponse {
    private let code: UInt16
    private let mime: Option<String>
    private let stream: InputStream
    var closed = false

    init(status: UInt16, bodyText!: String = "", contentType!: Option<String> = Some("text/event-stream")) {
        code = status
        mime = contentType
        stream = EventSourceInput(bodyText)
    }

    public func status(): UInt16 {
        code
    }
    public func header(name: String): Option<String> {
        if (name == "Content-Type") {
            mime
        } else {
            None<String>
        }
    }
    public func body(): InputStream {
        stream
    }
    public func origin(): Option<String> {
        Some("https://events.test")
    }
    public func close(): Unit {
        closed = true
    }
}

class EventSourceScriptTransport <: SseClientTransport {
    private let responses = ArrayList<EventSourceResponse>()
    private let failCalls = ArrayList<Int64>()
    let requests = ArrayList<SseHttpRequest>()
    var calls: Int64 = 0

    func addResponse(response: EventSourceResponse): Unit {
        responses.add(response)
    }
    func addFailure(callIndex: Int64): Unit {
        failCalls.add(callIndex)
    }

    public func send(request: SseHttpRequest, cancellation: Option<SseCancellationToken>): SseTransportResponse {
        let _ = cancellation
        calls++
        requests.add(request)
        for (callIndex in failCalls) {
            if (callIndex == calls) {
                throw SseException(SseErrorKind.TransportFailure, "scripted transport failure")
            }
        }
        let responseIndex = calls - 1 - failureCountThrough(calls)
        if (responseIndex < 0 || responseIndex >= responses.size) {
            throw SseException(SseErrorKind.TransportFailure, "script exhausted")
        }
        responses[responseIndex]
    }

    private func failureCountThrough(callIndex: Int64): Int64 {
        var count: Int64 = 0
        for (value in failCalls) {
            if (value <= callIndex) {
                count++
            }
        }
        count
    }
}

class RecordingSleeper <: SseReconnectSleeper {
    let delays = ArrayList<Int64>()
    var interrupted = false
    public func sleep(delayMillis: Int64): Bool {
        delays.add(delayMillis)
        !interrupted
    }
    public func interrupt(): Unit {
        interrupted = true
    }
}

class ZeroJitter <: SseJitterSource {
    public func nextInt64(upperExclusive: Int64): Int64 {
        upperExclusive / 2
    }
}

class RecordingEventSourceCallbacks <: EventSourceCallbacks {
    var opens: Int64 = 0
    var closedCount: Int64 = 0
    let messages = ArrayList<SseEvent>()
    let named = ArrayList<SseEvent>()
    let any = ArrayList<SseEvent>()
    let retries = ArrayList<Int64>()
    let errors = ArrayList<SseErrorKind>()
    var closeClient: Option<EventSourceClient> = None<EventSourceClient>
    var closeOnFirstMessage = false
    var throwOnMessage = false

    public override func onOpen(): Unit {
        opens++
    }
    public override func onMessage(event: SseEvent): Unit {
        messages.add(event)
        if (throwOnMessage) {
            throw Exception("callback boom")
        }
        if (closeOnFirstMessage) {
            match (closeClient) {
                case Some(client) => client.close()
                case None => ()
            }
        }
    }
    public override func onEvent(event: SseEvent): Unit {
        named.add(event)
    }
    public override func onAnyEvent(event: SseEvent): Unit {
        any.add(event)
    }
    public override func onError(error: SseException): Unit {
        errors.add(error.kind)
    }
    public override func onRetry(retryMillis: Int64): Unit {
        retries.add(retryMillis)
    }
    public override func onClosed(): Unit {
        closedCount++
    }
}

private func requestHeader(request: SseHttpRequest, name: String): Option<String> {
    for (header in request.headers()) {
        if (header.name == name) {
            return Some(header.value)
        }
    }
    None<String>
}

private func errorListContains(values: ArrayList<SseErrorKind>, expected: SseErrorKind): Bool {
    for (value in values) {
        if (value == expected) {
            return true
        }
    }
    false
}

@Test
func eventSourceReconnectsOnEofCarriesIdAndStopsOn204() {
    let first = EventSourceResponse(200, bodyText: "id:42\nretry:25\ndata:first\n\n")
    let second = EventSourceResponse(204)
    let transport = EventSourceScriptTransport()
    transport.addResponse(first)
    transport.addResponse(second)
    let sleeper = RecordingSleeper()
    let callbacks = RecordingEventSourceCallbacks()
    let client = EventSourceClient(
        EventSourceConfig(
            "https://events.test/stream",
            reconnectPolicy: SseReconnectPolicy(initialDelayMillis: 100, maxDelayMillis: 1000, maxAttempts: Some(4))
        ),
        callbacks: callbacks,
        transport: transport,
        sleeper: sleeper,
        jitterSource: ZeroJitter()
    )
    client.run()

    @Assert(transport.calls, 2)
    @Assert(first.closed)
    @Assert(second.closed)
    @Assert(callbacks.opens, 1)
    @Assert(callbacks.messages.size, 1)
    @Assert(callbacks.messages[0].data, "first")
    @Assert(callbacks.messages[0].origin, Some("https://events.test"))
    @Assert(callbacks.retries.size, 1)
    @Assert(callbacks.retries[0], 25)
    @Assert(sleeper.delays.size, 1)
    @Assert(sleeper.delays[0], 25)
    @Assert(requestHeader(transport.requests[0], "Last-Event-ID"), None<String>)
    @Assert(requestHeader(transport.requests[1], "Last-Event-ID"), Some("42"))
    @Assert(callbacks.closedCount, 1)
    @Assert(client.readyState == EventSourceReadyState.CLOSED)
}

@Test
func eventSourceEmptyIdRemovesReconnectHeaderAndNamedDoesNotFallbackToMessage() {
    let first = EventSourceResponse(200, bodyText: "id:old\n\ndata:x\n\nid:\n\nevent:custom\ndata:named\n\n")
    let second = EventSourceResponse(204)
    let transport = EventSourceScriptTransport()
    transport.addResponse(first)
    transport.addResponse(second)
    let callbacks = RecordingEventSourceCallbacks()
    let client = EventSourceClient(
        EventSourceConfig("https://events.test/stream",
            reconnectPolicy: SseReconnectPolicy(initialDelayMillis: 0, maxDelayMillis: 100)),
        callbacks: callbacks,
        transport: transport,
        sleeper: RecordingSleeper(),
        jitterSource: ZeroJitter()
    )
    client.run()

    @Assert(callbacks.messages.size, 1)
    @Assert(callbacks.named.size, 1)
    @Assert(callbacks.named[0].eventType, "custom")
    @Assert(callbacks.any.size, 2)
    @Assert(client.lastEventId, "")
    @Assert(requestHeader(transport.requests[1], "Last-Event-ID"), None<String>)
}

@Test
func eventSourceRecoversTransportFailureAndHonorsMaxAttempts() {
    let transport = EventSourceScriptTransport()
    transport.addFailure(1)
    transport.addFailure(2)
    transport.addFailure(3)
    let callbacks = RecordingEventSourceCallbacks()
    let sleeper = RecordingSleeper()
    let client = EventSourceClient(
        EventSourceConfig(
            "https://events.test/stream",
            reconnectPolicy: SseReconnectPolicy(
                mode: SseReconnectMode.ExponentialBackoff,
                initialDelayMillis: 10,
                maxDelayMillis: 100,
                maxAttempts: Some(3)
            )
        ),
        callbacks: callbacks,
        transport: transport,
        sleeper: sleeper,
        jitterSource: ZeroJitter()
    )
    client.run()

    @Assert(transport.calls, 3)
    @Assert(sleeper.delays.size, 3)
    @Assert(sleeper.delays[0], 10)
    @Assert(sleeper.delays[1], 20)
    @Assert(sleeper.delays[2], 40)
    @Assert(errorListContains(callbacks.errors, SseErrorKind.TransportFailure))
    @Assert(errorListContains(callbacks.errors, SseErrorKind.ReconnectExhausted))
    @Assert(callbacks.closedCount, 1)
}

@Test
func eventSourceHttpRejectionIsTerminalAndResponseCloses() {
    let response = EventSourceResponse(200, contentType: Some("application/json"))
    let transport = EventSourceScriptTransport()
    transport.addResponse(response)
    let callbacks = RecordingEventSourceCallbacks()
    let client = EventSourceClient(
        EventSourceConfig("https://events.test/stream"),
        callbacks: callbacks,
        transport: transport,
        sleeper: RecordingSleeper(),
        jitterSource: ZeroJitter()
    )
    client.run()
    @Assert(response.closed)
    @Assert(errorListContains(callbacks.errors, SseErrorKind.HttpResponseRejected))
    @Assert(callbacks.closedCount, 1)
}

@Test
func eventSourceCloseInsideCallbackStopsLaterCallbacksAndReconnects() {
    let response = EventSourceResponse(200, bodyText: "data:first\n\ndata:second\n\n")
    let transport = EventSourceScriptTransport()
    transport.addResponse(response)
    let callbacks = RecordingEventSourceCallbacks()
    callbacks.closeOnFirstMessage = true
    let client = EventSourceClient(
        EventSourceConfig("https://events.test/stream"),
        callbacks: callbacks,
        transport: transport,
        sleeper: RecordingSleeper(),
        jitterSource: ZeroJitter()
    )
    callbacks.closeClient = Some(client)
    client.run()
    @Assert(callbacks.messages.size, 1)
    @Assert(callbacks.any.size, 0)
    @Assert(transport.calls, 1)
    @Assert(callbacks.closedCount, 1)
}

@Test
func eventSourceCallbackPolicyForwardAndContinueAreBounded() {
    let response1 = EventSourceResponse(200, bodyText: "data:x\n\n")
    let stop1 = EventSourceResponse(204)
    let transport1 = EventSourceScriptTransport()
    transport1.addResponse(response1)
    transport1.addResponse(stop1)
    let callbacks1 = RecordingEventSourceCallbacks()
    callbacks1.throwOnMessage = true
    let client1 = EventSourceClient(
        EventSourceConfig(
            "https://events.test/stream",
            reconnectPolicy: SseReconnectPolicy(initialDelayMillis: 0, maxDelayMillis: 10),
            callbackExceptionPolicy: SseCallbackExceptionPolicy.ForwardToErrorHandler
        ),
        callbacks: callbacks1,
        transport: transport1,
        sleeper: RecordingSleeper(),
        jitterSource: ZeroJitter()
    )
    client1.run()
    @Assert(errorListContains(callbacks1.errors, SseErrorKind.InternalInvariantViolation))

    let response2 = EventSourceResponse(200, bodyText: "data:x\n\n")
    let stop2 = EventSourceResponse(204)
    let transport2 = EventSourceScriptTransport()
    transport2.addResponse(response2)
    transport2.addResponse(stop2)
    let callbacks2 = RecordingEventSourceCallbacks()
    callbacks2.throwOnMessage = true
    let client2 = EventSourceClient(
        EventSourceConfig(
            "https://events.test/stream",
            reconnectPolicy: SseReconnectPolicy(initialDelayMillis: 0, maxDelayMillis: 10),
            callbackExceptionPolicy: SseCallbackExceptionPolicy.Continue
        ),
        callbacks: callbacks2,
        transport: transport2,
        sleeper: RecordingSleeper(),
        jitterSource: ZeroJitter()
    )
    client2.run()
    @Assert(callbacks2.errors.size, 0)
}

@Test
func eventSourceCallbackClosePolicyClosesWithoutReconnect() {
    let response = EventSourceResponse(200, bodyText: "data:x\n\n")
    let transport = EventSourceScriptTransport()
    transport.addResponse(response)
    let callbacks = RecordingEventSourceCallbacks()
    callbacks.throwOnMessage = true
    let client = EventSourceClient(
        EventSourceConfig(
            "https://events.test/stream",
            callbackExceptionPolicy: SseCallbackExceptionPolicy.Close
        ),
        callbacks: callbacks,
        transport: transport,
        sleeper: RecordingSleeper(),
        jitterSource: ZeroJitter()
    )
    client.run()
    @Assert(transport.calls, 1)
    @Assert(response.closed)
    @Assert(callbacks.closedCount, 1)
    @Assert(client.readyState == EventSourceReadyState.CLOSED)
}

@Test
func eventSourceCloseIsIdempotent() {
    let callbacks = RecordingEventSourceCallbacks()
    let client = EventSourceClient(
        EventSourceConfig("https://events.test/stream"),
        callbacks: callbacks,
        transport: EventSourceScriptTransport(),
        sleeper: RecordingSleeper(),
        jitterSource: ZeroJitter()
    )
    client.close()
    client.close()
    client.close()
    @Assert(callbacks.closedCount, 1)
    @Assert(client.readyState == EventSourceReadyState.CLOSED)
}
