package sse4cj

import std.collection.*
import std.sync.*

public class EventSourceClient {
    private let config: EventSourceConfig
    private let transport: SseClientTransport
    private let callbacks: EventSourceCallbacks
    private let sleeper: SseReconnectSleeper
    private let jitterSource: SseJitterSource
    private let dispatcher: SseCallbackDispatcher
    private let stateMutex = Mutex()
    private let running = AtomicBool(false)
    private let closed = AtomicBool(false)
    private let closedNotified = AtomicBool(false)
    private let discardPendingCallbacks = AtomicBool(false)
    private var stateValue = EventSourceReadyState.CLOSED
    private var currentLastEventId = ""
    private var serverRetryMillis: Option<Int64> = None<Int64>
    private var activeCancellation: Option<SseCancellationToken> = None<SseCancellationToken>
    private var worker: Option<Future<Unit>> = None<Future<Unit>>

    public init(
        config: EventSourceConfig,
        callbacks!: EventSourceCallbacks = EventSourceCallbacks(),
        transport!: SseClientTransport = StdxSseClientTransport(),
        sleeper!: SseReconnectSleeper = DefaultSseReconnectSleeper(),
        jitterSource!: SseJitterSource = DefaultSseJitterSource(),
        dispatcher!: SseCallbackDispatcher = InlineSseCallbackDispatcher()
    ) {
        this.config = config
        this.callbacks = callbacks
        this.transport = transport
        this.sleeper = sleeper
        this.jitterSource = jitterSource
        this.dispatcher = dispatcher
    }

    public prop readyState: EventSourceReadyState {
        get() {
            synchronized(stateMutex) {
                stateValue
            }
        }
    }

    public prop lastEventId: String {
        get() {
            synchronized(stateMutex) {
                currentLastEventId
            }
        }
    }

    public func start(): Unit {
        if (closed.load()) {
            throw SseException(SseErrorKind.Closed, "EventSourceClient is closed")
        }
        if (!running.compareAndSwap(false, true)) {
            throw SseException(SseErrorKind.InvalidConfiguration, "EventSourceClient is already running")
        }
        setState(EventSourceReadyState.CONNECTING)
        let future: Future<Unit> = spawn {
            => runLoopAlreadyStarted()
        }
        synchronized(stateMutex) {
            worker = Some(future)
        }
    }

    // Blocking form used by deterministic tests and embedding code that owns its worker.
    public func run(): Unit {
        if (closed.load()) {
            throw SseException(SseErrorKind.Closed, "EventSourceClient is closed")
        }
        if (!running.compareAndSwap(false, true)) {
            throw SseException(SseErrorKind.InvalidConfiguration, "EventSourceClient is already running")
        }
        setState(EventSourceReadyState.CONNECTING)
        runLoopAlreadyStarted()
    }

    public func awaitClosed(): Unit {
        let active = synchronized(stateMutex) {
            worker
        }
        match (active) {
            case Some(value) => value.get()
            case None => ()
        }
    }

    public func close(): Unit {
        discardPendingCallbacks.store(true)
        dispatcher.closeNow()
        let wasClosed = closed.swap(true)
        setState(EventSourceReadyState.CLOSED)
        let token = synchronized(stateMutex) {
            activeCancellation
        }
        match (token) {
            case Some(value) => value.cancel()
            case None => ()
        }
        sleeper.interrupt()
        if (!wasClosed) {
            notifyClosedOnce()
        }
    }

    private func runLoopAlreadyStarted(): Unit {
        var connectionAttempts: Int64 = 0
        var reconnectIndex: Int64 = 0
        try {
            while (!closed.load()) {
                if (attemptLimitReached(connectionAttempts)) {
                    terminalError(
                        SseException(SseErrorKind.ReconnectExhausted, "EventSource reconnect attempts exhausted"))
                    return
                }
                connectionAttempts++
                setState(EventSourceReadyState.CONNECTING)
                let cancellation = SseCancellationToken()
                synchronized(stateMutex) {
                    activeCancellation = Some(cancellation)
                }

                let outcome = tryAttempt(cancellation)
                synchronized(stateMutex) {
                    activeCancellation = None<SseCancellationToken>
                }
                if (closed.load()) {
                    return
                }
                match (outcome) {
                    case EventSourceAttemptOutcome.Stop =>
                        closed.store(true)
                        setState(EventSourceReadyState.CLOSED)
                        return
                    case EventSourceAttemptOutcome.Terminal(error) =>
                        terminalError(error)
                        return
                    case EventSourceAttemptOutcome.Reconnect(opened) =>
                        if (opened) {
                            reconnectIndex = 0
                        }
                        let delay = config.reconnectPolicy.delayMillis(
                            reconnectIndex,
                            synchronized(stateMutex) {
                                serverRetryMillis
                            },
                            jitterSource
                        )
                        if (!opened) {
                            reconnectIndex++
                        }
                        if (!sleeper.sleep(delay)) {
                            return
                        }
                }
            }
        } finally {
            running.store(false)
            synchronized(stateMutex) {
                activeCancellation = None<SseCancellationToken>
                worker = None<Future<Unit>>
            }
            setState(EventSourceReadyState.CLOSED)
            if (discardPendingCallbacks.load()) {
                dispatcher.closeNow()
            } else {
                dispatcher.closeGracefully()
            }
            notifyClosedOnce()
        }
    }

    private func tryAttempt(cancellation: SseCancellationToken): EventSourceAttemptOutcome {
        var opened = false
        try {
            let value = transport.send(buildRequest(), Some(cancellation))
            try {
                if (value.status() == 204) {
                    return EventSourceAttemptOutcome.Stop
                }
                if (value.status() != 200 || !responseHasEventStreamMime(value)) {
                    return EventSourceAttemptOutcome.Terminal(
                        SseException(SseErrorKind.HttpResponseRejected, "EventSource HTTP response rejected"))
                }
                setState(EventSourceReadyState.OPEN)
                opened = true
                if (!invokeOpen()) {
                    return EventSourceAttemptOutcome.Stop
                }
                let sink = EventSourceDecodeSink(this, value.origin())
                let reader = SseStreamReader(
                    config: SseStreamReaderConfig(readBufferSize: config.readBufferSize,
                        decoderLimits: config.decoderLimits))
                reader.read(value.body(), sink, cancellation: Some(cancellation))
                return EventSourceAttemptOutcome.Reconnect(true)
            } finally {
                value.close()
            }
        } catch (error: SseException) {
            if (closed.load() || isSseErrorKind(error.kind, SseErrorKind.Cancelled)) {
                return EventSourceAttemptOutcome.Stop
            }
            if (isSseErrorKind(error.kind, SseErrorKind.TransportFailure)) {
                invokeError(error)
                return EventSourceAttemptOutcome.Reconnect(opened)
            }
            return EventSourceAttemptOutcome.Terminal(error)
        } catch (error: Exception) {
            if (closed.load()) {
                return EventSourceAttemptOutcome.Stop
            }
            let wrapped = SseException(SseErrorKind.TransportFailure, "EventSource attempt failed: ${error.message}")
            invokeError(wrapped)
            return EventSourceAttemptOutcome.Reconnect(opened)
        }
    }

    private func buildRequest(): SseHttpRequest {
        let headers = ArrayList<SseHeader>()
        for (header in config.headers) {
            headers.add(header)
        }
        let id = synchronized(stateMutex) {
            currentLastEventId
        }
        if (id.size > 0) {
            headers.add(SseHeader("Last-Event-ID", id))
        }
        SseHttpRequest(
            config.url,
            headers: headers.toArray(),
            connectTimeoutMillis: config.connectTimeoutMillis,
            readTimeoutMillis: config.readTimeoutMillis
        )
    }

    private func attemptLimitReached(connectionAttempts: Int64): Bool {
        match (config.reconnectPolicy.maxAttempts) {
            case Some(maximum) => connectionAttempts >= maximum
            case None => false
        }
    }

    private func responseHasEventStreamMime(response: SseTransportResponse): Bool {
        match (response.header("Content-Type")) {
            case Some(value) => isEventStreamContentType(value)
            case None => false
        }
    }

    private func setState(value: EventSourceReadyState): Unit {
        synchronized(stateMutex) {
            stateValue = value
        }
    }

    func decoderLastEventIdChanged(id: String): Unit {
        synchronized(stateMutex) {
            currentLastEventId = id
        }
    }

    func decoderRetryChanged(value: Int64): Unit {
        synchronized(stateMutex) {
            serverRetryMillis = Some(value)
        }
        invokeRetry(value)
    }

    func decoderEvent(event: SseEvent, origin: Option<String>): Unit {
        if (closed.load()) {
            return
        }
        let withOrigin = SseEvent(event.eventType, event.data, event.lastEventId, origin: origin)
        if (withOrigin.eventType == "message") {
            if (!invokeCallback({=> callbacks.onMessage(withOrigin)})) {
                return
            }
        } else {
            if (!invokeCallback({=> callbacks.onEvent(withOrigin)})) {
                return
            }
        }
        if (!closed.load()) {
            invokeCallback({=> callbacks.onAnyEvent(withOrigin)})
        }
    }

    private func invokeOpen(): Bool {
        invokeCallback({=> callbacks.onOpen()})
    }

    private func invokeRetry(value: Int64): Bool {
        invokeCallback({=> callbacks.onRetry(value)})
    }

    private func invokeCallback(action: () -> Unit): Bool {
        if (closed.load()) {
            return false
        }
        let accepted = dispatcher.dispatch(
            {
                =>
                    if (discardPendingCallbacks.load()) {
                        return
                    }
                    try {
                        action()
                    } catch (error: Exception) {
                        handleCallbackException(error)
                    }
            }
        )
        if (!accepted) {
            invokeError(
                SseException(SseErrorKind.SlowConsumer, "EventSource callback dispatcher queue is full or closed"))
            close()
            return false
        }
        !closed.load()
    }

    private func handleCallbackException(error: Exception): Unit {
        match (config.callbackExceptionPolicy) {
            case SseCallbackExceptionPolicy.Continue => ()
            case SseCallbackExceptionPolicy.Close => close()
            case SseCallbackExceptionPolicy.ForwardToErrorHandler => invokeError(
                SseException(
                    SseErrorKind.InternalInvariantViolation,
                    "EventSource callback failed: ${error.message}"
                )
            )
        }
    }

    private func invokeError(error: SseException): Unit {
        if (closed.load()) {
            return
        }
        try {
            callbacks.onError(error)
        } catch (_: Exception) {
            close()
        }
    }

    private func terminalError(error: SseException): Unit {
        invokeError(error)
        close()
    }

    private func notifyClosedOnce(): Unit {
        if (closedNotified.swap(true)) {
            return
        }
        try {
            callbacks.onClosed()
        } catch (_: Exception) {
            ()
        }
    }
}

private enum EventSourceAttemptOutcome {
    Stop
    | Reconnect(Bool)
    | Terminal(SseException)
}

private class EventSourceDecodeSink <: SseDecodeSink {
    private let client: EventSourceClient
    private let eventOrigin: Option<String>

    init(client: EventSourceClient, origin: Option<String>) {
        this.client = client
        this.eventOrigin = origin
    }

    public func onEvent(event: SseEvent): Unit {
        client.decoderEvent(event, eventOrigin)
    }

    public func onLastEventIdChanged(id: String): Unit {
        client.decoderLastEventIdChanged(id)
    }

    public func onRetryChanged(retryMillis: Int64): Unit {
        client.decoderRetryChanged(retryMillis)
    }
}

func isSseErrorKind(left: SseErrorKind, right: SseErrorKind): Bool {
    match (left) {
        case SseErrorKind.InvalidUtf8 => match (right) {
            case SseErrorKind.InvalidUtf8 => true;
            case _ => false
        }
        case SseErrorKind.InvalidOutboundField => match (right) {
            case SseErrorKind.InvalidOutboundField => true;
            case _ => false
        }
        case SseErrorKind.LineTooLarge => match (right) {
            case SseErrorKind.LineTooLarge => true;
            case _ => false
        }
        case SseErrorKind.FieldTooLarge => match (right) {
            case SseErrorKind.FieldTooLarge => true;
            case _ => false
        }
        case SseErrorKind.EventTooLarge => match (right) {
            case SseErrorKind.EventTooLarge => true;
            case _ => false
        }
        case SseErrorKind.DataTooLarge => match (right) {
            case SseErrorKind.DataTooLarge => true;
            case _ => false
        }
        case SseErrorKind.BufferLimitExceeded => match (right) {
            case SseErrorKind.BufferLimitExceeded => true;
            case _ => false
        }
        case SseErrorKind.InvalidConfiguration => match (right) {
            case SseErrorKind.InvalidConfiguration => true;
            case _ => false
        }
        case SseErrorKind.HttpResponseRejected => match (right) {
            case SseErrorKind.HttpResponseRejected => true;
            case _ => false
        }
        case SseErrorKind.TransportFailure => match (right) {
            case SseErrorKind.TransportFailure => true;
            case _ => false
        }
        case SseErrorKind.Cancelled => match (right) {
            case SseErrorKind.Cancelled => true;
            case _ => false
        }
        case SseErrorKind.Closed => match (right) {
            case SseErrorKind.Closed => true;
            case _ => false
        }
        case SseErrorKind.ReconnectExhausted => match (right) {
            case SseErrorKind.ReconnectExhausted => true;
            case _ => false
        }
        case SseErrorKind.SlowConsumer => match (right) {
            case SseErrorKind.SlowConsumer => true;
            case _ => false
        }
        case SseErrorKind.ReplayGap => match (right) {
            case SseErrorKind.ReplayGap => true;
            case _ => false
        }
        case SseErrorKind.InternalInvariantViolation => match (right) {
            case SseErrorKind.InternalInvariantViolation => true;
            case _ => false
        }
    }
}
