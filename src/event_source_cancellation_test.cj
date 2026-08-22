package sse4cj

import std.sync.*
import std.time.MonoTime

private class BlockingSleeper <: SseReconnectSleeper {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private var entered = false
    private var interrupted = false

    public func sleep(_delayMillis: Int64): Bool {
        let _ = _delayMillis
        synchronized(mutex) {
            entered = true
            condition.notifyAll()
            while (!interrupted) {
                condition.wait()
            }
            false
        }
    }
    public func interrupt(): Unit {
        synchronized(mutex) {
            interrupted = true
            condition.notifyAll()
        }
    }
    func awaitEntered(): Bool {
        let deadline = MonoTime.now() + Duration.second
        synchronized(mutex) {
            while (!entered) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                condition.wait(timeout: remaining)
            }
            entered
        }
    }
}

private class BlockingConnectTransport <: SseClientTransport {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private var entered = false
    private var interrupted = false

    public func send(_request: SseHttpRequest, cancellation: Option<SseCancellationToken>): SseTransportResponse {
        let _ = _request
        let token = match (cancellation) {
            case Some(value) => value
            case None => throw SseException(SseErrorKind.InternalInvariantViolation, "test requires cancellation")
        }
        let id = token.registerInterrupt({=> interruptConnect()})
        synchronized(mutex) {
            entered = true
            condition.notifyAll()
            while (!interrupted) {
                condition.wait()
            }
        }
        token.clearInterrupt(id)
        throw SseException(SseErrorKind.Cancelled, "connect cancelled")
    }

    private func interruptConnect(): Unit {
        synchronized(mutex) {
            interrupted = true
            condition.notifyAll()
        }
    }

    func awaitEntered(): Bool {
        let deadline = MonoTime.now() + Duration.second
        synchronized(mutex) {
            while (!entered) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                condition.wait(timeout: remaining)
            }
            entered
        }
    }
}

private class OpenSignalCallbacks <: EventSourceCallbacks {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private var opened = false
    public override func onOpen(): Unit {
        synchronized(mutex) {
            opened = true
            condition.notifyAll()
        }
    }
    func awaitOpen(): Bool {
        let deadline = MonoTime.now() + Duration.second * 3
        synchronized(mutex) {
            while (!opened) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                condition.wait(timeout: remaining)
            }
            opened
        }
    }
}

@Test
func eventSourceCloseInterruptsBlockingConnect() {
    let transport = BlockingConnectTransport()
    let client = EventSourceClient(
        EventSourceConfig("https://events.test/stream"),
        transport: transport,
        sleeper: RecordingSleeper(),
        jitterSource: ZeroJitter()
    )
    client.start()
    @Assert(transport.awaitEntered())
    client.close()
    client.awaitClosed()
    @Assert(match (client.readyState) { case EventSourceReadyState.CLOSED => true; case _ => false })
}

@Test
func eventSourceCloseInterruptsReconnectSleep() {
    let response = EventSourceResponse(200, bodyText: "")
    let transport = EventSourceScriptTransport()
    transport.addResponse(response)
    let sleeper = BlockingSleeper()
    let client = EventSourceClient(
        EventSourceConfig("https://events.test/stream",
            reconnectPolicy: SseReconnectPolicy(initialDelayMillis: 5000, maxDelayMillis: 5000)),
        transport: transport,
        sleeper: sleeper,
        jitterSource: ZeroJitter()
    )
    client.start()
    @Assert(sleeper.awaitEntered())
    client.close()
    client.awaitClosed()
    @Assert(response.closed)
}

@Test
func eventSourceCloseInterruptsRealStdxBlockingRead() {
    let endpoint = SseEndpoint("/events")
    let server = SseServer(
        config: SseServerConfig(address: "127.0.0.1", port: 0, readTimeoutMillis: 5000, writeTimeoutMillis: 5000))
    server.register(endpoint)
    server.start()
    server.awaitBound()

    let callbacks = OpenSignalCallbacks()
    let client = EventSourceClient(
        EventSourceConfig(
            "http://127.0.0.1:${server.boundPort}/events",
            connectTimeoutMillis: 2000,
            readTimeoutMillis: 5000,
            reconnectPolicy: SseReconnectPolicy(initialDelayMillis: 5000, maxDelayMillis: 5000)
        ),
        callbacks: callbacks
    )
    client.start()
    @Assert(callbacks.awaitOpen())
    @Assert(endpoint.hub.connectionCount, 1)
    client.close()
    client.awaitClosed()
    server.closeNow()
    @Assert(endpoint.hub.connectionCount, 0)
}
