package sse4cj

import std.sync.*
import std.time.*
import stdx.net.http.*

private class LifecycleBlockingWriter <: SseConnectionWriter {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private var entered = false
    private var released = false

    public func write(frame: EncodedSseFrame): Unit {
        let _ = frame.byteLength
        let deadline = MonoTime.now() + Duration.second * 5
        synchronized(mutex) {
            entered = true
            condition.notifyAll()
            while (!released) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                condition.wait(timeout: remaining)
            }
        }
    }

    public func flush(): Unit {}
    public func close(): Unit {
        synchronized(mutex) {
            condition.notifyAll()
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
                condition.wait(timeout: remaining)
            }
            entered
        }
    }

    func release(): Unit {
        synchronized(mutex) {
            released = true
            condition.notifyAll()
        }
    }
}

private class LifecycleServerTransport <: SseServerTransport {
    private let mutex = Mutex()
    private let changed = synchronized(mutex) { mutex.condition() }
    private var afterBind: Option<() -> Unit> = None<() -> Unit>
    private var serveEntered = false
    private var closeCalls: Int64 = 0

    public prop port: UInt16 { get() { 42424 } }

    public func register(path: String, handler: (HttpContext) -> Unit): Unit {
        let _ = path
        let _ = handler
    }

    public func serve(): Unit {
        match (synchronized(mutex) { afterBind }) {
            case Some(action) => action()
            case None => ()
        }
        let deadline = MonoTime.now() + Duration.second * 5
        synchronized(mutex) {
            serveEntered = true
            changed.notifyAll()
            while (closeCalls == 0) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                changed.wait(timeout: remaining)
            }
        }
    }

    public func close(): Unit {
        synchronized(mutex) {
            closeCalls++
            changed.notifyAll()
        }
    }

    func setAfterBind(action: () -> Unit): Unit { synchronized(mutex) { afterBind = Some(action) } }

    func awaitServe(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!serveEntered) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                changed.wait(timeout: remaining)
            }
            serveEntered
        }
    }

    prop closeCount: Int64 { get() { synchronized(mutex) { closeCalls } } }
}

private class LifecycleServerTransportFactory <: SseServerTransportFactory {
    let transport = LifecycleServerTransport()
    private let blockBuild: Bool
    private let mutex = Mutex()
    private let changed = synchronized(mutex) { mutex.condition() }
    private var buildEntered = false
    private var buildReleased = false

    init(blockBuild!: Bool = false) { this.blockBuild = blockBuild }

    public func build(_config: SseServerConfig, afterBind: () -> Unit): SseServerTransport {
        let _ = _config
        transport.setAfterBind(afterBind)
        if (blockBuild) {
            let deadline = MonoTime.now() + Duration.second * 5
            synchronized(mutex) {
                buildEntered = true
                changed.notifyAll()
                while (!buildReleased) {
                    let remaining = deadline - MonoTime.now()
                    if (remaining <= Duration.Zero) { break }
                    changed.wait(timeout: remaining)
                }
            }
        }
        transport
    }

    func awaitBuild(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!buildEntered) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                changed.wait(timeout: remaining)
            }
            buildEntered
        }
    }

    func releaseBuild(): Unit {
        synchronized(mutex) {
            buildReleased = true
            changed.notifyAll()
        }
    }
}

private func awaitLifecycleEndpointClosing(endpoint: SseEndpoint, timeoutMillis!: Int64 = 1000): Bool {
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

private func attachBlockedLifecycleConnection(endpoint: SseEndpoint, id: String): (SseConnection, 
    LifecycleBlockingWriter) {
    let connection = SseConnection(id, config: endpoint.config.connection)
    endpoint.attachConnection(connection, "")
    connection.send(SseFrame(data: Some("blocked")))
    let writer = LifecycleBlockingWriter()
    connection.start(writer)
    @Assert(writer.awaitEntered())
    (connection, writer)
}

@Test
func serverCloseWinningBeforeStartPublishClosesLoserTransportOnce() {
    let factory = LifecycleServerTransportFactory(blockBuild: true)
    let server = SseServer()
    server.transportFactory = factory
    let startTask: Future<Unit> = spawn {=> server.start()}
    @Assert(factory.awaitBuild())
    server.closeNow()
    factory.releaseBuild()
    startTask.get(Duration.second)
    @Assert(factory.transport.closeCount, 1)
}

@Test
func serverGracefulTransportCloseIsStrongAndIdempotent() {
    let factory = LifecycleServerTransportFactory()
    let server = SseServer()
    server.transportFactory = factory
    server.start()
    @Assert(factory.transport.awaitServe())

    server.closeGracefully(1000)
    @Assert(factory.transport.closeCount, 1)
    server.closeNow()
    @Assert(factory.transport.closeCount, 1)
}

@Test
func serverConcurrentGracefulUpgradeClosesTransportOnce() {
    let endpoint = SseEndpoint("/lifecycle-fake-upgrade",
        config: SseEndpointConfig(initialComment: None<String>))
    let (connection, writer) = attachBlockedLifecycleConnection(endpoint, "lifecycle-fake-upgrade-blocked")
    let factory = LifecycleServerTransportFactory()
    let server = SseServer()
    server.transportFactory = factory
    server.register(endpoint)
    server.start()
    @Assert(factory.transport.awaitServe())

    let graceful: Future<Unit> = spawn {=> server.closeGracefully(5000)}
    @Assert(awaitLifecycleEndpointClosing(endpoint))
    server.closeNow()
    graceful.get(Duration.second)
    @Assert(factory.transport.closeCount, 1)
    writer.release()
    @Assert(connection.awaitClosed(1000))
}

@Test
func serverGracefulCloseClosesEveryAdmissionGateBeforeDrain() {
    let first = SseEndpoint("/lifecycle-first", config: SseEndpointConfig(initialComment: None<String>))
    let second = SseEndpoint("/lifecycle-second", config: SseEndpointConfig(initialComment: None<String>))
    let (firstConnection, firstWriter) = attachBlockedLifecycleConnection(first, "lifecycle-first-blocked")
    let (secondConnection, secondWriter) = attachBlockedLifecycleConnection(second, "lifecycle-second-blocked")
    let server = SseServer()
    server.register(first)
    server.register(second)

    let graceful: Future<Unit> = spawn {=> server.closeGracefully(5_000)}
    @Assert(awaitLifecycleEndpointClosing(first))
    @Assert(awaitLifecycleEndpointClosing(second))
    @Assert(graceful.tryGet().isNone())

    let request = SseServerRequest("GET", "local", "", {_: String => None<String>})
    let decision = second.decide(request)
    @Assert(!decision.accepted)
    @Assert(decision.rejectStatus, 503)

    firstWriter.release()
    secondWriter.release()
    graceful.get(Duration.second)
    @Assert(firstConnection.awaitClosed(1000))
    @Assert(secondConnection.awaitClosed(1000))
}

@Test
func serverCloseNowUpgradesConcurrentGracefulClose() {
    let endpoint = SseEndpoint("/lifecycle-upgrade", config: SseEndpointConfig(initialComment: None<String>))
    let (connection, writer) = attachBlockedLifecycleConnection(endpoint, "lifecycle-upgrade-blocked")
    let server = SseServer()
    server.register(endpoint)

    let graceful: Future<Unit> = spawn {=> server.closeGracefully(5_000)}
    @Assert(awaitLifecycleEndpointClosing(endpoint))
    @Assert(graceful.tryGet().isNone())

    server.closeNow()
    graceful.get(Duration.second)
    @Assert(connection.isClosed)
    @Assert(!connection.isTransportClosed)
    @Assert(endpoint.hub.connectionCount, 0)

    let startRejected: Future<Bool> = spawn {
        => try {
            server.start()
            false
        } catch (_: SseException) {
            true
        }
    }
    @Assert(startRejected.get(Duration.second))

    // Both public terminal operations remain idempotent after force upgrade.
    server.closeNow()
    server.closeGracefully(1_000)

    writer.release()
    @Assert(connection.awaitClosed(1000))
}

@Test
func serverRejectsRegistrationAfterStartBoundary() {
    let server = SseServer()
    server.register(SseEndpoint("/lifecycle-started", config: SseEndpointConfig(initialComment: None<String>)))
    server.start()
    server.awaitBound()

    let registerRejected: Future<Bool> = spawn {
        => try {
            server.register(SseEndpoint("/lifecycle-too-late",
                config: SseEndpointConfig(initialComment: None<String>)))
            false
        } catch (_: SseException) {
            true
        }
    }
    @Assert(registerRejected.get(Duration.second))
    server.closeNow()
}

@Test
func serverGracefulCloseRejectsOverflowingTimeout() {
    let server = SseServer()
    var caught = false
    try {
        server.closeGracefully(Int64.Max)
    } catch (error: SseException) {
        caught = match (error.kind) {
            case SseErrorKind.InvalidConfiguration => true
            case _ => false
        }
    }
    @Assert(caught)
}
