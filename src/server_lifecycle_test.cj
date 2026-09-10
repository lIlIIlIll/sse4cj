package sse4cj

import std.sync.*
import std.time.*

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
                if (remaining <= Duration.Zero) {
                    break
                }
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
    private let changed = synchronized(mutex) {
        mutex.condition()
    }
    private let blockGraceful: Bool
    private let failRegister: Bool
    private var afterBind: Option<() -> Unit> = None<() -> Unit>
    private var serveEntered = false
    private var closeCalls: Int64 = 0
    private var gracefulCalls: Int64 = 0
    private var gracefulCompleted: Int64 = 0

    init(blockGraceful!: Bool = false, failRegister!: Bool = false) {
        this.blockGraceful = blockGraceful
        this.failRegister = failRegister
    }

    public prop port: UInt16 {
        get() {
            42424
        }
    }

    public func register(
        path: String,
        handler: (SseServerRequest, () -> Unit) -> SseServerTransportResponse
    ): Unit {
        let _ = path
        let _ = handler
        if (failRegister) {
            throw Exception("lifecycle register failure")
        }
    }

    public func serve(): Unit {
        match (synchronized(mutex) {
            afterBind
        }) {
            case Some(action) => action()
            case None => ()
        }
        let deadline = MonoTime.now() + Duration.second * 5
        synchronized(mutex) {
            serveEntered = true
            changed.notifyAll()
            while (closeCalls == 0 && gracefulCompleted == 0) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                changed.wait(timeout: remaining)
            }
        }
    }

    public func closeGracefully(deadline: MonoTime): Unit {
        synchronized(mutex) {
            gracefulCalls++
            changed.notifyAll()
            while (blockGraceful && closeCalls == 0) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                changed.wait(timeout: remaining)
            }
            gracefulCompleted++
            changed.notifyAll()
        }
    }

    public func close(): Unit {
        synchronized(mutex) {
            closeCalls++
            changed.notifyAll()
        }
    }

    func setAfterBind(action: () -> Unit): Unit {
        synchronized(mutex) {
            afterBind = Some(action)
        }
    }

    func awaitServe(timeoutMillis!: Int64 = 1000): Bool {
        awaitFlag(timeoutMillis, {=> serveEntered})
    }

    func awaitGraceful(timeoutMillis!: Int64 = 1000): Bool {
        awaitFlag(timeoutMillis, {=> gracefulCalls > 0})
    }

    private func awaitFlag(timeoutMillis: Int64, predicate: () -> Bool): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!predicate()) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                changed.wait(timeout: remaining)
            }
            predicate()
        }
    }

    prop closeCount: Int64 {
        get() {
            synchronized(mutex) {
                closeCalls
            }
        }
    }
    prop gracefulCount: Int64 {
        get() {
            synchronized(mutex) {
                gracefulCalls
            }
        }
    }
}

private class LifecycleServerTransportFactory <: SseServerTransportFactory {
    let transport: LifecycleServerTransport
    private let blockBuild: Bool
    private let failBuild: Bool
    private let mutex = Mutex()
    private let changed = synchronized(mutex) {
        mutex.condition()
    }
    private var buildEntered = false
    private var buildReleased = false

    init(
        blockBuild!: Bool = false,
        blockGraceful!: Bool = false,
        failBuild!: Bool = false,
        failRegister!: Bool = false
    ) {
        this.blockBuild = blockBuild
        this.failBuild = failBuild
        transport = LifecycleServerTransport(
            blockGraceful: blockGraceful,
            failRegister: failRegister
        )
    }

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
                    if (remaining <= Duration.Zero) {
                        break
                    }
                    changed.wait(timeout: remaining)
                }
            }
        }
        if (failBuild) {
            throw Exception("lifecycle build failure")
        }
        transport
    }

    func awaitBuild(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!buildEntered) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
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

private func lifecycleEndpoint(path: String): SseEndpoint {
    SseEndpoint(
        path,
        config: SseEndpointConfig(
            connection: SseConnectionConfig(transportAbortCapability: SseTransportAbortCapability.Immediate),
            initialComment: None<String>
        )
    )
}

private func awaitLifecycleEndpointClosing(
    endpoint: SseEndpoint,
    timeoutMillis!: Int64 = 1000
): Bool {
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

private func attachBlockedLifecycleConnection(
    endpoint: SseEndpoint,
    id: String
): (SseConnection, LifecycleBlockingWriter) {
    let connection = SseConnection(
        id,
        config: SseConnectionConfig(transportAbortCapability: SseTransportAbortCapability.ServerWideOnly)
    )
    endpoint.attachConnection(connection, "")
    connection.send(SseFrame(data: Some("blocked")))
    let writer = LifecycleBlockingWriter()
    connection.start(writer)
    @Assert(writer.awaitEntered())
    (connection, writer)
}

private func lifecycleAwaitBoundFailed(server: SseServer): Bool {
    try {
        server.awaitBound(timeoutMillis: 100)
        false
    } catch (error: SseException) {
        match (error.kind) {
            case SseErrorKind.TransportFailure => true
            case _ => false
        }
    }
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
func serverBuildFailureWakesAwaitBound() {
    let factory = LifecycleServerTransportFactory(failBuild: true)
    let server = SseServer()
    server.transportFactory = factory
    server.start()
    @Assert(lifecycleAwaitBoundFailed(server))
}

@Test
func serverRegisterFailureClosesBuiltTransportAndWakesAwaitBound() {
    let factory = LifecycleServerTransportFactory(failRegister: true)
    let server = SseServer()
    server.transportFactory = factory
    server.register(lifecycleEndpoint("/lifecycle-register-failure"))
    server.start()
    @Assert(lifecycleAwaitBoundFailed(server))
    @Assert(factory.transport.closeCount, 1)
}

@Test
func serverGracefulAndForceTransportCloseAreIndependentAndIdempotent() {
    let factory = LifecycleServerTransportFactory()
    let server = SseServer()
    server.transportFactory = factory
    server.start()
    @Assert(factory.transport.awaitServe())

    server.closeGracefully(1000)
    @Assert(factory.transport.gracefulCount, 1)
    @Assert(factory.transport.closeCount, 0)
    server.closeGracefully(1000)
    @Assert(factory.transport.gracefulCount, 1)

    server.closeNow()
    server.closeNow()
    @Assert(factory.transport.closeCount, 1)
}

@Test
func serverForceCloseInterruptsPendingGracefulTransportShutdown() {
    let factory = LifecycleServerTransportFactory(blockGraceful: true)
    let server = SseServer()
    server.transportFactory = factory
    server.start()
    @Assert(factory.transport.awaitServe())

    let graceful: Future<Unit> = spawn {=> server.closeGracefully(5000)}
    @Assert(factory.transport.awaitGraceful())
    @Assert(graceful.tryGet().isNone())

    server.closeNow()
    graceful.get(Duration.second)
    @Assert(factory.transport.gracefulCount, 1)
    @Assert(factory.transport.closeCount, 1)
}

@Test
func serverGracefulCloseClosesEveryAdmissionGateBeforeDrain() {
    let first = lifecycleEndpoint("/lifecycle-first")
    let second = lifecycleEndpoint("/lifecycle-second")
    let (firstConnection, firstWriter) = attachBlockedLifecycleConnection(
        first,
        "lifecycle-first-blocked"
    )
    let (secondConnection, secondWriter) = attachBlockedLifecycleConnection(
        second,
        "lifecycle-second-blocked"
    )
    let server = SseServer()
    server.register(first)
    server.register(second)

    let graceful: Future<Unit> = spawn {=> server.closeGracefully(5_000)}
    @Assert(awaitLifecycleEndpointClosing(first))
    @Assert(awaitLifecycleEndpointClosing(second))
    @Assert(graceful.tryGet().isNone())

    let request = SseServerRequest("GET", "", {_: String => None<String>})
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
func serverCloseNowUpgradesConcurrentGracefulEndpointDrain() {
    let endpoint = lifecycleEndpoint("/lifecycle-upgrade")
    let (connection, writer) = attachBlockedLifecycleConnection(
        endpoint,
        "lifecycle-upgrade-blocked"
    )
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

    server.closeNow()
    server.closeGracefully(1_000)

    writer.release()
    @Assert(connection.awaitClosed(1000))
}

@Test
func serverRejectsRegistrationAfterStartBoundary() {
    let factory = LifecycleServerTransportFactory()
    let server = SseServer()
    server.transportFactory = factory
    server.register(lifecycleEndpoint("/lifecycle-started"))
    server.start()
    @Assert(factory.transport.awaitServe())

    let registerRejected: Future<Bool> = spawn {
        => try {
            server.register(lifecycleEndpoint("/lifecycle-too-late"))
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
