package sse4cj

import std.collection.*
import std.sync.*
import std.time.*
import wirestack.http.*

public class SseServerRequest {
    public let method: String
    public let lastEventId: String
    private let headerLookup: (String) -> Option<String>

    init(method: String, lastEventId: String, headerLookup: (String) -> Option<String>) {
        this.method = method
        this.lastEventId = lastEventId
        this.headerLookup = headerLookup
    }

    public func header(name: String): Option<String> {
        headerLookup(name)
    }
}

public class SseAdmissionResult {
    public let accepted: Bool
    public let rejectStatus: UInt16

    private init(accepted: Bool, rejectStatus: UInt16) {
        this.accepted = accepted
        this.rejectStatus = rejectStatus
    }

    public static func accept(): SseAdmissionResult {
        SseAdmissionResult(true, 200)
    }

    public static func reject(status!: UInt16 = 403): SseAdmissionResult {
        if (status < 400 || status > 599) {
            throw SseException(SseErrorKind.InvalidConfiguration, "reject status must be in 400..599")
        }
        SseAdmissionResult(false, status)
    }
}

public abstract class SseAdmissionHandler {
    public func admit(request: SseServerRequest): SseAdmissionResult
}

public class SseEndpointConfig {
    public let connection: SseConnectionConfig
    public let gapPolicy: ReplayGapPolicy
    public let initialComment: Option<String>
    public let xAccelBufferingNo: Bool

    public init(
        connection!: SseConnectionConfig = SseConnectionConfig.DEFAULTS,
        gapPolicy!: ReplayGapPolicy = ReplayGapPolicy.StartLive,
        initialComment!: Option<String> = Some("open"),
        xAccelBufferingNo!: Bool = false
    ) {
        match (initialComment) {
            case Some(value) => SseEncoder.encode(SseFrame(comment: Some(value)))
            case None => ()
        }
        this.connection = connection
        this.gapPolicy = gapPolicy
        this.initialComment = initialComment
        this.xAccelBufferingNo = xAccelBufferingNo
    }
}

public class SseEndpoint {
    public let path: String
    public let hub: SseHub
    public let config: SseEndpointConfig
    private let admission: Option<SseAdmissionHandler>
    private let closing = AtomicBool(false)
    let closeMutex = Mutex()
    let closeChanged = synchronized(closeMutex) {
        closeMutex.condition()
    }

    public init(
        path: String,
        config!: SseEndpointConfig = SseEndpointConfig(),
        replay!: Option<ReplayProvider> = None<ReplayProvider>,
        admission!: Option<SseAdmissionHandler> = None<SseAdmissionHandler>
    ) {
        if (path.size == 0 || path[0] != b'/') {
            throw SseException(SseErrorKind.InvalidConfiguration, "endpoint path must start with /")
        }
        if (path.contains("?") || path.contains("#") || path.contains("\r") || path.contains("\n") || path.contains(
            "\0")) {
            throw SseException(SseErrorKind.InvalidConfiguration,
                "endpoint path must not contain query, fragment, CR, LF, or NUL")
        }
        this.path = path
        this.config = config
        this.hub = SseHub(replay: replay)
        this.admission = admission
    }

    public prop isClosing: Bool {
        get() {
            closing.load()
        }
    }

    func decide(request: SseServerRequest): SseAdmissionResult {
        if (closing.load()) {
            return SseAdmissionResult.reject(status: 503)
        }
        match (admission) {
            case Some(value) => value.admit(request)
            case None => SseAdmissionResult.accept()
        }
    }

    func attachConnection(connection: SseConnection, lastEventId: String): Unit {
        if (closing.load()) {
            throw SseException(SseErrorKind.Closed, "endpoint is closing")
        }
        hub.attach(connection, lastEventId: lastEventId, gapPolicy: config.gapPolicy)
        match (config.initialComment) {
            case Some(value) =>
                let result = connection.send(SseFrame(comment: Some(value)))
                match (result.status) {
                    case SseSendStatus.Accepted => ()
                    case _ =>
                        connection.closeNow()
                        throw SseException(SseErrorKind.SlowConsumer, "initial SSE comment could not be queued")
                }
            case None => ()
        }
    }

    public func closeNow(): Unit {
        beginClosing()
        hub.closeNow()
    }

    public func closeGracefully(timeoutMillis: Int64): Unit {
        if (timeoutMillis <= 0 || timeoutMillis > 24 * 60 * 60 * 1000) {
            throw SseException(SseErrorKind.InvalidConfiguration, "graceful close timeout must be finite and positive")
        }
        beginClosing()
        hub.drainGracefullyUntil(MonoTime.now() + Duration.millisecond * timeoutMillis)
    }

    func beginClosing(): Unit {
        synchronized(closeMutex) {
            closing.store(true)
            closeChanged.notifyAll()
        }
        hub.beginClosing()
    }

    func drainGracefullyUntil(deadline: MonoTime): Unit {
        hub.drainGracefullyUntil(deadline)
    }
}

public class SseServerConfig {
    public let listenEndpoint: SocketEndpoint
    public let maxConnections: Int64

    public init(
        listenEndpoint!: SocketEndpoint = SocketEndpoint(
            IpAddress(IpAddressFamily.Ipv4, [127u8, 0u8, 0u8, 1u8]),
            0
        ),
        maxConnections!: Int64 = 1024
    ) {
        if (maxConnections <= 0 || maxConnections > 65536) {
            throw SseException(SseErrorKind.InvalidConfiguration, "maxConnections must be in 1..65536")
        }
        this.listenEndpoint = listenEndpoint
        this.maxConnections = maxConnections
    }
}

class SseServerTransportResponse {
    let status: UInt16
    let headers: Array<SseHeader>
    let body: Option<SseConnectionStream>

    init(status: UInt16, headers: Array<SseHeader>, body: Option<SseConnectionStream>) {
        this.status = status
        this.headers = headers
        this.body = body
    }
}

abstract class SseServerTransport {
    public prop port: UInt16
    public func register(
        path: String,
        handler: (SseServerRequest, () -> Unit) -> SseServerTransportResponse
    ): Unit
    public func serve(): Unit
    public func closeGracefully(deadline: MonoTime): Unit
    public func close(): Unit
}

abstract class SseServerTransportFactory {
    public func build(config: SseServerConfig, afterBind: () -> Unit): SseServerTransport
}

public class SseServer {
    private let config: SseServerConfig
    private let mutex = Mutex()
    private let bindCondition = synchronized(mutex) {
        mutex.condition()
    }
    private let endpoints = HashMap<String, SseEndpoint>()
    private let started = AtomicBool(false)
    private let closing = AtomicBool(false)
    private let gracefulStarted = AtomicBool(false)
    private let forceClosing = AtomicBool(false)
    private let gracefulTransportClosing = AtomicBool(false)
    private let forceTransportClosing = AtomicBool(false)
    private let nextConnectionId = AtomicInt64(1)
    private let activeResponseCount = AtomicInt64(0)
    private var server: Option<SseServerTransport> = None<SseServerTransport>
    private var worker: Option<Future<Unit>> = None<Future<Unit>>
    private var bound = false
    private var serveError: Option<Exception> = None<Exception>
    var transportFactory: SseServerTransportFactory = WirestackSseServerTransportFactory()

    public init(config!: SseServerConfig = SseServerConfig()) {
        this.config = config
    }

    public func register(endpoint: SseEndpoint): Unit {
        if (started.load() || closing.load()) {
            throw SseException(SseErrorKind.InvalidConfiguration, "register endpoints before server start or shutdown")
        }
        match (endpoint.config.connection.transportAbortCapability) {
            case SseTransportAbortCapability.Immediate => ()
            case _ => throw SseException(
                SseErrorKind.InvalidConfiguration,
                "Wirestack SseServer requires transportAbortCapability=Immediate"
            )
        }
        synchronized(mutex) {
            if (started.load() || closing.load()) {
                throw SseException(SseErrorKind.InvalidConfiguration,
                    "register endpoints before server start or shutdown")
            }
            if (endpoints.contains(endpoint.path)) {
                throw SseException(SseErrorKind.InvalidConfiguration, "duplicate SSE endpoint path")
            }
            endpoints[endpoint.path] = endpoint
        }
    }

    public prop boundPort: UInt16 {
        get() {
            let active = synchronized(mutex) {
                server
            }
            match (active) {
                case Some(value) => value.port
                case None => config.listenEndpoint.port
            }
        }
    }

    public func start(): Unit {
        let snapshot = synchronized(mutex) {
            if (closing.load()) {
                throw SseException(SseErrorKind.InvalidConfiguration, "server is closing")
            }
            if (!started.compareAndSwap(false, true)) {
                throw SseException(SseErrorKind.InvalidConfiguration, "server already started")
            }
            let values = ArrayList<SseEndpoint>()
            for (endpoint in endpoints.values()) {
                values.add(endpoint)
            }
            values.toArray()
        }

        var built = None<SseServerTransport>
        try {
            let factory = synchronized(mutex) {
                transportFactory
            }
            let value = factory.build(config, {
                => synchronized(mutex) {
                    bound = true
                    bindCondition.notifyAll()
                }
            })
            built = Some(value)
            for (endpoint in snapshot) {
                value.register(endpoint.path, {request, abort => handleRequest(endpoint, request, abort)})
            }
            let published = synchronized(mutex) {
                if (closing.load()) {
                    false
                } else {
                    server = Some(value)
                    worker = Some(spawn {
                        => try {
                            value.serve()
                        } catch (_: Exception) {
                            if (!closing.load()) {
                                synchronized(mutex) {
                                    serveError = Some(
                                        SseException(SseErrorKind.TransportFailure, "SSE server failed to serve"))
                                    bindCondition.notifyAll()
                                }
                            }
                        }
                    })
                    true
                }
            }
            if (!published) {
                value.close()
            }
        } catch (_: Exception) {
            match (built) {
                case Some(value) => try {
                    value.close()
                } catch (_: Exception) {}
                case None => ()
            }
            synchronized(mutex) {
                serveError = Some(SseException(SseErrorKind.TransportFailure, "SSE server failed to start"))
                bindCondition.notifyAll()
            }
        }
    }

    public func awaitBound(timeoutMillis!: Int64 = 5_000): Unit {
        if (timeoutMillis <= 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "bind timeout must be positive")
        }
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        let error = synchronized(mutex) {
            while (!bound && serveError.isNone()) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                bindCondition.wait(timeout: remaining)
            }
            if (!bound && serveError.isNone()) {
                Some<Exception>(SseException(SseErrorKind.TransportFailure, "SSE server bind timed out"))
            } else {
                serveError
            }
        }
        match (error) {
            case Some(value) => throw value
            case None => ()
        }
    }

    public func closeNow(): Unit {
        closing.store(true)
        if (forceClosing.swap(true)) {
            return
        }
        let snapshot = snapshotEndpoints()
        for (endpoint in snapshot) {
            endpoint.beginClosing()
        }
        for (endpoint in snapshot) {
            endpoint.closeNow()
        }
        forceCloseServerTransport()
    }

    public func closeGracefully(timeoutMillis: Int64): Unit {
        if (timeoutMillis <= 0 || timeoutMillis > 24 * 60 * 60 * 1000) {
            throw SseException(SseErrorKind.InvalidConfiguration, "shutdown timeout must be finite and positive")
        }
        if (forceClosing.load() || !gracefulStarted.compareAndSwap(false, true)) {
            return
        }
        closing.store(true)
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        let snapshot = snapshotEndpoints()
        for (endpoint in snapshot) {
            endpoint.beginClosing()
        }
        for (endpoint in snapshot) {
            endpoint.drainGracefullyUntil(deadline)
        }
        gracefulCloseServerTransport(deadline)
    }

    private func handleRequest(
        endpoint: SseEndpoint,
        request: SseServerRequest,
        abort: () -> Unit
    ): SseServerTransportResponse {
        let decision = try {
            endpoint.decide(request)
        } catch (_: Exception) {
            return emptyResponse(500)
        }
        if (!decision.accepted) {
            return emptyResponse(decision.rejectStatus)
        }
        if (closing.load() || endpoint.isClosing) {
            return emptyResponse(503)
        }

        if (activeResponseCount.fetchAdd(1) >= config.maxConnections) {
            activeResponseCount.fetchSub(1)
            return emptyResponse(503)
        }
        if (closing.load() || endpoint.isClosing) {
            activeResponseCount.fetchSub(1)
            return emptyResponse(503)
        }

        let connection = SseConnection(
            "sse-" + nextConnectionId.fetchAdd(1).toString(),
            config: endpoint.config.connection
        )
        var stream = None<SseConnectionStream>
        try {
            let opened = connection.openStream(abort, {=> let _ = activeResponseCount.fetchSub(1)})
            stream = Some(opened)
            if (closing.load() || endpoint.isClosing) {
                opened.close()
                return emptyResponse(503)
            }
            endpoint.attachConnection(connection, request.lastEventId)
            let headers = ArrayList<SseHeader>()
            headers.add(SseHeader("Content-Type", "text/event-stream; charset=utf-8"))
            headers.add(SseHeader("Cache-Control", "no-cache, no-transform"))
            if (endpoint.config.xAccelBufferingNo) {
                headers.add(SseHeader("X-Accel-Buffering", "no"))
            }
            SseServerTransportResponse(200, headers.toArray(), Some(opened))
        } catch (_: Exception) {
            match (stream) {
                case Some(value) => try {
                    value.close()
                } catch (_: Exception) {}
                case None => activeResponseCount.fetchSub(1)
            }
            if (closing.load() || endpoint.isClosing) {
                emptyResponse(503)
            } else {
                emptyResponse(500)
            }
        }
    }

    private func emptyResponse(status: UInt16): SseServerTransportResponse {
        SseServerTransportResponse(status, Array<SseHeader>(), None<SseConnectionStream>)
    }

    private func snapshotEndpoints(): Array<SseEndpoint> {
        synchronized(mutex) {
            let result = ArrayList<SseEndpoint>()
            for (endpoint in endpoints.values()) {
                result.add(endpoint)
            }
            result.toArray()
        }
    }

    private func awaitServerWorker(): Unit {
        match (synchronized(mutex) {
            worker
        }) {
            case Some(value) => try {
                value.get()
            } catch (_: Exception) {}
            case None => ()
        }
    }

    private func gracefulCloseServerTransport(deadline: MonoTime): Unit {
        try {
            if (!gracefulTransportClosing.swap(true)) {
                match (synchronized(mutex) {
                    server
                }) {
                    case Some(value) => value.closeGracefully(deadline)
                    case None => ()
                }
            }
        } finally {
            awaitServerWorker()
        }
    }

    private func forceCloseServerTransport(): Unit {
        try {
            if (!forceTransportClosing.swap(true)) {
                match (synchronized(mutex) {
                    server
                }) {
                    case Some(value) => value.close()
                    case None => ()
                }
            }
        } finally {
            awaitServerWorker()
        }
    }
}
