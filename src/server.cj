package sse4cj

import std.collection.*
import std.sync.*
import std.time.*
import stdx.net.http.*

public class SseServerRequest {
    public let method: String
    public let remoteAddress: String
    public let lastEventId: String
    private let headerLookup: (String) -> Option<String>

    init(method: String, remoteAddress: String, lastEventId: String, headerLookup: (String) -> Option<String>) {
        this.method = method
        this.remoteAddress = remoteAddress
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
    let closeChanged = synchronized(closeMutex) { closeMutex.condition() }

    public init(
        path: String,
        config!: SseEndpointConfig = SseEndpointConfig(),
        replay!: Option<ReplayProvider> = None<ReplayProvider>,
        admission!: Option<SseAdmissionHandler> = None<SseAdmissionHandler>
    ) {
        if (path.size == 0 || path[0] != b'/') {
            throw SseException(SseErrorKind.InvalidConfiguration, "endpoint path must start with /")
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

    func drainGracefullyUntil(deadline: MonoTime): Unit { hub.drainGracefullyUntil(deadline) }
}

private class StdxSseConnectionWriter <: SseConnectionWriter {
    private let responseWriter: HttpResponseWriter

    init(ctx: HttpContext) {
        responseWriter = HttpResponseWriter(ctx)
    }

    public func write(frame: EncodedSseFrame): Unit {
        // stdx HttpResponseWriter.write is synchronous and non-interruptible.
        // This call blocks until the socket write completes or throws.
        responseWriter.write(frame.sharedBytes())
    }

    public func flush(): Unit {}
    public func close(): Unit {}
}

public class SseServerConfig {
    public let address: String
    public let port: UInt16
    public let readTimeoutMillis: Int64
    // Retained for source compatibility. It is intentionally not forwarded to
    // stdx ServerBuilder.writeTimeout for streaming SSE responses; see
    // doc/transport-close-contract.md.
    public let writeTimeoutMillis: Int64
    public let readHeaderTimeoutMillis: Int64
    public let maxConnections: Int64

    public init(
        address!: String = "127.0.0.1",
        port!: UInt16 = 0,
        readTimeoutMillis!: Int64 = 120_000,
        writeTimeoutMillis!: Int64 = 30_000,
        readHeaderTimeoutMillis!: Int64 = 10_000,
        maxConnections!: Int64 = 1024
    ) {
        if (address.size == 0 || readTimeoutMillis <= 0 || writeTimeoutMillis <= 0 || readHeaderTimeoutMillis <= 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "server address/timeouts must be finite and valid")
        }
        if (maxConnections <= 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "maxConnections must be positive")
        }
        this.address = address
        this.port = port
        this.readTimeoutMillis = readTimeoutMillis
        this.writeTimeoutMillis = writeTimeoutMillis
        this.readHeaderTimeoutMillis = readHeaderTimeoutMillis
        this.maxConnections = maxConnections
    }
}

abstract class SseServerTransport {
    public prop port: UInt16
    public func register(path: String, handler: (HttpContext) -> Unit): Unit
    public func serve(): Unit
    public func close(): Unit
}

abstract class SseServerTransportFactory {
    public func build(config: SseServerConfig, afterBind: () -> Unit): SseServerTransport
}

private class StdxSseServerTransport <: SseServerTransport {
    private let server: Server

    init(server: Server) { this.server = server }

    public prop port: UInt16 { get() { server.port } }
    public func register(path: String, handler: (HttpContext) -> Unit): Unit {
        server.distributor.register(path, handler)
    }
    public func serve(): Unit { server.serve() }
    public func close(): Unit { server.close() }
}

private class StdxSseServerTransportFactory <: SseServerTransportFactory {
    public func build(config: SseServerConfig, afterBind: () -> Unit): SseServerTransport {
        // Do not enable stdx ServerBuilder.writeTimeout for SSE responses.
        // Current stdx HTTP/1.1 treats it as a response-lifetime timer.
        StdxSseServerTransport(ServerBuilder().addr(config.address).port(config.port)
            .readTimeout(Duration.millisecond * config.readTimeoutMillis)
            .readHeaderTimeout(Duration.millisecond * config.readHeaderTimeoutMillis)
            .afterBind(afterBind).build())
    }
}

public class SseServer {
    private let config: SseServerConfig
    private let mutex = Mutex()
    private let bindCondition = synchronized(mutex) { mutex.condition() }
    private let endpoints = HashMap<String, SseEndpoint>()
    private let started = AtomicBool(false)
    private let closing = AtomicBool(false)
    private let gracefulStarted = AtomicBool(false)
    private let forceClosing = AtomicBool(false)
    private let transportClosing = AtomicBool(false)
    private let nextConnectionId = AtomicInt64(1)
    private let activeConnectionCount = AtomicInt64(0)
    private var server: Option<SseServerTransport> = None<SseServerTransport>
    private var worker: Option<Future<Unit>> = None<Future<Unit>>
    private var bound = false
    private var serveError: Option<Exception> = None<Exception>
    var transportFactory: SseServerTransportFactory = StdxSseServerTransportFactory()

    public init(config!: SseServerConfig = SseServerConfig()) { this.config = config }

    public func register(endpoint: SseEndpoint): Unit {
        if (started.load() || closing.load()) {
            throw SseException(SseErrorKind.InvalidConfiguration, "register endpoints before server start or shutdown")
        }
        match (endpoint.config.connection.transportAbortCapability) {
            case SseTransportAbortCapability.ServerWideOnly => ()
            case _ => throw SseException(
                SseErrorKind.InvalidConfiguration,
                "stdx SseServer requires transportAbortCapability=ServerWideOnly"
            )
        }
        synchronized(mutex) {
            if (started.load() || closing.load()) {
                throw SseException(SseErrorKind.InvalidConfiguration, "register endpoints before server start or shutdown")
            }
            if (endpoints.contains(endpoint.path)) { throw SseException(SseErrorKind.InvalidConfiguration, "duplicate SSE endpoint path") }
            endpoints[endpoint.path] = endpoint
        }
    }

    public prop boundPort: UInt16 {
        get() {
            let active = synchronized(mutex) { server }
            match (active) { case Some(value) => value.port; case None => config.port }
        }
    }

    public func start(): Unit {
        let snapshot = synchronized(mutex) {
            if (closing.load()) { throw SseException(SseErrorKind.InvalidConfiguration, "server is closing") }
            if (!started.compareAndSwap(false, true)) { throw SseException(SseErrorKind.InvalidConfiguration, "server already started") }
            let values = ArrayList<SseEndpoint>()
            for (endpoint in endpoints.values()) { values.add(endpoint) }
            values.toArray()
        }
        let factory = synchronized(mutex) { transportFactory }
        let built = factory.build(config, { => synchronized(mutex) {
            bound = true
            bindCondition.notifyAll()
        } })
        for (endpoint in snapshot) { built.register(endpoint.path, {ctx => handleRequest(endpoint, ctx)}) }
        let published = synchronized(mutex) {
            if (closing.load()) { false }
            else {
                server = Some(built)
                worker = Some(spawn { => try { built.serve() } catch (error: Exception) {
                    if (!closing.load()) { synchronized(mutex) { serveError = Some(error); bindCondition.notifyAll() } }
                } })
                true
            }
        }
        if (!published) { built.close() }
    }

    public func awaitBound(timeoutMillis!: Int64 = 5_000): Unit {
        if (timeoutMillis <= 0) { throw SseException(SseErrorKind.InvalidConfiguration, "bind timeout must be positive") }
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        let error = synchronized(mutex) {
            while (!bound && serveError.isNone()) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                bindCondition.wait(timeout: remaining)
            }
            if (!bound && serveError.isNone()) { Some<Exception>(SseException(SseErrorKind.TransportFailure, "SSE server bind timed out")) } else { serveError }
        }
        match (error) { case Some(value) => throw value; case None => () }
    }

    public func closeNow(): Unit {
        closing.store(true)
        if (forceClosing.swap(true)) { return }
        let snapshot = snapshotEndpoints()
        for (endpoint in snapshot) { endpoint.beginClosing() }
        for (endpoint in snapshot) { endpoint.closeNow() }
        closeServerTransport()
    }

    public func closeGracefully(timeoutMillis: Int64): Unit {
        if (timeoutMillis <= 0 || timeoutMillis > 24 * 60 * 60 * 1000) {
            throw SseException(SseErrorKind.InvalidConfiguration, "shutdown timeout must be finite and positive")
        }
        if (forceClosing.load() || !gracefulStarted.compareAndSwap(false, true)) { return }
        closing.store(true)
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        let snapshot = snapshotEndpoints()
        for (endpoint in snapshot) { endpoint.beginClosing() }
        for (endpoint in snapshot) { endpoint.drainGracefullyUntil(deadline) }
        if (forceClosing.load()) { return }
        closeServerTransport()
    }

    private func handleRequest(endpoint: SseEndpoint, ctx: HttpContext): Unit {
        if (ctx.request.method != "GET") { ctx.responseBuilder.status(405).body(""); return }
        let headers = ctx.request.headers
        let lastId = match (headers.getFirst("Last-Event-ID")) { case Some(value) => value; case None => "" }
        let request = SseServerRequest(ctx.request.method, ctx.request.remoteAddr.toString(), lastId, {name => headers.getFirst(name)})
        let decision = try { endpoint.decide(request) } catch (_: Exception) { ctx.responseBuilder.status(500).body(""); return }
        if (!decision.accepted) { ctx.responseBuilder.status(decision.rejectStatus).body(""); return }
        // Enforce max connections to prevent coroutine accumulation under slow-consumer attacks.
        // stdx HttpResponseWriter.write is non-interruptible; without a connection limit,
        // blocked writer coroutines would accumulate unboundedly.
        if (activeConnectionCount.fetchAdd(1) >= config.maxConnections) {
            activeConnectionCount.fetchSub(1)
            ctx.responseBuilder.status(503).body("")
            return
        }
        ctx.responseBuilder.status(200).header("Content-Type", "text/event-stream; charset=utf-8")
            .header("Cache-Control", "no-cache, no-transform")
        // Current stdx has no public unknown-length/chunked streaming switch.
        // HttpResponseWriter works for HTTP/1.1 streaming only when chunked
        // transfer coding is declared. Keep this workaround inside the stdx
        // adapter and never emit it for HTTP/2, where Transfer-Encoding is invalid.
        match (ctx.request.version) {
            case Protocol.HTTP1_1 => ctx.responseBuilder.header("Transfer-Encoding", "chunked")
            case Protocol.HTTP2_0 => ()
            case _ => ()
        }
        if (endpoint.config.xAccelBufferingNo) { ctx.responseBuilder.header("X-Accel-Buffering", "no") }
        let responseWriter = StdxSseConnectionWriter(ctx)
        let connection = SseConnection("sse-" + nextConnectionId.fetchAdd(1).toString(), config: endpoint.config.connection)
        try {
            endpoint.attachConnection(connection, lastId)
            connection.start(responseWriter)
            connection.awaitClosed()
        } catch (error: Exception) {
            connection.closeNow()
            throw error
        } finally {
            activeConnectionCount.fetchSub(1)
        }
    }

    private func snapshotEndpoints(): Array<SseEndpoint> {
        synchronized(mutex) {
            let result = ArrayList<SseEndpoint>()
            for (endpoint in endpoints.values()) { result.add(endpoint) }
            result.toArray()
        }
    }

    private func awaitServerWorker(): Unit {
        match (synchronized(mutex) { worker }) {
            case Some(value) => try { value.get() } catch (_: Exception) {}
            case None => ()
        }
    }

    private func closeServerTransport(): Unit {
        if (!transportClosing.swap(true)) {
            match (synchronized(mutex) { server }) { case Some(value) => value.close(); case None => () }
        }
        awaitServerWorker()
    }
}
