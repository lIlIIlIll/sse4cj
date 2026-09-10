package sse4cj

import std.collection.*
import std.sync.*
import std.time.*
import wirestack.http.*

class WirestackSseServerTransportFactory <: SseServerTransportFactory {
    private let tls: Option<HttpServerTlsConfig>

    init(tls!: Option<HttpServerTlsConfig> = None<HttpServerTlsConfig>) {
        this.tls = tls
    }

    public func build(config: SseServerConfig, afterBind: () -> Unit): SseServerTransport {
        let router = WirestackSseServerRouter()
        let builder = HttpServer
            .builder()
            .listen(config.listenEndpoint)
            .handler(router)
            .connectionLimit(config.maxConnections)
        match (tls) {
            case Some(value) => builder.tls(value)
            case None => ()
        }
        WirestackSseServerTransport(builder.build(), router, afterBind)
    }
}

private class WirestackSseServerTransport <: SseServerTransport {
    private let server: HttpServer
    private let router: WirestackSseServerRouter
    private let afterBind: () -> Unit
    private let gracefulClosing = AtomicBool(false)
    private let forceClosing = AtomicBool(false)

    init(server: HttpServer, router: WirestackSseServerRouter, afterBind: () -> Unit) {
        this.server = server
        this.router = router
        this.afterBind = afterBind
    }

    public prop port: UInt16 {
        get() {
            server.localEndpoint.port
        }
    }

    public func register(
        path: String,
        handler: (SseServerRequest, () -> Unit) -> SseServerTransportResponse
    ): Unit {
        router.register(path, handler)
    }

    public func serve(): Unit {
        router.beginServing()
        afterBind()
        server.serve()
    }

    public func closeGracefully(deadline: MonoTime): Unit {
        if (gracefulClosing.swap(true)) {
            return
        }
        router.beginClosing()
        try {
            let _ = server.shutdown(context: OperationContext(deadline: Some(Deadline(deadline))))
        } finally {
            router.closeActiveBodies()
        }
    }

    public func close(): Unit {
        if (forceClosing.swap(true)) {
            return
        }
        router.beginClosing()
        try {
            server.close()
        } finally {
            router.closeActiveBodies()
        }
    }
}

private class WirestackSseServerRouter <: HttpServerHandler {
    private let mutex = Mutex()
    private let routes = HashMap<String, (SseServerRequest, () -> Unit) -> SseServerTransportResponse>()
    private let activeBodies = HashMap<String, ResponseBody>()
    private var serving = false
    private var closing = false

    func register(
        path: String,
        handler: (SseServerRequest, () -> Unit) -> SseServerTransportResponse
    ): Unit {
        synchronized(mutex) {
            if (serving || closing) {
                throw SseException(SseErrorKind.InvalidConfiguration, "SSE routes cannot change after serving starts")
            }
            if (routes.contains(path)) {
                throw SseException(SseErrorKind.InvalidConfiguration, "duplicate SSE transport route")
            }
            routes[path] = handler
        }
    }

    func beginServing(): Unit {
        synchronized(mutex) {
            if (closing) {
                throw SseException(SseErrorKind.Closed, "SSE server transport is closing")
            }
            serving = true
        }
    }

    func beginClosing(): Unit {
        synchronized(mutex) {
            closing = true
        }
    }

    public func handle(request: HttpServerRequest, context: OperationContext): HttpServerResponse {
        let target = request.target
        if (target.isEmpty() || target[0] != b'/') {
            return emptyResponse(400, version: request.version)
        }
        let path = originFormPath(target)
        let route = synchronized(mutex) {
            if (closing || !routes.contains(path)) {
                None<(SseServerRequest, () -> Unit) -> SseServerTransportResponse>
            } else {
                Some(routes[path])
            }
        }
        let handler = match (route) {
            case Some(value) => value
            case None =>
                if (synchronized(mutex) {
                    closing
                }) {
                    return emptyResponse(503, version: request.version)
                }
                return emptyResponse(404, version: request.version)
        }
        if (request.method != HttpMethod("GET")) {
            return emptyResponse(405, version: request.version)
        }

        let lastEventId = match (request.headers.getFirst("Last-Event-ID")) {
            case Some(value) => value
            case None => ""
        }
        let providerNeutralRequest = SseServerRequest(
            request.method.value,
            lastEventId,
            {name => request.headers.getFirst(name)}
        )
        let abort = {=> let _ = request.requestCancellation.cancel()}
        let result = handler(providerNeutralRequest, abort)
        match (result.body) {
            case Some(stream) => streamingResponse(request.version, result, stream, context)
            case None => emptyResponse(result.status, version: request.version, headers: result.headers)
        }
    }

    private func streamingResponse(
        version: HttpVersion,
        value: SseServerTransportResponse,
        stream: SseConnectionStream,
        context: OperationContext
    ): HttpServerResponse {
        var acquiredRegistration = None<CancellationRegistration>
        var acquiredBody = None<ResponseBody>
        try {
            let registration = context.cancellation.register {=> stream.cancel()}
            acquiredRegistration = Some(registration)
            let streamBody = WirestackSseHttpBody(
                stream,
                registration,
                {id => removeActiveBody(id)}
            )
            let body = ResponseBody(streamBody, completeAtDeclaredLength: false)
            acquiredBody = Some(body)
            let published = synchronized(mutex) {
                if (closing || stream.isClosed() || activeBodies.contains(stream.id)) {
                    false
                } else {
                    activeBodies[stream.id] = body
                    true
                }
            }
            if (!published) {
                try {
                    body.close()
                } catch (_: Exception) {}
                return emptyResponse(503, version: version)
            }

            let headers = responseHeaders(value.headers, version, streaming: true)
            HttpServerResponse(
                HttpResponse(
                    Int64(value.status),
                    "",
                    body,
                    version: version,
                    headers: headers
                )
            )
        } catch (error: Exception) {
            match (acquiredBody) {
                case Some(body) => try {
                    body.close()
                } catch (_: Exception) {}
                case None =>
                    match (acquiredRegistration) {
                        case Some(registration) => registration.unregister()
                        case None => ()
                    }
                    try {
                        stream.close()
                    } catch (_: Exception) {}
            }
            throw error
        }
    }

    private func emptyResponse(
        status: UInt16,
        version!: HttpVersion = HttpVersion.Http11,
        headers!: Array<SseHeader> = Array<SseHeader>()
    ): HttpServerResponse {
        HttpServerResponse(
            HttpResponse(
                Int64(status),
                "",
                ResponseBody.empty(),
                version: version,
                headers: responseHeaders(headers, version, streaming: false)
            )
        )
    }

    private func responseHeaders(
        headers: Array<SseHeader>,
        version: HttpVersion,
        streaming!: Bool
    ): HttpHeaders {
        let builder = HttpHeaders.builder()
        for (header in headers) {
            builder.add(header.name, header.value)
        }
        if (streaming && version == HttpVersion.Http11) {
            builder.add("Transfer-Encoding", "chunked")
            builder.add("Connection", "close")
        }
        builder.build()
    }

    private func removeActiveBody(id: String): Unit {
        synchronized(mutex) {
            activeBodies.remove(id)
        }
    }

    func closeActiveBodies(): Unit {
        let snapshot = synchronized(mutex) {
            let values = ArrayList<ResponseBody>()
            for (body in activeBodies.values()) {
                values.add(body)
            }
            values.toArray()
        }
        for (body in snapshot) {
            try {
                body.close()
            } catch (_: Exception) {}
        }
    }
}

private class WirestackSseHttpBody <: HttpBodyStream {
    private let stream: SseConnectionStream
    private let cancellation: CancellationRegistration
    private let released: (String) -> Unit
    private let closing = AtomicBool(false)

    init(
        stream: SseConnectionStream,
        cancellation: CancellationRegistration,
        released: (String) -> Unit
    ) {
        this.stream = stream
        this.cancellation = cancellation
        this.released = released
    }

    public prop contentLength: Option<Int64> {
        get() {
            None<Int64>
        }
    }

    public func read(destination: Array<Byte>): Int64 {
        stream.read(destination)
    }

    public func close(): Unit {
        if (closing.swap(true)) {
            return
        }
        try {
            stream.close()
        } finally {
            cancellation.unregister()
            released(stream.id)
        }
    }

    public func isClosed(): Bool {
        closing.load() || stream.isClosed()
    }
}

private func originFormPath(target: String): String {
    var end = target.size
    for (index in 0..target.size) {
        if (target[index] == b'?') {
            end = index
            break
        }
    }
    target[..end]
}
