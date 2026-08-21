package sse4cj

import std.io.InputStream
import std.net.*
import std.sync.*
import stdx.net.http.*

private class StdxConnectionState {
    private let mutex = Mutex()
    private var client: Option<Client> = None<Client>
    private var connectingSocket: Option<TcpSocket> = None<TcpSocket>
    private var cancelled: Bool = false

    func attachClient(value: Client): Unit {
        var closeNow = false
        synchronized(mutex) {
            client = Some(value)
            closeNow = cancelled
        }
        if (closeNow) {
            value.close()
        }
    }

    func beginConnect(socket: TcpSocket): Unit {
        var closeNow = false
        synchronized(mutex) {
            connectingSocket = Some(socket)
            closeNow = cancelled
        }
        if (closeNow) {
            socket.close()
        }
    }

    func endConnect(): Unit {
        synchronized(mutex) {
            connectingSocket = None<TcpSocket>
        }
    }

    func cancel(): Unit {
        var socket: Option<TcpSocket> = None<TcpSocket>
        var activeClient: Option<Client> = None<Client>
        synchronized(mutex) {
            cancelled = true
            socket = connectingSocket
            activeClient = client
        }
        match (socket) {
            case Some(value) => value.close()
            case None => ()
        }
        match (activeClient) {
            case Some(value) => value.close()
            case None => ()
        }
    }
}

private class StdxSseTransportResponse <: SseTransportResponse {
    private let response: HttpResponse
    private let client: Client
    private let cancellation: Option<SseCancellationToken>
    private let interruptId: Int64
    private let responseOrigin: Option<String>
    private let closed = AtomicBool(false)

    init(
        response: HttpResponse,
        client: Client,
        cancellation: Option<SseCancellationToken>,
        interruptId: Int64,
        origin: Option<String>
    ) {
        this.response = response
        this.client = client
        this.cancellation = cancellation
        this.interruptId = interruptId
        this.responseOrigin = origin
    }

    public func status(): UInt16 {
        response.status
    }

    public func header(name: String): Option<String> {
        response.headers.getFirst(name)
    }

    public func body(): InputStream {
        response.body
    }
    public func origin(): Option<String> {
        responseOrigin
    }

    public func close(): Unit {
        if (closed.swap(true)) {
            return
        }
        try {
            response.close()
        } finally {
            try {
                client.close()
            } finally {
                match (cancellation) {
                    case Some(token) => token.clearInterrupt(interruptId)
                    case None => ()
                }
            }
        }
    }
}

public class StdxSseClientTransport <: SseClientTransport {
    public init() {}

    public func send(
        request: SseHttpRequest,
        cancellation: Option<SseCancellationToken>
    ): SseTransportResponse {
        if (isTransportCancelled(cancellation)) {
            throw SseException(SseErrorKind.Cancelled, "SSE HTTP request cancelled")
        }

        let state = StdxConnectionState()
        let connectTimeout = Duration.millisecond * request.connectTimeoutMillis
        let readTimeout = Duration.millisecond * request.readTimeoutMillis
        let client = ClientBuilder().autoRedirect(false).readTimeout(readTimeout).connector {
            address =>
                let socket = TcpSocket(address)
                state.beginConnect(socket)
                try {
                    socket.connect(timeout: Some(connectTimeout))
                    socket
                } finally {
                    state.endConnect()
                }
        }.build()
        state.attachClient(client)

        let interruptId = match (cancellation) {
            case Some(token) => token.registerInterrupt({=> state.cancel()})
            case None => 0
        }

        if (isTransportCancelled(cancellation)) {
            state.cancel()
            clearTransportInterrupt(cancellation, interruptId)
            throw SseException(SseErrorKind.Cancelled, "SSE HTTP request cancelled")
        }

        try {
            var builder = HttpRequestBuilder().get().url(request.url).readTimeout(readTimeout)
            for (header in request.headers()) {
                builder = builder.header(header.name, header.value)
            }
            builder = builder.header("Accept", "text/event-stream")
            let response = client.send(builder.build())
            if (isTransportCancelled(cancellation)) {
                response.close()
                client.close()
                clearTransportInterrupt(cancellation, interruptId)
                throw SseException(SseErrorKind.Cancelled, "SSE HTTP request cancelled")
            }
            return StdxSseTransportResponse(
                response,
                client,
                cancellation,
                interruptId,
                originFromHttpUrl(request.url)
            )
        } catch (e: SseException) {
            client.close()
            clearTransportInterrupt(cancellation, interruptId)
            throw e
        } catch (e: Exception) {
            client.close()
            clearTransportInterrupt(cancellation, interruptId)
            if (isTransportCancelled(cancellation)) {
                throw SseException(SseErrorKind.Cancelled, "SSE HTTP request cancelled")
            }
            throw SseException(SseErrorKind.TransportFailure, "SSE HTTP request failed: ${e.message}")
        }
    }
}

private func isTransportCancelled(cancellation: Option<SseCancellationToken>): Bool {
    match (cancellation) {
        case Some(token) => token.isCancelled
        case None => false
    }
}

private func clearTransportInterrupt(cancellation: Option<SseCancellationToken>, interruptId: Int64): Unit {
    match (cancellation) {
        case Some(token) => token.clearInterrupt(interruptId)
        case None => ()
    }
}
