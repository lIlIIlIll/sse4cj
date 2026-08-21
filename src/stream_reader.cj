package sse4cj

import std.io.*
import std.sync.*

public class SseCancellationToken {
    private let cancelled = AtomicBool(false)
    private let interruptMutex = Mutex()
    private var interruptAction: Option<() -> Unit> = None<() -> Unit>
    private var interruptId: Int64 = 0
    private var nextInterruptId: Int64 = 1

    public prop isCancelled: Bool {
        get() {
            cancelled.load()
        }
    }

    public func cancel(): Unit {
        if (!cancelled.swap(true)) {
            let action = synchronized(interruptMutex) {
                interruptAction
            }
            match (action) {
                case Some(value) => value()
                case None => ()
            }
        }
    }

    func registerInterrupt(action: () -> Unit): Int64 {
        var invokeImmediately = false
        let id = synchronized(interruptMutex) {
            if (interruptAction.isSome()) {
                throw SseException(
                    SseErrorKind.InvalidConfiguration,
                    "A cancellation token cannot drive multiple active SSE transports"
                )
            }
            if (cancelled.load()) {
                invokeImmediately = true
                0
            } else {
                let current = nextInterruptId
                nextInterruptId++
                interruptId = current
                interruptAction = Some(action)
                current
            }
        }
        if (invokeImmediately) {
            action()
        }
        id
    }

    func clearInterrupt(id: Int64): Unit {
        if (id == 0) {
            return
        }
        synchronized(interruptMutex) {
            if (interruptId == id) {
                interruptAction = None<() -> Unit>
                interruptId = 0
            }
        }
    }
}

public class SseStreamReaderConfig {
    public let readBufferSize: Int64
    public let decoderLimits: SseDecoderLimits

    public static let DEFAULTS: SseStreamReaderConfig = SseStreamReaderConfig(
        readBufferSize: 16 * 1024,
        decoderLimits: SseDecoderLimits.DEFAULTS
    )

    public init(readBufferSize!: Int64, decoderLimits!: SseDecoderLimits = SseDecoderLimits.DEFAULTS) {
        if (readBufferSize <= 0 || readBufferSize > 16 * 1024 * 1024) {
            throw SseException(
                SseErrorKind.InvalidConfiguration,
                "readBufferSize must be in 1..16777216 bytes"
            )
        }
        this.readBufferSize = readBufferSize
        this.decoderLimits = decoderLimits
    }
}

public class SseStreamReader {
    private let config: SseStreamReaderConfig

    public init(config!: SseStreamReaderConfig = SseStreamReaderConfig.DEFAULTS) {
        this.config = config
    }

    public func read(
        input: InputStream,
        sink: SseDecodeSink,
        cancellation!: Option<SseCancellationToken> = None<SseCancellationToken>
    ): Unit {
        let decoder = SseDecoder(limits: config.decoderLimits)
        let buffer = Array<Byte>(config.readBufferSize, repeat: 0)
        while (true) {
            checkCancellation(cancellation)
            let count = try {
                input.read(buffer)
            } catch (e: Exception) {
                if (isCancelled(cancellation)) {
                    throw SseException(SseErrorKind.Cancelled, "SSE stream read cancelled")
                }
                throw SseException(SseErrorKind.TransportFailure, "SSE stream read failed: ${e.message}")
            }
            checkCancellation(cancellation)
            if (count <= 0) {
                decoder.finish(sink)
                return
            }
            if (count > buffer.size) {
                throw SseException(
                    SseErrorKind.InternalInvariantViolation,
                    "InputStream returned more bytes than the provided buffer can hold"
                )
            }
            decoder.feed(buffer, 0, count, sink)
        }
    }

    private func checkCancellation(cancellation: Option<SseCancellationToken>): Unit {
        if (isCancelled(cancellation)) {
            throw SseException(SseErrorKind.Cancelled, "SSE stream read cancelled")
        }
    }

    private func isCancelled(cancellation: Option<SseCancellationToken>): Bool {
        match (cancellation) {
            case Some(token) => token.isCancelled
            case None => false
        }
    }
}

private class OriginatingDecodeSink <: SseDecodeSink {
    private let downstream: SseDecodeSink
    private let eventOrigin: Option<String>

    init(downstream: SseDecodeSink, origin: Option<String>) {
        this.downstream = downstream
        this.eventOrigin = origin
    }

    public func onEvent(event: SseEvent): Unit {
        downstream.onEvent(
            SseEvent(
                event.eventType,
                event.data,
                event.lastEventId,
                origin: eventOrigin
            )
        )
    }

    public func onLastEventIdChanged(id: String): Unit {
        downstream.onLastEventIdChanged(id)
    }

    public func onRetryChanged(retryMillis: Int64): Unit {
        downstream.onRetryChanged(retryMillis)
    }
}

public func readSseHttpResponse(
    reader: SseStreamReader,
    request: SseHttpRequest,
    transport: SseClientTransport,
    sink: SseDecodeSink,
    cancellation!: Option<SseCancellationToken> = None<SseCancellationToken>
): Unit {
    let response = transport.send(request, cancellation)
    try {
        if (response.status() != 200) {
            throw SseException(
                SseErrorKind.HttpResponseRejected,
                "Expected HTTP 200 for finite SSE response, got ${response.status()}"
            )
        }
        let contentType = response.header("Content-Type")
        let accepted = match (contentType) {
            case Some(value) => isEventStreamContentType(value)
            case None => false
        }
        if (!accepted) {
            throw SseException(
                SseErrorKind.HttpResponseRejected,
                "Expected Content-Type essence text/event-stream"
            )
        }
        reader.read(response.body(), OriginatingDecodeSink(sink, response.origin()), cancellation: cancellation)
    } finally {
        response.close()
    }
}
