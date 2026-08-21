package sse4cj

public enum SseErrorKind {
    InvalidUtf8
    | InvalidOutboundField
    | LineTooLarge
    | FieldTooLarge
    | EventTooLarge
    | DataTooLarge
    | BufferLimitExceeded
    | InvalidConfiguration
    | HttpResponseRejected
    | TransportFailure
    | Cancelled
    | Closed
    | ReconnectExhausted
    | SlowConsumer
    | ReplayGap
    | InternalInvariantViolation
}

public class SseException <: Exception {
    public let kind: SseErrorKind

    public init(kind: SseErrorKind, message: String) {
        super(message)
        this.kind = kind
    }
}

public class SseEvent {
    public let eventType: String
    public let data: String
    public let lastEventId: String
    public let origin: Option<String>

    public init(
        eventType: String,
        data: String,
        lastEventId: String,
        origin!: Option<String> = None<String>
    ) {
        this.eventType = eventType
        this.data = data
        this.lastEventId = lastEventId
        this.origin = origin
    }
}

public class SseFrame {
    public let eventType: Option<String>
    public let id: Option<String>
    public let retryMillis: Option<Int64>
    public let data: Option<String>
    public let comment: Option<String>

    public init(
        eventType!: Option<String> = None<String>,
        id!: Option<String> = None<String>,
        retryMillis!: Option<Int64> = None<Int64>,
        data!: Option<String> = None<String>,
        comment!: Option<String> = None<String>
    ) {
        this.eventType = eventType
        this.id = id
        this.retryMillis = retryMillis
        this.data = data
        this.comment = comment
    }
}

public class SseDecoderLimits {
    public let maxLineBytes: Int64
    public let maxFieldBytes: Int64
    public let maxDataBytes: Int64
    public let maxEventBytes: Int64
    public let maxBufferedBytes: Int64

    public static let DEFAULTS: SseDecoderLimits = SseDecoderLimits(
        maxLineBytes: 64 * 1024,
        maxFieldBytes: 64 * 1024,
        maxDataBytes: 1024 * 1024,
        maxEventBytes: 2 * 1024 * 1024,
        maxBufferedBytes: 4 * 1024 * 1024
    )

    public init(
        maxLineBytes!: Int64,
        maxFieldBytes!: Int64,
        maxDataBytes!: Int64,
        maxEventBytes!: Int64,
        maxBufferedBytes!: Int64
    ) {
        if (maxLineBytes <= 0 || maxFieldBytes <= 0 || maxDataBytes <= 0 || maxEventBytes <= 0 || maxBufferedBytes <= 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "SSE limits must be positive")
        }
        if (maxFieldBytes > maxLineBytes) {
            throw SseException(SseErrorKind.InvalidConfiguration, "maxFieldBytes must not exceed maxLineBytes")
        }
        if (maxDataBytes > maxEventBytes) {
            throw SseException(SseErrorKind.InvalidConfiguration, "maxDataBytes must not exceed maxEventBytes")
        }
        if (maxEventBytes > maxBufferedBytes) {
            throw SseException(SseErrorKind.InvalidConfiguration, "maxEventBytes must not exceed maxBufferedBytes")
        }
        this.maxLineBytes = maxLineBytes
        this.maxFieldBytes = maxFieldBytes
        this.maxDataBytes = maxDataBytes
        this.maxEventBytes = maxEventBytes
        this.maxBufferedBytes = maxBufferedBytes
    }
}

public abstract class SseDecodeSink {
    public func onEvent(event: SseEvent): Unit
    public func onLastEventIdChanged(id: String): Unit
    public func onRetryChanged(retryMillis: Int64): Unit
}
