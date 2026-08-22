package sse4cj

public enum SseTransportAbortCapability {
    Immediate | ServerWideOnly | Unsupported
}

public enum SseConnectionState {
    Open | Draining | LogicallyClosed | TransportClosed
}

public enum SseCloseReason {
    ExplicitCloseNow
    | GracefulDrainComplete
    | GracefulDeadlineExceeded
    | QueueFrameLimitExceeded
    | QueueByteLimitExceeded
    | WriteFailure
    | ServerShutdown
}

/// Only the connection writer fiber calls write/flush during normal operation.
/// If `SseConnectionConfig.transportAbortCapability == Immediate`, `close()`
/// must additionally be safe to call concurrently and must interrupt in-flight write.
public interface SseConnectionWriter {
    func write(frame: EncodedSseFrame): Unit
    func flush(): Unit
    func close(): Unit
}

public enum SseBackpressureStrategy {
    DisconnectSlowConsumer | DropNewest | DropOldest | RejectSend | BlockWithTimeout
}

public enum SseSendStatus {
    Accepted | Rejected | Dropped | Disconnected | Closed | Failed
}

public class SseSendResult {
    public let status: SseSendStatus
    public let connectionId: String
    public let droppedFrames: Int64

    init(status: SseSendStatus, connectionId: String, droppedFrames!: Int64 = 0) {
        this.status = status
        this.connectionId = connectionId
        this.droppedFrames = droppedFrames
    }
}

public class SseConnectionConfig {
    /// Historical names retained: both limits cover queued + in-flight frames.
    public let maxQueuedFrames: Int64
    public let maxQueuedBytes: Int64
    public let backpressure: SseBackpressureStrategy
    public let blockTimeoutMillis: Int64
    public let heartbeatEnabled: Bool
    public let heartbeatIntervalMillis: Int64
    public let heartbeatComment: String
    public let heartbeatOnlyWhenIdle: Bool
    public let transportAbortCapability: SseTransportAbortCapability

    public init(
        maxQueuedFrames!: Int64 = 256,
        maxQueuedBytes!: Int64 = 1024 * 1024,
        backpressure!: SseBackpressureStrategy = SseBackpressureStrategy.DisconnectSlowConsumer,
        blockTimeoutMillis!: Int64 = 5_000,
        heartbeatEnabled!: Bool = false,
        heartbeatIntervalMillis!: Int64 = 15_000,
        heartbeatComment!: String = "heartbeat",
        heartbeatOnlyWhenIdle!: Bool = true,
        transportAbortCapability!: SseTransportAbortCapability = SseTransportAbortCapability.ServerWideOnly
    ) {
        if (maxQueuedFrames <= 0 || maxQueuedFrames > 1_000_000 || maxQueuedBytes <= 0 || maxQueuedBytes > 1024 * 1024 * 1024) {
            throw SseException(SseErrorKind.InvalidConfiguration, "connection buffer limits must be finite and positive")
        }
        if (blockTimeoutMillis <= 0 || blockTimeoutMillis > 60_000) {
            throw SseException(SseErrorKind.InvalidConfiguration, "block timeout must be in 1..60000 ms")
        }
        if (heartbeatEnabled && heartbeatIntervalMillis <= 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "heartbeat interval must be positive")
        }
        if (heartbeatEnabled) { SseEncoder.encode(SseFrame(comment: Some(heartbeatComment))) }
        this.maxQueuedFrames = maxQueuedFrames
        this.maxQueuedBytes = maxQueuedBytes
        this.backpressure = backpressure
        this.blockTimeoutMillis = blockTimeoutMillis
        this.heartbeatEnabled = heartbeatEnabled
        this.heartbeatIntervalMillis = heartbeatIntervalMillis
        this.heartbeatComment = heartbeatComment
        this.heartbeatOnlyWhenIdle = heartbeatOnlyWhenIdle
        this.transportAbortCapability = transportAbortCapability
    }

    public static let DEFAULTS: SseConnectionConfig = SseConnectionConfig()
}

public enum ReplayGapPolicy { StartLive | Reject | Reset }

public interface ReplayProvider {
    func store(frame: EncodedSseFrame, id: String): Unit
    func replay(afterId: String, gapPolicy: ReplayGapPolicy): Array<EncodedSseFrame>
}

class SseQueueEntry {
    let frame: EncodedSseFrame
    let byteLength: Int64
    init(frame: EncodedSseFrame) {
        this.frame = frame
        byteLength = frame.byteLength
    }
}
