package sse4cj

import std.deriving.Derive
import std.random.Random
import std.sync.*

@Derive[Equatable]
public enum EventSourceReadyState {
    CONNECTING | OPEN | CLOSED
}

public enum SseCallbackExceptionPolicy {
    Continue | Close | ForwardToErrorHandler
}

@Derive[Equatable]
public enum SseReconnectMode {
    Fixed | ExponentialBackoff
}

public class SseReconnectPolicy {
    public let mode: SseReconnectMode
    public let initialDelayMillis: Int64
    public let minDelayMillis: Int64
    public let maxDelayMillis: Int64
    public let jitterMillis: Int64
    public let maxAttempts: Option<Int64>

    public init(
        mode!: SseReconnectMode = SseReconnectMode.Fixed,
        initialDelayMillis!: Int64 = 3000,
        minDelayMillis!: Int64 = 0,
        maxDelayMillis!: Int64 = 60_000,
        jitterMillis!: Int64 = 0,
        maxAttempts!: Option<Int64> = None<Int64>
    ) {
        if (initialDelayMillis < 0 || minDelayMillis < 0 || maxDelayMillis < 0 || jitterMillis < 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "Reconnect delays must be non-negative")
        }
        if (minDelayMillis > maxDelayMillis || initialDelayMillis > maxDelayMillis) {
            throw SseException(SseErrorKind.InvalidConfiguration, "Reconnect delay bounds are inconsistent")
        }
        match (maxAttempts) {
            case Some(value) =>
                if (value <= 0) {
                    throw SseException(SseErrorKind.InvalidConfiguration, "maxAttempts must be positive when present")
                }
            case None => ()
        }
        this.mode = mode
        this.initialDelayMillis = initialDelayMillis
        this.minDelayMillis = minDelayMillis
        this.maxDelayMillis = maxDelayMillis
        this.jitterMillis = jitterMillis
        this.maxAttempts = maxAttempts
    }

    public func delayMillis(
        reconnectIndex: Int64,
        serverRetryMillis: Option<Int64>,
        jitterSource: SseJitterSource
    ): Int64 {
        let base = match (serverRetryMillis) {
            case Some(value) => clampDelay(value)
            case None => exponentialBase(reconnectIndex)
        }
        if (jitterMillis == 0) {
            return base
        }
        let span = if (jitterMillis > 0x3FFF_FFFF) {
            0x7FFF_FFFF
        } else {
            jitterMillis * 2 + 1
        }
        let offset = jitterSource.nextInt64(span) - (span / 2)
        if (offset < 0 && base < -offset) {
            return minDelayMillis
        }
        if (offset > 0 && base > maxDelayMillis - offset) {
            return maxDelayMillis
        }
        clampDelay(base + offset)
    }

    private func exponentialBase(reconnectIndex: Int64): Int64 {
        if (mode == SseReconnectMode.Fixed || reconnectIndex <= 0) {
            return clampDelay(initialDelayMillis)
        }
        var value = initialDelayMillis
        var remaining = reconnectIndex
        while (remaining > 0 && value < maxDelayMillis) {
            if (value > maxDelayMillis / 2) {
                value = maxDelayMillis
            } else {
                value *= 2
            }
            remaining--
        }
        clampDelay(value)
    }

    private func clampDelay(value: Int64): Int64 {
        if (value < minDelayMillis) {
            return minDelayMillis
        }
        if (value > maxDelayMillis) {
            return maxDelayMillis
        }
        value
    }
}

public abstract class SseJitterSource {
    public func nextInt64(upperExclusive: Int64): Int64
}

public class DefaultSseJitterSource <: SseJitterSource {
    private let mutex = Mutex()
    private let random = Random()

    public init() {}

    public func nextInt64(upperExclusive: Int64): Int64 {
        if (upperExclusive <= 1) {
            return 0
        }
        synchronized(mutex) {
            random.nextInt64(upperExclusive)
        }
    }
}

public abstract class SseReconnectSleeper {
    // Return false when interrupted/closed before the delay elapsed.
    public func sleep(delayMillis: Int64): Bool
    public func interrupt(): Unit
}

public class DefaultSseReconnectSleeper <: SseReconnectSleeper {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private var interrupted = false

    public init() {}

    public func sleep(delayMillis: Int64): Bool {
        if (delayMillis <= 0) {
            return synchronized(mutex) {
                !interrupted
            }
        }
        synchronized(mutex) {
            if (interrupted) {
                return false
            }
            condition.wait(timeout: Duration.millisecond * delayMillis)
            !interrupted
        }
    }

    public func interrupt(): Unit {
        synchronized(mutex) {
            interrupted = true
            condition.notifyAll()
        }
    }
}

public open class EventSourceCallbacks {
    public init() {}
    public open func onOpen(): Unit {}
    public open func onMessage(event: SseEvent): Unit {
        let _ = event
    }
    // Called for non-"message" events. There is deliberately no fallback to onMessage.
    public open func onEvent(event: SseEvent): Unit {
        let _ = event
    }
    public open func onAnyEvent(event: SseEvent): Unit {
        let _ = event
    }
    public open func onError(error: SseException): Unit {
        let _ = error
    }
    public open func onRetry(retryMillis: Int64): Unit {
        let _ = retryMillis
    }
    public open func onClosed(): Unit {}
}

public class EventSourceConfig {
    public let url: String
    public let headers: Array<SseHeader>
    public let openTimeoutMillis: Int64
    public let readTimeoutMillis: Int64
    public let readBufferSize: Int64
    public let decoderLimits: SseDecoderLimits
    public let reconnectPolicy: SseReconnectPolicy
    public let callbackExceptionPolicy: SseCallbackExceptionPolicy

    public init(
        url: String,
        headers!: Array<SseHeader> = [],
        openTimeoutMillis!: Int64 = 10_000,
        readTimeoutMillis!: Int64 = 120_000,
        readBufferSize!: Int64 = 16 * 1024,
        decoderLimits!: SseDecoderLimits = SseDecoderLimits.DEFAULTS,
        reconnectPolicy!: SseReconnectPolicy = SseReconnectPolicy(),
        callbackExceptionPolicy!: SseCallbackExceptionPolicy = SseCallbackExceptionPolicy.ForwardToErrorHandler
    ) {
        // Reuse request and stream-reader validation without retaining temporary objects.
        SseHttpRequest(
            url,
            headers: headers,
            openTimeoutMillis: openTimeoutMillis,
            readTimeoutMillis: readTimeoutMillis
        )
        SseStreamReaderConfig(readBufferSize: readBufferSize, decoderLimits: decoderLimits)
        this.url = url
        this.headers = headers.clone()
        this.openTimeoutMillis = openTimeoutMillis
        this.readTimeoutMillis = readTimeoutMillis
        this.readBufferSize = readBufferSize
        this.decoderLimits = decoderLimits
        this.reconnectPolicy = reconnectPolicy
        this.callbackExceptionPolicy = callbackExceptionPolicy
    }
}
