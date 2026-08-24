package sse4cj

import std.collection.concurrent.*
import std.sync.*
import std.time.*

public class SseConnection {
    public let id: String
    private let config: SseConnectionConfig
    private let queue = LinkedBlockingQueue<SseQueueEntry>(Int64.Max)
    private let mutex = Mutex()
    private let condition = synchronized(mutex) { mutex.condition() }

    private var queuedBytes: Int64 = 0
    private var inFlightBytes: Int64 = 0
    private var inFlightFrames: Int64 = 0
    private var accepting = true
    private var draining = false
    private var logicalClosed = false
    private var transportClosed = false
    private var closeReasonValue: Option<SseCloseReason> = None<SseCloseReason>

    private let dead = AtomicBool(false)
    private let started = AtomicBool(false)
    private let deregistered = AtomicBool(false)
    private var writer: Option<SseConnectionWriter> = None<SseConnectionWriter>
    private var writerTask: Option<Future<Unit>> = None<Future<Unit>>
    private var hub: Option<SseHub> = None<SseHub>
    private var lastWriteTime = MonoTime.now()
    private var lastHeartbeatTime = MonoTime.now()

    public init(id: String, config!: SseConnectionConfig = SseConnectionConfig.DEFAULTS) {
        if (id.size == 0) { throw SseException(SseErrorKind.InvalidConfiguration, "connection id must not be empty") }
        this.id = id
        this.config = config
    }

    func setHub(value: SseHub): Unit { hub = Some(value) }

    public prop state: SseConnectionState {
        get() { synchronized(mutex) {
            if (transportClosed) { SseConnectionState.TransportClosed }
            else if (logicalClosed) { SseConnectionState.LogicallyClosed }
            else if (draining) { SseConnectionState.Draining }
            else { SseConnectionState.Open }
        } }
    }
    public prop isClosed: Bool { get() { synchronized(mutex) { logicalClosed || transportClosed } } }
    public prop isTransportClosed: Bool { get() { synchronized(mutex) { transportClosed } } }
    public prop isDead: Bool { get() { dead.load() } }
    public prop queuedFrameCount: Int64 { get() { synchronized(mutex) { queue.size } } }
    public prop queuedByteCount: Int64 { get() { synchronized(mutex) { queuedBytes } } }
    public prop inFlightByteCount: Int64 { get() { synchronized(mutex) { inFlightBytes } } }
    public prop bufferedFrameCount: Int64 { get() { synchronized(mutex) { queue.size + inFlightFrames } } }
    public prop bufferedByteCount: Int64 { get() { synchronized(mutex) { queuedBytes + inFlightBytes } } }
    public prop abortCapability: SseTransportAbortCapability { get() { config.transportAbortCapability } }
    public prop closeReason: Option<SseCloseReason> { get() { synchronized(mutex) { closeReasonValue } } }

    public func send(frame: SseFrame): SseSendResult { sendEncoded(SseEncoder.encode(frame)) }

    public func sendEncoded(frame: EncodedSseFrame): SseSendResult {
        match (config.backpressure) {
            case SseBackpressureStrategy.BlockWithTimeout => return blockUntilFits(frame)
            case _ => ()
        }
        var disconnect = false
        var reason = SseCloseReason.QueueByteLimitExceeded
        var result = SseSendResult(SseSendStatus.Failed, id)
        synchronized(mutex) {
            if (!accepting || logicalClosed) {
                result = SseSendResult(SseSendStatus.Closed, id)
            } else if (fitsLocked(frame)) {
                enqueueLocked(frame)
                result = SseSendResult(SseSendStatus.Accepted, id)
            } else {
                match (config.backpressure) {
                    case SseBackpressureStrategy.DisconnectSlowConsumer =>
                        disconnect = true
                        reason = if (queue.size + inFlightFrames >= config.maxQueuedFrames) {
                            SseCloseReason.QueueFrameLimitExceeded
                        } else { SseCloseReason.QueueByteLimitExceeded }
                        result = SseSendResult(SseSendStatus.Disconnected, id)
                    case SseBackpressureStrategy.DropNewest => result = SseSendResult(SseSendStatus.Dropped, id, droppedFrames: 1)
                    case SseBackpressureStrategy.DropOldest => result = dropOldestLocked(frame)
                    case SseBackpressureStrategy.RejectSend => result = SseSendResult(SseSendStatus.Rejected, id)
                    case SseBackpressureStrategy.BlockWithTimeout => ()
                }
            }
        }
        if (disconnect) { requestLogicalClose(reason) }
        result
    }

    func enqueueInitial(frames: Array<EncodedSseFrame>): Bool {
        synchronized(mutex) {
            if (!accepting || logicalClosed) { return false }
            var frameCount: Int64 = 0
            var bytes: Int64 = 0
            for (frame in frames) {
                if (frame.byteLength > config.maxQueuedBytes - bytes) { return false }
                frameCount++
                bytes += frame.byteLength
            }
            if (frameCount > config.maxQueuedFrames - (queue.size + inFlightFrames)) { return false }
            if (bytes > config.maxQueuedBytes - (queuedBytes + inFlightBytes)) { return false }
            for (frame in frames) { enqueueLocked(frame) }
            true
        }
    }

    public func start(value: SseConnectionWriter): Unit {
        var alreadyClosed = false
        synchronized(mutex) {
            if (!started.compareAndSwap(false, true)) { throw SseException(SseErrorKind.InvalidConfiguration, "connection writer already started") }
            writer = Some(value)
            alreadyClosed = logicalClosed
        }
        if (alreadyClosed) {
            try { value.close() } finally { markTransportClosed() }
            return
        }
        let future: Future<Unit> = spawn {=> writerLoop(value)}
        synchronized(mutex) { writerTask = Some(future) }
    }

    public func closeNow(): Unit { requestLogicalClose(SseCloseReason.ExplicitCloseNow) }

    public func closeGracefully(timeoutMillis: Int64): Unit {
        if (timeoutMillis <= 0 || timeoutMillis > 24 * 60 * 60 * 1000) {
            throw SseException(SseErrorKind.InvalidConfiguration, "graceful close timeout must be finite and positive")
        }
        synchronized(mutex) {
            if (logicalClosed || transportClosed) { return }
            accepting = false
            draining = true
            condition.notifyAll()
        }
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        let drained = synchronized(mutex) {
            while (!logicalClosed && !transportClosed && queue.size + inFlightFrames > 0) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                condition.wait(timeout: remaining)
            }
            queue.size + inFlightFrames == 0
        }
        requestLogicalClose(if (drained) { SseCloseReason.GracefulDrainComplete } else { SseCloseReason.GracefulDeadlineExceeded })
    }

    public func awaitClosed(): Unit {
        match (synchronized(mutex) { writerTask }) {
            case Some(task) => try { task.get() } catch (_: Exception) {}
            case None => ()
        }
    }

    public func awaitClosed(timeoutMillis: Int64): Bool {
        if (timeoutMillis <= 0) { throw SseException(SseErrorKind.InvalidConfiguration, "close wait timeout must be positive") }
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!transportClosed) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                condition.wait(timeout: remaining)
            }
            transportClosed
        }
    }

    private func fitsLocked(frame: EncodedSseFrame): Bool {
        if (queue.size + inFlightFrames >= config.maxQueuedFrames) { return false }
        frame.byteLength <= config.maxQueuedBytes - (queuedBytes + inFlightBytes)
    }

    private func enqueueLocked(frame: EncodedSseFrame): Unit {
        queue.add(SseQueueEntry(frame))
        queuedBytes += frame.byteLength
        condition.notify()
    }

    private func dropOldestLocked(frame: EncodedSseFrame): SseSendResult {
        if (frame.byteLength > config.maxQueuedBytes) { return SseSendResult(SseSendStatus.Dropped, id, droppedFrames: 1) }
        var dropped: Int64 = 0
        while (!fitsLocked(frame)) {
            match (queue.tryRemove()) {
                case Some(old) => queuedBytes -= old.byteLength; dropped++
                case None => return SseSendResult(SseSendStatus.Dropped, id, droppedFrames: dropped + 1)
            }
        }
        enqueueLocked(frame)
        SseSendResult(SseSendStatus.Accepted, id, droppedFrames: dropped)
    }

    private func blockUntilFits(frame: EncodedSseFrame): SseSendResult {
        if (frame.byteLength > config.maxQueuedBytes) { return SseSendResult(SseSendStatus.Rejected, id) }
        let deadline = MonoTime.now() + Duration.millisecond * config.blockTimeoutMillis
        synchronized(mutex) {
            while (accepting && !logicalClosed && !fitsLocked(frame)) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { return SseSendResult(SseSendStatus.Rejected, id) }
                condition.wait(timeout: remaining)
            }
            if (!accepting || logicalClosed) { return SseSendResult(SseSendStatus.Closed, id) }
            enqueueLocked(frame)
            SseSendResult(SseSendStatus.Accepted, id)
        }
    }

    private func writerLoop(value: SseConnectionWriter): Unit {
        let heartbeat = if (config.heartbeatEnabled) { Some(SseEncoder.encode(SseFrame(comment: Some(config.heartbeatComment)))) } else { None<EncodedSseFrame> }
        try {
            while (true) {
                match (takeNext(heartbeat)) {
                    case Some(entry) =>
                        try {
                            value.write(entry.frame)
                            value.flush()
                            synchronized(mutex) { lastWriteTime = MonoTime.now(); lastHeartbeatTime = lastWriteTime }
                        } catch (_: Exception) {
                            requestLogicalClose(SseCloseReason.WriteFailure)
                            return
                        } finally { completeInFlight(entry) }
                    case None => return
                }
            }
        } finally {
            try { value.close() } finally { deregisterOnce(); markTransportClosed() }
        }
    }

    private func takeNext(heartbeat: Option<EncodedSseFrame>): Option<SseQueueEntry> {
        synchronized(mutex) {
            while (true) {
                if (logicalClosed) { return None<SseQueueEntry> }
                match (queue.tryRemove()) {
                    case Some(entry) =>
                        queuedBytes -= entry.byteLength
                        if (logicalClosed) { condition.notifyAll(); return None<SseQueueEntry> }
                        inFlightBytes += entry.byteLength
                        inFlightFrames++
                        condition.notifyAll()
                        return Some(entry)
                    case None => ()
                }
                if (draining || !accepting) { return None<SseQueueEntry> }
                match (heartbeat) {
                    case Some(frame) =>
                        let basis = if (config.heartbeatOnlyWhenIdle) { lastWriteTime } else { lastHeartbeatTime }
                        let remaining = (basis + Duration.millisecond * config.heartbeatIntervalMillis) - MonoTime.now()
                        if (remaining <= Duration.Zero) {
                            if (frame.byteLength <= config.maxQueuedBytes - inFlightBytes && inFlightFrames < config.maxQueuedFrames) {
                                let entry = SseQueueEntry(frame)
                                inFlightBytes += entry.byteLength
                                inFlightFrames++
                                return Some(entry)
                            }
                            condition.wait(timeout: Duration.millisecond)
                        } else { condition.wait(timeout: remaining) }
                    case None => condition.wait(timeout: Duration.second)
                }
            }
            None<SseQueueEntry>
        }
    }

    private func completeInFlight(entry: SseQueueEntry): Unit {
        synchronized(mutex) {
            if (inFlightFrames <= 0 || inFlightBytes < entry.byteLength) { throw SseException(SseErrorKind.InternalInvariantViolation, "invalid SSE in-flight accounting") }
            inFlightFrames--
            inFlightBytes -= entry.byteLength
            condition.notifyAll()
        }
    }

    private func clearPendingLocked(): Unit {
        while (let Some(entry) <- queue.tryRemove()) { queuedBytes -= entry.byteLength }
        if (queuedBytes != 0) { throw SseException(SseErrorKind.InternalInvariantViolation, "invalid SSE queued byte accounting") }
    }

    private func requestLogicalClose(reason: SseCloseReason): Unit {
        var first = false
        var activeWriter: Option<SseConnectionWriter> = None<SseConnectionWriter>
        var noWriter = false
        synchronized(mutex) {
            if (!logicalClosed) {
                accepting = false
                draining = false
                logicalClosed = true
                dead.store(true)
                closeReasonValue = Some(reason)
                clearPendingLocked()
                activeWriter = writer
                noWriter = !started.load()
                first = true
                condition.notifyAll()
            }
        }
        if (!first) { return }
        deregisterOnce()
        match (config.transportAbortCapability) {
            case SseTransportAbortCapability.Immediate =>
                match (activeWriter) { case Some(value) => value.close(); case None => () }
            case _ => ()
        }
        if (noWriter) { markTransportClosed() }
    }

    private func deregisterOnce(): Unit {
        if (deregistered.swap(true)) { return }
        match (hub) { case Some(value) => value.removeConnection(id); case None => () }
    }

    private func markTransportClosed(): Unit {
        synchronized(mutex) {
            accepting = false
            draining = false
            logicalClosed = true
            transportClosed = true
            dead.store(true)
            clearPendingLocked()
            condition.notifyAll()
        }
    }
}
