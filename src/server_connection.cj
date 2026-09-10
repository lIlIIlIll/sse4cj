package sse4cj

import std.collection.concurrent.*
import std.io.*
import std.sync.*
import std.time.*

public class SseConnection {
    public let id: String
    private let config: SseConnectionConfig
    private let queue = LinkedBlockingQueue<SseQueueEntry>(Int64.Max)
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }

    private var queuedBytes: Int64 = 0
    private var inFlightBytes: Int64 = 0
    private var inFlightFrames: Int64 = 0
    private var accepting = true
    private var draining = false
    private var logicalClosed = false
    private var transportClosed = false
    private var closeReasonValue: Option<SseCloseReason> = None<SseCloseReason>

    private let dead = AtomicBool(false)
    private let deregistered = AtomicBool(false)
    private var ownerClaimed = false
    private var writer: Option<SseConnectionWriter> = None<SseConnectionWriter>
    private var writerTask: Option<Future<Unit>> = None<Future<Unit>>
    private var writerAbortRequested = false
    private var hub: Option<SseHub> = None<SseHub>
    private var lastWriteTime = MonoTime.now()
    private var lastHeartbeatTime = MonoTime.now()

    // Pull mode is owned by the same queue and accounting state as push mode.
    private var pullAbort: Option<() -> Unit> = None<() -> Unit>
    private var pullReleased: Option<() -> Unit> = None<() -> Unit>
    private var pullAbortRequested = false
    private var pullCurrent: Option<SseQueueEntry> = None<SseQueueEntry>
    private var pullOffset: Int64 = 0
    private var pullAckPending = false
    private var pullCloseRequested = false
    private var pullReadActive = false
    private var pullTerminalCloseClaimed = false
    private var pullHeartbeat: Option<EncodedSseFrame> = None<EncodedSseFrame>

    public init(id: String, config!: SseConnectionConfig = SseConnectionConfig.DEFAULTS) {
        if (id.size == 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "connection id must not be empty")
        }
        this.id = id
        this.config = config
    }

    func setHub(value: SseHub): Unit {
        hub = Some(value)
    }

    public prop state: SseConnectionState {
        get() {
            synchronized(mutex) {
                if (transportClosed) {
                    SseConnectionState.TransportClosed
                } else if (logicalClosed) {
                    SseConnectionState.LogicallyClosed
                } else if (draining) {
                    SseConnectionState.Draining
                } else {
                    SseConnectionState.Open
                }
            }
        }
    }
    public prop isClosed: Bool {
        get() {
            synchronized(mutex) {
                logicalClosed || transportClosed
            }
        }
    }
    public prop isTransportClosed: Bool {
        get() {
            synchronized(mutex) {
                transportClosed
            }
        }
    }
    public prop isDead: Bool {
        get() {
            dead.load()
        }
    }
    public prop queuedFrameCount: Int64 {
        get() {
            synchronized(mutex) {
                queue.size
            }
        }
    }
    public prop queuedByteCount: Int64 {
        get() {
            synchronized(mutex) {
                queuedBytes
            }
        }
    }
    public prop inFlightByteCount: Int64 {
        get() {
            synchronized(mutex) {
                inFlightBytes
            }
        }
    }
    public prop bufferedFrameCount: Int64 {
        get() {
            synchronized(mutex) {
                queue.size + inFlightFrames
            }
        }
    }
    public prop bufferedByteCount: Int64 {
        get() {
            synchronized(mutex) {
                queuedBytes + inFlightBytes
            }
        }
    }
    public prop abortCapability: SseTransportAbortCapability {
        get() {
            config.transportAbortCapability
        }
    }
    public prop closeReason: Option<SseCloseReason> {
        get() {
            synchronized(mutex) {
                closeReasonValue
            }
        }
    }

    public func send(frame: SseFrame): SseSendResult {
        sendEncoded(SseEncoder.encode(frame))
    }

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
                        } else {
                            SseCloseReason.QueueByteLimitExceeded
                        }
                        result = SseSendResult(SseSendStatus.Disconnected, id)
                    case SseBackpressureStrategy.DropNewest => result = SseSendResult(SseSendStatus.Dropped, id,
                        droppedFrames: 1)
                    case SseBackpressureStrategy.DropOldest => result = dropOldestLocked(frame)
                    case SseBackpressureStrategy.RejectSend => result = SseSendResult(SseSendStatus.Rejected, id)
                    case SseBackpressureStrategy.BlockWithTimeout => ()
                }
            }
        }
        if (disconnect) {
            requestLogicalClose(reason)
        }
        result
    }

    func enqueueInitial(frames: Array<EncodedSseFrame>): Bool {
        synchronized(mutex) {
            if (!accepting || logicalClosed) {
                return false
            }
            var frameCount: Int64 = 0
            var bytes: Int64 = 0
            for (frame in frames) {
                if (frame.byteLength > config.maxQueuedBytes - bytes) {
                    return false
                }
                frameCount++
                bytes += frame.byteLength
            }
            if (frameCount > config.maxQueuedFrames - (queue.size + inFlightFrames)) {
                return false
            }
            if (bytes > config.maxQueuedBytes - (queuedBytes + inFlightBytes)) {
                return false
            }
            for (frame in frames) {
                enqueueLocked(frame)
            }
            true
        }
    }

    public func start(value: SseConnectionWriter): Unit {
        var alreadyClosed = false
        synchronized(mutex) {
            if (ownerClaimed) {
                throw SseException(SseErrorKind.InvalidConfiguration, "connection writer already started")
            }
            writer = Some(value)
            ownerClaimed = true
            alreadyClosed = logicalClosed
        }
        if (alreadyClosed) {
            try {
                value.close()
            } finally {
                markTransportClosed()
            }
            return
        }
        let future: Future<Unit> = spawn {=> writerLoop(value)}
        synchronized(mutex) {
            writerTask = Some(future)
        }
    }

    func openStream(abort: () -> Unit, released: () -> Unit): SseConnectionStream {
        let stream = SseConnectionStream(this)
        synchronized(mutex) {
            if (logicalClosed || transportClosed) {
                throw SseException(SseErrorKind.Closed, "connection is closed")
            }
            if (ownerClaimed) {
                throw SseException(SseErrorKind.InvalidConfiguration, "connection transport owner already started")
            }
            pullAbort = Some(abort)
            pullReleased = Some(released)
            pullHeartbeat = if (config.heartbeatEnabled) {
                Some(SseEncoder.encode(SseFrame(comment: Some(config.heartbeatComment))))
            } else {
                None<EncodedSseFrame>
            }
            ownerClaimed = true
        }
        stream
    }

    public func closeNow(): Unit {
        requestLogicalClose(SseCloseReason.ExplicitCloseNow)
    }

    public func closeGracefully(timeoutMillis: Int64): Unit {
        if (timeoutMillis <= 0 || timeoutMillis > 24 * 60 * 60 * 1000) {
            throw SseException(SseErrorKind.InvalidConfiguration, "graceful close timeout must be finite and positive")
        }
        synchronized(mutex) {
            if (logicalClosed || transportClosed) {
                return
            }
            accepting = false
            draining = true
            condition.notifyAll()
        }
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        let drained = synchronized(mutex) {
            while (!logicalClosed && !transportClosed && queue.size + inFlightFrames > 0) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                condition.wait(timeout: remaining)
            }
            queue.size + inFlightFrames == 0
        }
        requestLogicalClose(if (drained) {
            SseCloseReason.GracefulDrainComplete
        } else {
            SseCloseReason.GracefulDeadlineExceeded
        })
    }

    public func awaitClosed(): Unit {
        let task = synchronized(mutex) {
            while (!transportClosed) {
                condition.wait()
            }
            writerTask
        }
        match (task) {
            case Some(value) => try {
                value.get()
            } catch (_: Exception) {}
            case None => ()
        }
    }

    public func awaitClosed(timeoutMillis: Int64): Bool {
        if (timeoutMillis <= 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "close wait timeout must be positive")
        }
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!transportClosed) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                condition.wait(timeout: remaining)
            }
            transportClosed
        }
    }

    private func fitsLocked(frame: EncodedSseFrame): Bool {
        if (queue.size + inFlightFrames >= config.maxQueuedFrames) {
            return false
        }
        frame.byteLength <= config.maxQueuedBytes - (queuedBytes + inFlightBytes)
    }

    private func enqueueLocked(frame: EncodedSseFrame): Unit {
        queue.add(SseQueueEntry(frame))
        queuedBytes += frame.byteLength
        condition.notify()
    }

    private func dropOldestLocked(frame: EncodedSseFrame): SseSendResult {
        if (frame.byteLength > config.maxQueuedBytes) {
            return SseSendResult(SseSendStatus.Dropped, id, droppedFrames: 1)
        }
        var dropped: Int64 = 0
        while (!fitsLocked(frame)) {
            match (queue.tryRemove()) {
                case Some(old) =>
                    queuedBytes -= old.byteLength
                    dropped++
                case None => return SseSendResult(SseSendStatus.Dropped, id, droppedFrames: dropped + 1)
            }
        }
        enqueueLocked(frame)
        SseSendResult(SseSendStatus.Accepted, id, droppedFrames: dropped)
    }

    private func blockUntilFits(frame: EncodedSseFrame): SseSendResult {
        if (frame.byteLength > config.maxQueuedBytes) {
            return SseSendResult(SseSendStatus.Rejected, id)
        }
        let deadline = MonoTime.now() + Duration.millisecond * config.blockTimeoutMillis
        synchronized(mutex) {
            while (accepting && !logicalClosed && !fitsLocked(frame)) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    return SseSendResult(SseSendStatus.Rejected, id)
                }
                condition.wait(timeout: remaining)
            }
            if (!accepting || logicalClosed) {
                return SseSendResult(SseSendStatus.Closed, id)
            }
            enqueueLocked(frame)
            SseSendResult(SseSendStatus.Accepted, id)
        }
    }

    private func writerLoop(value: SseConnectionWriter): Unit {
        let heartbeat = if (config.heartbeatEnabled) {
            Some(SseEncoder.encode(SseFrame(comment: Some(config.heartbeatComment))))
        } else {
            None<EncodedSseFrame>
        }
        try {
            while (true) {
                match (takeNext(heartbeat)) {
                    case Some(entry) => try {
                        value.write(entry.frame)
                        value.flush()
                        synchronized(mutex) {
                            lastWriteTime = MonoTime.now()
                            lastHeartbeatTime = lastWriteTime
                        }
                    } catch (_: Exception) {
                        requestLogicalClose(SseCloseReason.WriteFailure)
                        return
                    } finally {
                        completeInFlight(entry)
                    }
                    case None => return
                }
            }
        } finally {
            try {
                value.close()
            } finally {
                deregisterOnce();
                markTransportClosed()
            }
        }
    }

    private func takeNext(heartbeat: Option<EncodedSseFrame>): Option<SseQueueEntry> {
        synchronized(mutex) {
            takeNextLocked(heartbeat)
        }
    }

    // Caller holds mutex. Both the push writer and pull stream use this queue/heartbeat algorithm.
    private func takeNextLocked(heartbeat: Option<EncodedSseFrame>): Option<SseQueueEntry> {
        while (true) {
            if (logicalClosed) {
                return None<SseQueueEntry>
            }
            match (queue.tryRemove()) {
                case Some(entry) =>
                    queuedBytes -= entry.byteLength
                    inFlightBytes += entry.byteLength
                    inFlightFrames++
                    condition.notifyAll()
                    return Some(entry)
                case None => ()
            }
            if (draining || !accepting || pullCloseRequested) {
                return None<SseQueueEntry>
            }
            match (heartbeat) {
                case Some(frame) =>
                    let basis = if (config.heartbeatOnlyWhenIdle) {
                        lastWriteTime
                    } else {
                        lastHeartbeatTime
                    }
                    let remaining = (basis + Duration.millisecond * config.heartbeatIntervalMillis) - MonoTime.now()
                    if (remaining <= Duration.Zero) {
                        if (frame.byteLength <= config.maxQueuedBytes - inFlightBytes && inFlightFrames < config
                            .maxQueuedFrames) {
                            let entry = SseQueueEntry(frame)
                            inFlightBytes += entry.byteLength
                            inFlightFrames++
                            return Some(entry)
                        }
                        condition.wait(timeout: Duration.millisecond)
                    } else {
                        condition.wait(timeout: remaining)
                    }
                case None => condition.wait(timeout: Duration.second)
            }
        }
        None<SseQueueEntry>
    }

    private func completeInFlight(entry: SseQueueEntry): Unit {
        synchronized(mutex) {
            completeInFlightLocked(entry)
        }
    }

    private func completeInFlightLocked(entry: SseQueueEntry): Unit {
        if (inFlightFrames <= 0 || inFlightBytes < entry.byteLength) {
            throw SseException(SseErrorKind.InternalInvariantViolation, "invalid SSE in-flight accounting")
        }
        inFlightFrames--
        inFlightBytes -= entry.byteLength
        condition.notifyAll()
    }

    private func clearPendingLocked(): Unit {
        while (let Some(entry) <- queue.tryRemove()) {
            queuedBytes -= entry.byteLength
        }
        if (queuedBytes != 0) {
            throw SseException(SseErrorKind.InternalInvariantViolation, "invalid SSE queued byte accounting")
        }
    }

    private func isForcedCloseReason(reason: SseCloseReason): Bool {
        match (reason) {
            case SseCloseReason.GracefulDrainComplete => false
            case _ => true
        }
    }

    private func claimLogicalCloseLocked(reason: SseCloseReason): Bool {
        if (logicalClosed) {
            return false
        }
        accepting = false
        draining = false
        logicalClosed = true
        dead.store(true)
        closeReasonValue = Some(reason)
        clearPendingLocked()
        condition.notifyAll()
        true
    }

    private func requestLogicalClose(reason: SseCloseReason): Unit {
        var first = false
        var noOwner = false
        var activeWriter: Option<SseConnectionWriter> = None<SseConnectionWriter>
        var abort: Option<() -> Unit> = None<() -> Unit>
        synchronized(mutex) {
            first = claimLogicalCloseLocked(reason)
            noOwner = first && !ownerClaimed
            if (isForcedCloseReason(reason) && !transportClosed) {
                if (!pullAbortRequested) {
                    match (pullAbort) {
                        case Some(value) =>
                            pullAbortRequested = true
                            abort = Some(value)
                        case None => ()
                    }
                }
                match (config.transportAbortCapability) {
                    case SseTransportAbortCapability.Immediate =>
                        if (!writerAbortRequested) {
                            match (writer) {
                                case Some(value) =>
                                    writerAbortRequested = true
                                    activeWriter = Some(value)
                                case None => ()
                            }
                        }
                    case _ => ()
                }
            }
        }
        if (first) {
            deregisterOnce()
        }
        match (abort) {
            case Some(value) => try {
                value()
            } catch (_: Exception) {}
            case None => ()
        }
        match (activeWriter) {
            case Some(value) => value.close();
            case None => ()
        }
        if (noOwner) {
            markTransportClosed()
        }
    }

    func readPull(destination: Array<Byte>): Int64 {
        if (destination.isEmpty()) {
            return 0
        }
        synchronized(mutex) {
            if (pullReadActive) {
                throw SseException(SseErrorKind.InvalidConfiguration, "SSE connection stream permits one reader")
            }
            pullReadActive = true
        }
        try {
            let (count, deregister) = synchronized(mutex) {
                readPullLocked(destination)
            }
            if (deregister) {
                deregisterOnce()
            }
            count
        } finally {
            synchronized(mutex) {
                pullReadActive = false
                condition.notifyAll()
            }
        }
    }

    // The previous call's final slice is acknowledged here. Wirestack calls read again
    // only after the prior H1 write or H2 write ticket has completed.
    private func readPullLocked(destination: Array<Byte>): (Int64, Bool) {
        if (pullAckPending) {
            match (pullCurrent) {
                case Some(entry) => completeInFlightLocked(entry)
                case None => throw SseException(SseErrorKind.InternalInvariantViolation,
                    "missing SSE pull acknowledgement")
            }
            pullCurrent = None<SseQueueEntry>
            pullOffset = 0
            pullAckPending = false
            lastWriteTime = MonoTime.now()
            lastHeartbeatTime = lastWriteTime
        }
        if (pullCloseRequested || logicalClosed) {
            return (0, false)
        }

        if (pullCurrent.isNone()) {
            match (takeNextLocked(pullHeartbeat)) {
                case Some(entry) =>
                    pullCurrent = Some(entry)
                    pullOffset = 0
                case None =>
                    var claimedGraceful = false
                    if (draining && !logicalClosed && queue.size + inFlightFrames == 0) {
                        claimedGraceful = claimLogicalCloseLocked(SseCloseReason.GracefulDrainComplete)
                    }
                    return (0, claimedGraceful)
            }
        }

        match (pullCurrent) {
            case Some(entry) =>
                let remaining = entry.byteLength - pullOffset
                let count = min(destination.size, remaining)
                entry.frame.sharedBytes().copyTo(destination, pullOffset, 0, count)
                pullOffset += count
                if (pullOffset == entry.byteLength) {
                    pullAckPending = true
                }
                (count, false)
            case None => throw SseException(SseErrorKind.InternalInvariantViolation, "missing SSE pull frame")
        }
    }

    func cancelPull(): Unit {
        requestLogicalClose(SseCloseReason.WriteFailure)
    }

    func closePull(): Unit {
        var first = false
        var unexpected = false
        synchronized(mutex) {
            if (!pullCloseRequested && !pullTerminalCloseClaimed) {
                pullCloseRequested = true
                first = true
                unexpected = closeReasonValue.isNone()
                condition.notifyAll()
            }
        }
        if (!first) {
            return
        }
        if (unexpected) {
            requestLogicalClose(SseCloseReason.WriteFailure)
        }

        let released = synchronized(mutex) {
            while (pullReadActive) {
                condition.wait()
            }
            pullTerminalCloseClaimed = true
            match (pullCurrent) {
                case Some(entry) =>
                    completeInFlightLocked(entry)
                    pullCurrent = None<SseQueueEntry>
                    pullOffset = 0
                    pullAckPending = false
                case None => ()
            }
            let value = pullReleased
            pullReleased = None<() -> Unit>
            pullAbort = None<() -> Unit>
            pullHeartbeat = None<EncodedSseFrame>
            value
        }
        deregisterOnce()
        markTransportClosed()
        match (released) {
            case Some(value) => value();
            case None => ()
        }
    }

    func isPullClosed(): Bool {
        synchronized(mutex) {
            logicalClosed || pullCloseRequested || transportClosed
        }
    }

    private func deregisterOnce(): Unit {
        if (deregistered.swap(true)) {
            return
        }
        match (hub) {
            case Some(value) => value.removeConnection(id);
            case None => ()
        }
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

class SseConnectionStream <: InputStream & Resource {
    private let connection: SseConnection

    init(connection: SseConnection) {
        this.connection = connection
    }

    public prop id: String {
        get() {
            connection.id
        }
    }
    public func read(destination: Array<Byte>): Int64 {
        connection.readPull(destination)
    }
    public func cancel(): Unit {
        connection.cancelPull()
    }
    public func close(): Unit {
        connection.closePull()
    }
    public func isClosed(): Bool {
        connection.isPullClosed()
    }
}
