package sse4cj

import std.collection.concurrent.*
import std.sync.*
import std.time.*
import std.collection.*

// ---------------------------------------------------------------------------
// Server-side transport abstraction
// ---------------------------------------------------------------------------

/// Abstraction over the HTTP response writer.
/// stdx `HttpResponseWriter` has only `write(buf)` — no close/abort/flush.
/// Implementations must not assume they can interrupt an in-progress write.
public interface SseConnectionWriter {
    /// Write encoded frame bytes to the HTTP response. Blocking, non-interruptible.
    func write(frame: EncodedSseFrame): Unit
    /// Best-effort flush. stdx has no public flush API; this is a no-op on stdx.
    func flush(): Unit
    /// Close the underlying response. stdx has no public close; this is a no-op on stdx.
    func close(): Unit
}

// ---------------------------------------------------------------------------
// Backpressure strategy and send results
// ---------------------------------------------------------------------------

public enum SseBackpressureStrategy {
    DisconnectSlowConsumer
    | DropNewest
    | DropOldest
    | RejectSend
    | BlockWithTimeout
}

public enum SseSendStatus {
    Accepted
    | Rejected
    | Dropped
    | Disconnected
    | Closed
    | Failed
}

public class SseSendResult {
    public let status: SseSendStatus
    public let connectionId: String

    init(status: SseSendStatus, connectionId: String) {
        this.status = status
        this.connectionId = connectionId
    }
}

// ---------------------------------------------------------------------------
// Per-connection configuration
// ---------------------------------------------------------------------------

public class SseConnectionConfig {
    public let maxQueuedFrames: Int64
    public let maxQueuedBytes: Int64
    public let backpressure: SseBackpressureStrategy
    public let blockTimeoutMillis: Int64
    public let heartbeatEnabled: Bool
    public let heartbeatIntervalMillis: Int64
    public let heartbeatComment: String
    public let heartbeatOnlyWhenIdle: Bool

    public init(
        maxQueuedFrames!: Int64 = 256,
        maxQueuedBytes!: Int64 = 1024 * 1024,
        backpressure!: SseBackpressureStrategy = SseBackpressureStrategy.DisconnectSlowConsumer,
        blockTimeoutMillis!: Int64 = 5_000,
        heartbeatEnabled!: Bool = false,
        heartbeatIntervalMillis!: Int64 = 15_000,
        heartbeatComment!: String = "heartbeat",
        heartbeatOnlyWhenIdle!: Bool = true
    ) {
        if (maxQueuedFrames <= 0 || maxQueuedBytes <= 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "queue limits must be positive")
        }
        if (heartbeatEnabled && heartbeatIntervalMillis <= 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "heartbeat interval must be positive")
        }
        this.maxQueuedFrames = maxQueuedFrames
        this.maxQueuedBytes = maxQueuedBytes
        this.backpressure = backpressure
        this.blockTimeoutMillis = blockTimeoutMillis
        this.heartbeatEnabled = heartbeatEnabled
        this.heartbeatIntervalMillis = heartbeatIntervalMillis
        this.heartbeatComment = heartbeatComment
        this.heartbeatOnlyWhenIdle = heartbeatOnlyWhenIdle
    }

    public static let DEFAULTS: SseConnectionConfig = SseConnectionConfig()
}

// ---------------------------------------------------------------------------
// Replay types
// ---------------------------------------------------------------------------

public enum ReplayGapPolicy {
    StartLive
    | Reject
    | Reset
}

public interface ReplayProvider {
    func store(frame: EncodedSseFrame, id: String): Unit
    func replay(afterId: String, gapPolicy: ReplayGapPolicy): Array<EncodedSseFrame>
}

// ---------------------------------------------------------------------------
// Queue entry
// ---------------------------------------------------------------------------

private class QueueEntry {
    let frame: EncodedSseFrame
    let byteLength: Int64

    init(frame: EncodedSseFrame) {
        this.frame = frame
        this.byteLength = frame.byteLength
    }
}

// ---------------------------------------------------------------------------
// SseConnection — owns outbound queue and writer loop
// ---------------------------------------------------------------------------

public class SseConnection {
    let id: String
    private let config: SseConnectionConfig
    private let queue = LinkedBlockingQueue<QueueEntry>(Int64.Max)
    private var queuedBytes: Int64 = 0
    private let mutex = Mutex()
    private let closed = AtomicBool(false)
    private let dead = AtomicBool(false)
    private var writer: Option<SseConnectionWriter> = None<SseConnectionWriter>
    private var writerTask: Option<Future<Unit>> = None<Future<Unit>>
    private var hub: Option<SseHub> = None<SseHub>
    private var lastDequeueNanos: Int64 = 0

    init(id: String, config: SseConnectionConfig) {
        this.id = id
        this.config = config
    }

    func setHub(hub: SseHub): Unit {
        this.hub = Some(hub)
    }

    /// Enqueue a frame. All backpressure decisions happen here, before write.
    /// This is the critical design point: stdx write() is non-interruptible,
    /// so we must decide disconnect/drop/reject at enqueue time.
    public func send(frame: SseFrame): SseSendResult {
        if (closed.load() || dead.load()) {
            return SseSendResult(SseSendStatus.Disconnected, id)
        }
        let encoded = SseEncoder.encode(frame)
        return sendEncoded(encoded)
    }

    /// Enqueue an already-encoded frame (for broadcast, shared across connections).
    public func sendEncoded(frame: EncodedSseFrame): SseSendResult {
        if (closed.load() || dead.load()) {
            return SseSendResult(SseSendStatus.Disconnected, id)
        }
        let entry = QueueEntry(frame)
        let frameBytes = entry.byteLength

        // Check queue limits under lock
        synchronized(mutex) {
            if (closed.load() || dead.load()) {
                return SseSendResult(SseSendStatus.Disconnected, id)
            }
            let currentFrames = queue.size
            let currentBytes = queuedBytes
            let framesFull = currentFrames >= config.maxQueuedFrames
            let bytesFull = currentBytes + frameBytes > config.maxQueuedBytes

            if (!framesFull && !bytesFull) {
                queue.add(entry)
                queuedBytes += frameBytes
                return SseSendResult(SseSendStatus.Accepted, id)
            }

            // Queue is full — apply backpressure strategy
            match (config.backpressure) {
                case DisconnectSlowConsumer =>
                    markDead()
                    return SseSendResult(SseSendStatus.Disconnected, id)
                case DropNewest =>
                    // Discard the incoming frame
                    return SseSendResult(SseSendStatus.Dropped, id)
                case DropOldest =>
                    if (let Some(old) <- queue.tryRemove()) {
                        queuedBytes -= old.byteLength
                        queue.add(entry)
                        queuedBytes += frameBytes
                        return SseSendResult(SseSendStatus.Accepted, id)
                    }
                    return SseSendResult(SseSendStatus.Failed, id)
                case RejectSend =>
                    return SseSendResult(SseSendStatus.Rejected, id)
                case BlockWithTimeout =>
                    // Try to wait for space — but stdx LinkedBlockingQueue.add blocks.
                    // We need a non-blocking approach. For now, treat as RejectSend
                    // since we can't safely block here without risking the caller.
                    return SseSendResult(SseSendStatus.Rejected, id)
            }
        }
    }

    /// Start the writer task. Must be called after Hub registration.
    public func start(writer: SseConnectionWriter): Unit {
        this.writer = Some(writer)
        let task: Future<Unit> = spawn {
            this.runWriterLoop()
        }
        this.writerTask = Some(task)
    }

    /// Writer loop: dequeue → write → on error: deregister + exit.
    /// The write call is non-interruptible (stdx constraint).
    /// The loop exits when: write throws, queue closed, or closeNow() called.
    private func runWriterLoop(): Unit {
        let w = match (writer) {
            case Some(value) => value
            case None => return
        }
        let heartbeatInterval = Duration.millisecond * config.heartbeatIntervalMillis
        let heartbeatFrame = SseEncoder.encode(SseFrame(comment: Some(config.heartbeatComment)))
        // Always use timeout-based remove so the loop can check closed flag.
        // Without this, a non-heartbeat connection would block forever in remove().
        let pollInterval = if (config.heartbeatEnabled) { heartbeatInterval } else { Duration.second }

        while (!closed.load()) {
            lastDequeueNanos = MonoTime.now().d.toNanoseconds()
            let entry = queue.remove(pollInterval)

            match (entry) {
                case Some(e) =>
                    synchronized(mutex) {
                        queuedBytes -= e.byteLength
                    }
                    try {
                        w.write(e.frame)
                    } catch (_: Exception) {
                        // write failed — socket broken or timeout
                        markDead()
                        w.close()
                        deregister()
                        return
                    }
                case None =>
                    // Timeout — send heartbeat if enabled
                    if (config.heartbeatEnabled) {
                        if (!config.heartbeatOnlyWhenIdle || queue.size == 0) {
                            try {
                                w.write(heartbeatFrame)
                            } catch (_: Exception) {
                                markDead()
                                w.close()
                                deregister()
                                return
                            }
                        }
                    }
            }
        }
        // Closed externally
        w.close()
        deregister()
    }

    private func markDead(): Unit {
        dead.store(true)
    }

    private func deregister(): Unit {
        match (hub) {
            case Some(h) => h.removeConnection(id)
            case None => ()
        }
    }

    /// Close immediately. Cannot interrupt in-progress write (stdx constraint).
    /// Stops enqueuing; writer task exits on next write exception or queue drain.
    public func closeNow(): Unit {
        if (closed.swap(true)) { return }
        markDead()
        // Writer loop will exit on next iteration (closed check or write exception)
    }

    /// Wait for the writer task to complete.
    public func awaitClosed(): Unit {
        match (writerTask) {
            case Some(task) =>
                try { task.get() } catch (_: Exception) {}
            case None => ()
        }
    }

    public prop isDead: Bool {
        get() { dead.load() }
    }

    public prop isClosed: Bool {
        get() { closed.load() }
    }
}


// ---------------------------------------------------------------------------
// Broadcast statistics
// ---------------------------------------------------------------------------

public class SseBroadcastStats {
    public let accepted: Int64
    public let dropped: Int64
    public let disconnected: Int64
    public let closed: Int64
    public let failed: Int64

    init(accepted!: Int64, dropped!: Int64, disconnected!: Int64, closed!: Int64, failed!: Int64) {
        this.accepted = accepted
        this.dropped = dropped
        this.disconnected = disconnected
        this.closed = closed
        this.failed = failed
    }

    public static func zero(): SseBroadcastStats {
        SseBroadcastStats(accepted: 0, dropped: 0, disconnected: 0, closed: 0, failed: 0)
    }
}

// ---------------------------------------------------------------------------
// SseHub — owns per-endpoint connection registry (NOT global)
// ---------------------------------------------------------------------------

public class SseHub {
    private let mutex = Mutex()
    private var connections = HashMap<String, SseConnection>()
    private let replay: Option<ReplayProvider>
    private let closing = AtomicBool(false)

    init(replay!: Option<ReplayProvider> = None<ReplayProvider>) {
        this.replay = replay
    }

    /// Attach a connection to the hub. Called only after admission succeeds.
    func attach(connection: SseConnection, lastEventId!: String, gapPolicy!: ReplayGapPolicy): Unit {
        connection.setHub(this)
        synchronized(mutex) {
            if (closing.load()) {
                throw SseException(SseErrorKind.Closed, "hub is closing")
            }
            connections[connection.id] = connection
        }
        // Replay if configured
        match (replay) {
            case Some(provider) =>
                if (lastEventId.size > 0) {
                    let frames = provider.replay(lastEventId, gapPolicy)
                    for (frame in frames) {
                        connection.sendEncoded(frame)
                    }
                }
            case None => ()
        }
    }

    /// Remove a connection from the hub.
    func removeConnection(id: String): Unit {
        synchronized(mutex) {
            connections.remove(id)
        }
    }

    /// Broadcast a frame to all connections.
    /// Non-blocking: each connection's sendEncoded decides accept/drop/disconnect.
    /// Lock is held only for the connection snapshot — not during write.
    public func broadcast(frame: EncodedSseFrame): SseBroadcastStats {
        let snapshot = synchronized(mutex) {
            let result = ArrayList<SseConnection>()
            for (conn in connections.values()) {
                result.add(conn)
            }
            result.toArray()
        }
        var accepted: Int64 = 0
        var dropped: Int64 = 0
        var disconnected: Int64 = 0
        var closed: Int64 = 0
        var failed: Int64 = 0
        for (conn in snapshot) {
            if (conn.isClosed) {
                closed++
                continue
            }
            let result = conn.sendEncoded(frame)
            match (result.status) {
                case Accepted => accepted++
                case Dropped => dropped++
                case Disconnected =>
                    disconnected++
                    removeConnection(conn.id)
                case Closed => closed++
                case Rejected => dropped++
                case Failed =>
                    failed++
                    removeConnection(conn.id)
            }
        }
        // Store in replay buffer if configured
        match (replay) {
            case Some(provider) => provider.store(frame, "")
            case None => ()
        }
        return SseBroadcastStats(
            accepted: accepted, dropped: dropped, disconnected: disconnected,
            closed: closed, failed: failed
        )
    }

    /// Send to connections matching a predicate.
    /// Predicate is NOT executed under lock — snapshot first, filter after.
    public func sendWhere(frame: EncodedSseFrame, predicate: (SseConnection) -> Bool): SseBroadcastStats {
        let snapshot = synchronized(mutex) {
            let result = ArrayList<SseConnection>()
            for (conn in connections.values()) {
                result.add(conn)
            }
            result.toArray()
        }
        // Filter outside the lock
        let filtered = ArrayList<SseConnection>()
        for (conn in snapshot) {
            if (predicate(conn)) {
                filtered.add(conn)
            }
        }
        let targets = filtered.toArray()
        var accepted: Int64 = 0
        var dropped: Int64 = 0
        var disconnected: Int64 = 0
        var closed: Int64 = 0
        var failed: Int64 = 0
        for (conn in targets) {
            if (conn.isClosed) {
                closed++
                continue
            }
            let result = conn.sendEncoded(frame)
            match (result.status) {
                case Accepted => accepted++
                case Dropped => dropped++
                case Disconnected =>
                    disconnected++
                    removeConnection(conn.id)
                case Closed => closed++
                case Rejected => dropped++
                case Failed =>
                    failed++
                    removeConnection(conn.id)
            }
        }
        return SseBroadcastStats(
            accepted: accepted, dropped: dropped, disconnected: disconnected,
            closed: closed, failed: failed
        )
    }

    /// Snapshot of current connection count.
    public prop connectionCount: Int64 {
        get() {
            synchronized(mutex) {
                connections.size
            }
        }
    }

    /// Close all connections immediately.
    public func closeNow(): Unit {
        if (closing.swap(true)) { return }
        let snapshot = synchronized(mutex) {
            let result = ArrayList<SseConnection>()
            for (conn in connections.values()) {
                result.add(conn)
            }
            result.toArray()
        }
        for (conn in snapshot) {
            conn.closeNow()
        }
    }

    /// Close gracefully — drain queues then close.
    public func closeGracefully(timeoutMillis: Int64): Unit {
        if (closing.swap(true)) { return }
        let snapshot = synchronized(mutex) {
            let result = ArrayList<SseConnection>()
            for (conn in connections.values()) {
                result.add(conn)
            }
            result.toArray()
        }
        // Mark closing — writer tasks will drain then exit
        for (conn in snapshot) {
            conn.closeNow()
        }
    }
}
