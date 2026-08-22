package sse4cj

import std.collection.*
import std.sync.*
import std.time.*

public class SseBroadcastStats {
    public let accepted: Int64
    public let dropped: Int64
    public let disconnected: Int64
    public let closed: Int64
    public let failed: Int64
    init(accepted!: Int64, dropped!: Int64, disconnected!: Int64, closed!: Int64, failed!: Int64) {
        this.accepted = accepted; this.dropped = dropped; this.disconnected = disconnected; this.closed = closed; this.failed = failed
    }
    public static func zero(): SseBroadcastStats { SseBroadcastStats(accepted: 0, dropped: 0, disconnected: 0, closed: 0, failed: 0) }
}

public class SseHub {
    private let mutex = Mutex()
    private var connections = HashMap<String, SseConnection>()
    private let replay: Option<ReplayProvider>
    private let closing = AtomicBool(false)

    public init(replay!: Option<ReplayProvider> = None<ReplayProvider>) { this.replay = replay }

    /// Attach is serialized with replay-aware logical broadcasts. This creates a
    /// total ordering: a frame is either already in replay before attach, or it is
    /// delivered live after the connection has been registered. It cannot be lost
    /// in the replay/live handoff window.
    func attach(connection: SseConnection, lastEventId!: String = "", gapPolicy!: ReplayGapPolicy = ReplayGapPolicy.StartLive): Unit {
        connection.setHub(this)
        synchronized(mutex) {
            if (closing.load()) { throw SseException(SseErrorKind.Closed, "hub is closing") }
            if (connections.contains(connection.id)) { throw SseException(SseErrorKind.InvalidConfiguration, "duplicate SSE connection id") }
            match (replay) {
                case Some(provider) =>
                    if (lastEventId.size > 0) {
                        let frames = provider.replay(lastEventId, gapPolicy)
                        if (!connection.enqueueInitial(frames)) { throw SseException(SseErrorKind.SlowConsumer, "replay does not fit connection buffer limits") }
                    }
                case None => ()
            }
            connections[connection.id] = connection
        }
    }

    func removeConnection(id: String): Unit { synchronized(mutex) { connections.remove(id) } }
    public prop connectionCount: Int64 { get() { synchronized(mutex) { connections.size } } }

    /// Preferred replay-aware broadcast path. The logical frame is encoded once,
    /// recorded by id when present, and the same immutable bytes are enqueued to
    /// every current connection.
    public func broadcast(frame: SseFrame): SseBroadcastStats {
        let encoded = SseEncoder.encode(frame)
        let id = match (frame.id) { case Some(value) => value; case None => "" }
        let targets = synchronized(mutex) {
            match (replay) {
                case Some(provider) => provider.store(encoded, id)
                case None => ()
            }
            snapshotLocked()
        }
        deliver(encoded, targets)
    }

    /// Compatibility/high-performance path for callers that already own encoded
    /// bytes. Because EncodedSseFrame intentionally does not expose parsed fields,
    /// this overload cannot infer an event id and therefore does not add replay
    /// history. Use broadcast(SseFrame) when replay must be retained.
    public func broadcast(frame: EncodedSseFrame): SseBroadcastStats { deliver(frame, snapshot()) }

    /// Encoded replay-aware path for callers that already encoded a logical event
    /// and can provide its id without reparsing the wire bytes.
    public func broadcastEncoded(frame: EncodedSseFrame, id: String): SseBroadcastStats {
        let targets = synchronized(mutex) {
            match (replay) {
                case Some(provider) => provider.store(frame, id)
                case None => ()
            }
            snapshotLocked()
        }
        deliver(frame, targets)
    }

    public func sendWhere(frame: EncodedSseFrame, predicate: (SseConnection) -> Bool): SseBroadcastStats {
        let selected = ArrayList<SseConnection>()
        var predicateFailures: Int64 = 0
        for (connection in snapshot()) {
            try { if (predicate(connection)) { selected.add(connection) } }
            catch (_: Exception) { predicateFailures++ }
        }
        let stats = deliver(frame, selected.toArray())
        SseBroadcastStats(accepted: stats.accepted, dropped: stats.dropped, disconnected: stats.disconnected, closed: stats.closed, failed: stats.failed + predicateFailures)
    }

    public func closeNow(): Unit {
        if (closing.swap(true)) { return }
        for (connection in snapshot()) { connection.closeNow() }
    }

    public func closeGracefully(timeoutMillis: Int64): Unit {
        if (timeoutMillis <= 0 || timeoutMillis > 24 * 60 * 60 * 1000) { throw SseException(SseErrorKind.InvalidConfiguration, "graceful close timeout must be finite and positive") }
        if (closing.swap(true)) { return }
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        for (connection in snapshot()) {
            let remaining = deadline - MonoTime.now()
            if (remaining <= Duration.Zero) { connection.closeNow() }
            else {
                let nanos = remaining.toNanoseconds()
                let millis = nanos / 1_000_000
                connection.closeGracefully(if (nanos % 1_000_000 == 0) { millis } else { millis + 1 })
            }
        }
    }

    private func snapshot(): Array<SseConnection> { synchronized(mutex) { snapshotLocked() } }

    private func snapshotLocked(): Array<SseConnection> {
        let result = ArrayList<SseConnection>()
        for (connection in connections.values()) { result.add(connection) }
        result.toArray()
    }

    private func deliver(frame: EncodedSseFrame, targets: Array<SseConnection>): SseBroadcastStats {
        var accepted: Int64 = 0; var dropped: Int64 = 0; var disconnected: Int64 = 0; var closed: Int64 = 0; var failed: Int64 = 0
        for (connection in targets) {
            let result = try { connection.sendEncoded(frame) } catch (_: Exception) { failed++; removeConnection(connection.id); continue }
            match (result.status) {
                case SseSendStatus.Accepted => accepted++
                case SseSendStatus.Dropped | SseSendStatus.Rejected => dropped++
                case SseSendStatus.Disconnected => disconnected++
                case SseSendStatus.Closed => closed++
                case SseSendStatus.Failed => failed++
            }
        }
        SseBroadcastStats(accepted: accepted, dropped: dropped, disconnected: disconnected, closed: closed, failed: failed)
    }
}
