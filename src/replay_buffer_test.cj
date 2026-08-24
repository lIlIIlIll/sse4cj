package sse4cj

import std.collection.*
import std.sync.*
import std.time.*

private class ReplayRecordingWriter <: SseConnectionWriter {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) { mutex.condition() }
    private let values = ArrayList<String>()

    public func write(frame: EncodedSseFrame): Unit {
        synchronized(mutex) {
            values.add(String.fromUtf8(frame.toBytes()))
            condition.notifyAll()
        }
    }
    public func flush(): Unit {}
    public func close(): Unit { synchronized(mutex) { condition.notifyAll() } }

    func frames(): Array<String> { synchronized(mutex) { values.toArray() } }
}

private func replayThrowsGap(replay: MemoryReplayBuffer, id: String): Bool {
    try {
        replay.replay(id, ReplayGapPolicy.Reject)
        false
    } catch (e: SseException) {
        e.kind == SseErrorKind.ReplayGap
    }
}

@Test
func memoryReplayBufferEvictsByFrameAndByteBounds() {
    let replay = MemoryReplayBuffer(maxFrames: 2, maxBytes: 1024)
    let f1 = SseEncoder.encode(SseFrame(id: Some("1"), data: Some("one")))
    let f2 = SseEncoder.encode(SseFrame(id: Some("2"), data: Some("two")))
    let f3 = SseEncoder.encode(SseFrame(id: Some("3"), data: Some("three")))
    replay.store(f1, "1")
    replay.store(f2, "2")
    replay.store(f3, "3")

    @Assert(replay.frameCount, 2)
    @Assert(replayThrowsGap(replay, "1"))
    let frames = replay.replay("2", ReplayGapPolicy.Reject)
    @Assert(frames.size, 1)
    @Assert(String.fromUtf8(frames[0].toBytes()), "id:3\ndata:three\n\n")
    @Assert(replay.byteCount <= replay.maxBytes)
}

@Test
func memoryReplayBufferRingGrowsWrapsAndPreservesOrder() {
    let replay = MemoryReplayBuffer(maxFrames: 20, maxBytes: 64 * 1024)
    for (index in 1..31) {
        let id = index.toString()
        let encoded = SseEncoder.encode(SseFrame(id: Some(id), data: Some("v" + id)))
        replay.store(encoded, id)
    }
    @Assert(replay.frameCount, 20)
    @Assert(replayThrowsGap(replay, "10"))
    let frames = replay.replay("11", ReplayGapPolicy.Reject)
    @Assert(frames.size, 19)
    @Assert(String.fromUtf8(frames[0].toBytes()), "id:12\ndata:v12\n\n")
    @Assert(String.fromUtf8(frames[18].toBytes()), "id:30\ndata:v30\n\n")
}

@Test
func memoryReplayBufferGapPoliciesAreExplicit() {
    let replay = MemoryReplayBuffer(maxFrames: 2, maxBytes: 1024)
    let frame = SseEncoder.encode(SseFrame(id: Some("2"), data: Some("two")))
    replay.store(frame, "2")

    @Assert(replay.replay("missing", ReplayGapPolicy.StartLive).size, 0)
    let reset = replay.replay("missing", ReplayGapPolicy.Reset)
    @Assert(reset.size, 1)
    @Assert(String.fromUtf8(reset[0].toBytes()), "id:\n\n")
    @Assert(replayThrowsGap(replay, "missing"))
}

@Test
func memoryReplayBufferUsesMostRecentDuplicateId() {
    let replay = MemoryReplayBuffer(maxFrames: 8, maxBytes: 4096)
    replay.store(SseEncoder.encode(SseFrame(id: Some("1"), data: Some("old"))), "1")
    replay.store(SseEncoder.encode(SseFrame(id: Some("2"), data: Some("middle"))), "2")
    replay.store(SseEncoder.encode(SseFrame(id: Some("1"), data: Some("new"))), "1")
    replay.store(SseEncoder.encode(SseFrame(id: Some("3"), data: Some("tail"))), "3")

    let frames = replay.replay("1", ReplayGapPolicy.Reject)
    @Assert(frames.size, 1)
    @Assert(String.fromUtf8(frames[0].toBytes()), "id:3\ndata:tail\n\n")
}

@Test
func hubLogicalBroadcastRecordsReplayAndPreservesReplayThenLiveOrder() {
    let replay = MemoryReplayBuffer(maxFrames: 8, maxBytes: 4096)
    let hub = SseHub(replay: Some(replay))
    hub.broadcast(SseFrame(id: Some("1"), data: Some("one")))
    hub.broadcast(SseFrame(id: Some("2"), data: Some("two")))
    hub.broadcast(SseFrame(id: Some("3"), data: Some("three")))

    let connection = SseConnection(
        "replay-live",
        config: SseConnectionConfig(maxQueuedFrames: 8, maxQueuedBytes: 4096)
    )
    hub.attach(connection, lastEventId: "1", gapPolicy: ReplayGapPolicy.Reject)
    hub.broadcast(SseFrame(id: Some("4"), data: Some("four")))

    let writer = ReplayRecordingWriter()
    connection.start(writer)
    connection.closeGracefully(1000)
    @Assert(connection.awaitClosed(1000))
    let frames = writer.frames()
    @Assert(frames.size, 3)
    @Assert(frames[0], "id:2\ndata:two\n\n")
    @Assert(frames[1], "id:3\ndata:three\n\n")
    @Assert(frames[2], "id:4\ndata:four\n\n")
}

@Test
func hubReplayGapPoliciesDoNotCreateGhostConnections() {
    let replay = MemoryReplayBuffer(maxFrames: 2, maxBytes: 1024)
    let hub = SseHub(replay: Some(replay))
    hub.broadcast(SseFrame(id: Some("2"), data: Some("two")))

    var rejected = false
    try {
        hub.attach(SseConnection("reject-gap"), lastEventId: "missing", gapPolicy: ReplayGapPolicy.Reject)
    } catch (e: SseException) {
        rejected = e.kind == SseErrorKind.ReplayGap
    }
    @Assert(rejected)
    @Assert(hub.connectionCount, 0)

    let reset = SseConnection("reset-gap")
    hub.attach(reset, lastEventId: "missing", gapPolicy: ReplayGapPolicy.Reset)
    let writer = ReplayRecordingWriter()
    reset.start(writer)
    reset.closeGracefully(1000)
    @Assert(reset.awaitClosed(1000))
    let resetFrames = writer.frames()
    @Assert(resetFrames.size, 1)
    @Assert(resetFrames[0], "id:\n\n")

    let live = SseConnection("start-live")
    hub.attach(live, lastEventId: "missing", gapPolicy: ReplayGapPolicy.StartLive)
    @Assert(live.queuedFrameCount, 0)
    live.closeNow()
    @Assert(hub.connectionCount, 0)
}

@Test
func replayBatchMustFitConnectionBufferAtomically() {
    let replay = MemoryReplayBuffer(maxFrames: 8, maxBytes: 4096)
    let hub = SseHub(replay: Some(replay))
    hub.broadcast(SseFrame(id: Some("1"), data: Some("one")))
    hub.broadcast(SseFrame(id: Some("2"), data: Some("two")))
    hub.broadcast(SseFrame(id: Some("3"), data: Some("three")))

    let tooSmall = SseConnection(
        "too-small",
        config: SseConnectionConfig(maxQueuedFrames: 1, maxQueuedBytes: 4096)
    )
    var slow = false
    try {
        hub.attach(tooSmall, lastEventId: "1", gapPolicy: ReplayGapPolicy.Reject)
    } catch (e: SseException) {
        slow = e.kind == SseErrorKind.SlowConsumer
    }
    @Assert(slow)
    @Assert(tooSmall.queuedFrameCount, 0)
    @Assert(hub.connectionCount, 0)
}

@Test
func encodedBroadcastWithoutIdDoesNotInventReplayHistory() {
    let replay = MemoryReplayBuffer(maxFrames: 8, maxBytes: 4096)
    let hub = SseHub(replay: Some(replay))
    let encoded = SseEncoder.encode(SseFrame(id: Some("1"), data: Some("one")))
    hub.broadcast(encoded)
    @Assert(replay.frameCount, 0)

    hub.broadcastEncoded(encoded, "1")
    @Assert(replay.frameCount, 1)
}

@Test
func memoryReplayBufferEvictsOnByteLimitIndependentlyOfFrameLimit() {
    let f1 = SseEncoder.encode(SseFrame(id: Some("1"), data: Some("aaaaaaaa")))
    let f2 = SseEncoder.encode(SseFrame(id: Some("2"), data: Some("bbbbbbbb")))
    let f3 = SseEncoder.encode(SseFrame(id: Some("3"), data: Some("cccccccc")))
    let budget = f2.byteLength + f3.byteLength
    let replay = MemoryReplayBuffer(maxFrames: 10, maxBytes: budget)
    replay.store(f1, "1")
    replay.store(f2, "2")
    replay.store(f3, "3")

    @Assert(replay.frameCount, 2)
    @Assert(replay.byteCount <= budget)
    @Assert(replayThrowsGap(replay, "1"))
    let frames = replay.replay("2", ReplayGapPolicy.Reject)
    @Assert(frames.size, 1)
    @Assert(String.fromUtf8(frames[0].toBytes()), "id:3\ndata:cccccccc\n\n")
}

private class ReplayAttachGateProvider <: ReplayProvider {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) { mutex.condition() }
    private var replayEntered = false
    private var released = false
    private let replayFrame = SseEncoder.encode(SseFrame(id: Some("2"), data: Some("replay")))

    public func store(_frame: EncodedSseFrame, _id: String): Unit {
        let _ = _frame
        let _ = _id
    }

    public func replay(_afterId: String, _gapPolicy: ReplayGapPolicy): Array<EncodedSseFrame> {
        let _ = _afterId
        let _ = _gapPolicy
        synchronized(mutex) {
            replayEntered = true
            condition.notifyAll()
            while (!released) { condition.wait() }
        }
        [replayFrame]
    }

    func awaitReplayEntered(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!replayEntered) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                condition.wait(timeout: remaining)
            }
            replayEntered
        }
    }

    func releaseReplay(): Unit {
        synchronized(mutex) {
            released = true
            condition.notifyAll()
        }
    }
}

@Test
func concurrentAttachAndLiveBroadcastCannotOvertakeReplayBatch() {
    let provider = ReplayAttachGateProvider()
    let hub = SseHub(replay: Some<ReplayProvider>(provider))
    let connection = SseConnection(
        "race-replay-live",
        config: SseConnectionConfig(maxQueuedFrames: 8, maxQueuedBytes: 4096)
    )

    let attached = AtomicBool(false)
    let attachTask: Future<Unit> = spawn {
        => hub.attach(connection, lastEventId: "1", gapPolicy: ReplayGapPolicy.Reject)
        attached.store(true)
    }
    @Assert(provider.awaitReplayEntered())

    // attach currently owns the Hub ordering lock while replay() is blocked.
    // This broadcast must wait and may only enqueue after replay is installed.
    let broadcastDone = AtomicBool(false)
    let broadcastTask: Future<Unit> = spawn {
        => let stats = hub.broadcast(SseFrame(id: Some("3"), data: Some("live")))
        @Assert(stats.accepted, 1)
        broadcastDone.store(true)
    }

    provider.releaseReplay()
    attachTask.get()
    broadcastTask.get()
    @Assert(attached.load())
    @Assert(broadcastDone.load())

    let writer = ReplayRecordingWriter()
    connection.start(writer)
    connection.closeGracefully(1000)
    @Assert(connection.awaitClosed(1000))
    let frames = writer.frames()
    @Assert(frames.size, 2)
    @Assert(frames[0], "id:2\ndata:replay\n\n")
    @Assert(frames[1], "id:3\ndata:live\n\n")
}
