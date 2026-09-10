package sse4cj

import std.collection.*
import std.sync.*
import std.time.*

private class PullCounters {
    let aborts = AtomicInt64(0)
    let releases = AtomicInt64(0)

    func abort(): Unit {
        let _ = aborts.fetchAdd(1)
    }
    func release(): Unit {
        let _ = releases.fetchAdd(1)
    }
}

private class PullSignal {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private var signalled = false

    func signal(): Unit {
        synchronized(mutex) {
            signalled = true
            condition.notifyAll()
        }
    }

    func await(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!signalled) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                condition.wait(timeout: remaining)
            }
            signalled
        }
    }
}

private class PullStartGate {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private var opened = false

    func await(): Unit {
        synchronized(mutex) {
            while (!opened) {
                condition.wait()
            }
        }
    }
    func open(): Unit {
        synchronized(mutex) {
            opened = true
            condition.notifyAll()
        }
    }
}

private class PullStreamHolder {
    private let mutex = Mutex()
    private var value: Option<SseConnectionStream> = None<SseConnectionStream>

    func set(stream: SseConnectionStream): Unit {
        synchronized(mutex) {
            value = Some(stream)
        }
    }
    func get(): Option<SseConnectionStream> {
        synchronized(mutex) {
            value
        }
    }
}

private class PullRaceWriter <: SseConnectionWriter {
    let closes = AtomicInt64(0)
    public func write(frame: EncodedSseFrame): Unit {
        let _ = frame.byteLength
    }
    public func flush(): Unit {}
    public func close(): Unit {
        let _ = closes.fetchAdd(1)
    }
}

private class PullReentrantRelease {
    private let mutex = Mutex()
    private var stream: Option<SseConnectionStream> = None<SseConnectionStream>
    let calls = AtomicInt64(0)

    func install(value: SseConnectionStream): Unit {
        synchronized(mutex) {
            stream = Some(value)
        }
    }
    func release(): Unit {
        let _ = calls.fetchAdd(1)
        match (synchronized(mutex) {
            stream
        }) {
            case Some(value) => value.close()
            case None => ()
        }
    }
}

private func pullCloseReasonIs(connection: SseConnection, expected: SseCloseReason): Bool {
    match (connection.closeReason) {
        case Some(value) => match ((value, expected)) {
            case (SseCloseReason.ExplicitCloseNow, SseCloseReason.ExplicitCloseNow) => true
            case (SseCloseReason.GracefulDrainComplete, SseCloseReason.GracefulDrainComplete) => true
            case (SseCloseReason.GracefulDeadlineExceeded, SseCloseReason.GracefulDeadlineExceeded) => true
            case _ => false
        }
        case None => false
    }
}

@Test
func pullStreamSlicesCanonicalFrameAndEmptyReadDoesNotAcknowledge() {
    let counters = PullCounters()
    let connection = SseConnection("pull-slices")
    let stream = connection.openStream({=> counters.abort()}, {=> counters.release()})
    let frame = SseEncoder.encode(SseFrame(id: Some("1"), data: Some("fragmented")))
    @Assert(connection.sendEncoded(frame).status == SseSendStatus.Accepted)

    let actual = ArrayList<Byte>()
    let expected = frame.toBytes()
    var nextSize: Int64 = 1
    while (actual.size < expected.size) {
        let destination = Array<Byte>(nextSize, repeat: 0)
        let count = stream.read(destination)
        @Assert(count > 0)
        for (index in 0..count) {
            actual.add(destination[index])
        }
        nextSize = if (nextSize == 1) {
            3
        } else {
            1
        }
    }
    @Assert(String.fromUtf8(actual.toArray()), String.fromUtf8(expected))
    @Assert(connection.bufferedFrameCount, 1)
    @Assert(stream.read(Array<Byte>()), 0)
    @Assert(connection.bufferedFrameCount, 1)

    stream.close()
    @Assert(connection.awaitClosed(1000))
    @Assert(connection.bufferedFrameCount, 0)
    @Assert(connection.bufferedByteCount, 0)
    @Assert(counters.releases.load(), 1)
}

@Test
func pullStreamAcknowledgesOnlyWhenProviderRequestsNextSegment() {
    let counters = PullCounters()
    let connection = SseConnection(
        "pull-delayed-ack",
        config: SseConnectionConfig(
            maxQueuedFrames: 1,
            maxQueuedBytes: 1024,
            backpressure: SseBackpressureStrategy.RejectSend
        )
    )
    let stream = connection.openStream({=> counters.abort()}, {=> counters.release()})
    let first = SseEncoder.encode(SseFrame(data: Some("first")))
    @Assert(connection.sendEncoded(first).status == SseSendStatus.Accepted)
    let destination = Array<Byte>(first.byteLength, repeat: 0)
    @Assert(stream.read(destination), first.byteLength)
    @Assert(connection.bufferedFrameCount, 1)
    @Assert(connection.send(SseFrame(data: Some("second"))).status == SseSendStatus.Rejected)

    let graceful: Future<Unit> = spawn {=> connection.closeGracefully(1000)}
    @Assert(stream.read(Array<Byte>(1, repeat: 0)), 0)
    graceful.get(Duration.second)
    @Assert(pullCloseReasonIs(connection, SseCloseReason.GracefulDrainComplete))
    @Assert(counters.aborts.load(), 0)

    stream.close()
    @Assert(connection.awaitClosed(1000))
    @Assert(counters.releases.load(), 1)
}

@Test
func pullStreamCancelWakesWaitingReadButDefersRelease() {
    let counters = PullCounters()
    let connection = SseConnection("pull-waiting")
    let stream = connection.openStream({=> counters.abort()}, {=> counters.release()})
    let entered = PullSignal()
    let result = AtomicInt64(-1)
    let reader: Future<Unit> = spawn {
        =>
            entered.signal()
            result.store(stream.read(Array<Byte>(8, repeat: 0)))
    }
    @Assert(entered.await())

    stream.cancel()
    @Assert(stream.isClosed())
    @Assert(counters.aborts.load(), 1)
    @Assert(!connection.awaitClosed(10))
    @Assert(counters.releases.load(), 0)
    reader.get(Duration.second)
    @Assert(result.load(), 0)

    stream.close()
    @Assert(connection.awaitClosed(1000))
    @Assert(counters.releases.load(), 1)
}

@Test
func pullCloseNowAbortsAndWakesWaitingReadWithoutReleasingBody() {
    let counters = PullCounters()
    let connection = SseConnection("pull-waiting-close-now")
    let stream = connection.openStream({=> counters.abort()}, {=> counters.release()})
    let entered = PullSignal()
    let result = AtomicInt64(-1)
    let reader: Future<Unit> = spawn {
        =>
            entered.signal()
            result.store(stream.read(Array<Byte>(8, repeat: 0)))
    }
    @Assert(entered.await())

    connection.closeNow()
    reader.get(Duration.second)
    @Assert(result.load(), 0)
    @Assert(pullCloseReasonIs(connection, SseCloseReason.ExplicitCloseNow))
    @Assert(counters.aborts.load(), 1)
    @Assert(counters.releases.load(), 0)
    @Assert(!connection.awaitClosed(10))

    stream.close()
    @Assert(connection.awaitClosed(1000))
    @Assert(counters.releases.load(), 1)
}

@Test
func pullStreamCloseTerminatesWaitingReadAndReleasesOnce() {
    let counters = PullCounters()
    let connection = SseConnection("pull-waiting-close")
    let stream = connection.openStream({=> counters.abort()}, {=> counters.release()})
    let entered = PullSignal()
    let result = AtomicInt64(-1)
    let reader: Future<Unit> = spawn {
        =>
            entered.signal()
            result.store(stream.read(Array<Byte>(8, repeat: 0)))
    }
    @Assert(entered.await())

    let closer: Future<Unit> = spawn {=> stream.close()}
    closer.get(Duration.second)
    reader.get(Duration.second)
    @Assert(result.load(), 0)
    @Assert(connection.awaitClosed(1000))
    @Assert(counters.aborts.load(), 1)
    @Assert(counters.releases.load(), 1)
}

@Test
func pullStreamLogicalEvictionDropsPendingButRetainsPartialInflightUntilClose() {
    let counters = PullCounters()
    let hub = SseHub()
    let connection = SseConnection(
        "pull-partial",
        config: SseConnectionConfig(maxQueuedFrames: 4, maxQueuedBytes: 1024)
    )
    hub.attach(connection)
    let stream = connection.openStream({=> counters.abort()}, {=> counters.release()})
    @Assert(connection.send(SseFrame(data: Some("partial"))).status == SseSendStatus.Accepted)
    @Assert(connection.send(SseFrame(data: Some("pending"))).status == SseSendStatus.Accepted)
    @Assert(stream.read(Array<Byte>(1, repeat: 0)), 1)

    connection.closeNow()
    @Assert(stream.isClosed())
    @Assert(hub.connectionCount, 0)
    @Assert(connection.queuedFrameCount, 0)
    @Assert(connection.bufferedFrameCount, 1)
    @Assert(connection.inFlightByteCount > 0)
    @Assert(counters.aborts.load(), 1)

    stream.close()
    @Assert(connection.awaitClosed(1000))
    @Assert(connection.bufferedFrameCount, 0)
    @Assert(connection.bufferedByteCount, 0)
    @Assert(counters.releases.load(), 1)
}

@Test
func pullStreamCloseSettlesAckPendingFrameExactlyOnce() {
    let counters = PullCounters()
    let connection = SseConnection("pull-ack-close")
    let stream = connection.openStream({=> counters.abort()}, {=> counters.release()})
    let frame = SseEncoder.encode(SseFrame(data: Some("complete")))
    @Assert(connection.sendEncoded(frame).status == SseSendStatus.Accepted)
    @Assert(stream.read(Array<Byte>(frame.byteLength, repeat: 0)), frame.byteLength)
    @Assert(connection.bufferedFrameCount, 1)

    stream.close()
    stream.close()
    @Assert(connection.awaitClosed(1000))
    @Assert(connection.bufferedFrameCount, 0)
    @Assert(counters.aborts.load(), 1)
    @Assert(counters.releases.load(), 1)
}

@Test
func pullGracefulDeadlineAbortsAndReleasesOnlyOnBodyClose() {
    let counters = PullCounters()
    let connection = SseConnection("pull-deadline")
    let stream = connection.openStream({=> counters.abort()}, {=> counters.release()})
    @Assert(connection.send(SseFrame(data: Some("blocked"))).status == SseSendStatus.Accepted)

    connection.closeGracefully(10)
    @Assert(stream.isClosed())
    @Assert(pullCloseReasonIs(connection, SseCloseReason.GracefulDeadlineExceeded))
    @Assert(counters.aborts.load(), 1)
    @Assert(counters.releases.load(), 0)
    @Assert(!connection.awaitClosed(10))

    stream.close()
    @Assert(connection.awaitClosed(1000))
    @Assert(counters.releases.load(), 1)
}

@Test
func pullTransportOwnerRaceTransfersExactlyOneOwner() {
    for (iteration in 0..16) {
        let connection = SseConnection("pull-owner-${iteration}")
        let gate = PullStartGate()
        let holder = PullStreamHolder()
        let writer = PullRaceWriter()
        let releases = AtomicInt64(0)

        let pull: Future<Bool> = spawn {
            =>
                gate.await()
                try {
                    holder.set(
                        connection.openStream(
                            {=> connection.closeNow()},
                            {
                                =>
                                let _ = releases.fetchAdd(1);
                                ()
                            }
                        )
                    )
                    true
                } catch (_: SseException) {
                    false
                }
        }
        let push: Future<Bool> = spawn {
            =>
                gate.await()
                try {
                    connection.start(writer);
                    true
                } catch (_: SseException) {
                    false
                }
        }
        let closer: Future<Unit> = spawn {
            =>
            gate.await();
            connection.closeNow()
        }
        gate.open()

        let pullWon = pull.get(Duration.second)
        let pushWon = push.get(Duration.second)
        closer.get(Duration.second)
        @Assert(pullWon != pushWon)
        if (pullWon) {
            match (holder.get()) {
                case Some(stream) => stream.close()
                case None => @Fail("pull owner was not published")
            }
            @Assert(releases.load(), 1)
            @Assert(writer.closes.load(), 0)
        } else {
            @Assert(holder.get().isNone())
            @Assert(releases.load(), 0)
        }
        @Assert(connection.awaitClosed(1000))
    }
}

@Test
func pullStreamReleaseMayReenterCloseWithoutDuplicateCleanup() {
    let counters = PullCounters()
    let reentrant = PullReentrantRelease()
    let connection = SseConnection("pull-reentrant-close")
    let stream = connection.openStream({=> counters.abort()}, {=> reentrant.release()})
    reentrant.install(stream)
    @Assert(connection.send(SseFrame(data: Some("held"))).status == SseSendStatus.Accepted)
    @Assert(stream.read(Array<Byte>(1, repeat: 0)), 1)

    stream.close()
    @Assert(connection.awaitClosed(1000))
    @Assert(reentrant.calls.load(), 1)
    @Assert(counters.aborts.load(), 1)
    @Assert(connection.bufferedFrameCount, 0)
}
