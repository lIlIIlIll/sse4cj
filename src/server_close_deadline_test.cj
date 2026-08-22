package sse4cj

import std.sync.*
import std.time.*

private class DeadlineBlockingWriter <: SseConnectionWriter {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) { mutex.condition() }
    private var entered = false
    private var released = false

    public func write(frame: EncodedSseFrame): Unit {
        let _ = frame.byteLength
        synchronized(mutex) {
            entered = true
            condition.notifyAll()
            while (!released) { condition.wait() }
        }
    }
    public func flush(): Unit {}
    public func close(): Unit { synchronized(mutex) { condition.notifyAll() } }

    func awaitEntered(timeoutMillis!: Int64 = 1000): Bool {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMillis
        synchronized(mutex) {
            while (!entered) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) { break }
                condition.wait(timeout: remaining)
            }
            entered
        }
    }
    func release(): Unit { synchronized(mutex) { released = true; condition.notifyAll() } }
}

private func deadlineStatusIs(value: SseSendStatus, expected: SseSendStatus): Bool {
    match (value) {
        case SseSendStatus.Accepted => match (expected) { case SseSendStatus.Accepted => true; case _ => false }
        case SseSendStatus.Rejected => match (expected) { case SseSendStatus.Rejected => true; case _ => false }
        case SseSendStatus.Dropped => match (expected) { case SseSendStatus.Dropped => true; case _ => false }
        case SseSendStatus.Disconnected => match (expected) { case SseSendStatus.Disconnected => true; case _ => false }
        case SseSendStatus.Closed => match (expected) { case SseSendStatus.Closed => true; case _ => false }
        case SseSendStatus.Failed => match (expected) { case SseSendStatus.Failed => true; case _ => false }
    }
}

@Test
func gracefulDeadlineClosesLogicallyBeforeTransportReturns() {
    let hub = SseHub()
    let connection = SseConnection(
        "graceful-blocked",
        config: SseConnectionConfig(maxQueuedFrames: 2, maxQueuedBytes: 1024)
    )
    hub.attach(connection)
    connection.send(SseFrame(data: Some("first")))
    let writer = DeadlineBlockingWriter()
    connection.start(writer)
    @Assert(writer.awaitEntered())

    connection.closeGracefully(10)
    @Assert(connection.isClosed)
    @Assert(!connection.isTransportClosed)
    @Assert(hub.connectionCount, 0)
    @Assert(connection.bufferedFrameCount, 1)

    writer.release()
    @Assert(connection.awaitClosed(1000))
    @Assert(connection.isTransportClosed)
}

@Test
func blockWithTimeoutCountsInflightFrameAgainstCapacity() {
    let connection = SseConnection(
        "bounded-block",
        config: SseConnectionConfig(
            maxQueuedFrames: 1,
            maxQueuedBytes: 1024,
            backpressure: SseBackpressureStrategy.BlockWithTimeout,
            blockTimeoutMillis: 10
        )
    )
    connection.send(SseFrame(data: Some("first")))
    let writer = DeadlineBlockingWriter()
    connection.start(writer)
    @Assert(writer.awaitEntered())

    let started = MonoTime.now()
    let result = connection.send(SseFrame(data: Some("second")))
    let elapsed = MonoTime.now() - started
    @Assert(deadlineStatusIs(result.status, SseSendStatus.Rejected))
    @Assert(elapsed < Duration.millisecond * 500)
    @Assert(connection.bufferedFrameCount, 1)

    connection.closeNow()
    writer.release()
    @Assert(connection.awaitClosed(1000))
}
