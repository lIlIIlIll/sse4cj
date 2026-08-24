package sse4cj

import std.sync.*
import std.time.*

private class BlockingCapabilityWriter <: SseConnectionWriter {
    private let mutex = Mutex()
    private let condition = synchronized(mutex) { mutex.condition() }
    private let capabilityValue: SseTransportAbortCapability
    private var entered = false
    private var released = false
    var abortRequests: Int64 = 0
    var closes: Int64 = 0

    init(capability: SseTransportAbortCapability) {
        capabilityValue = capability
    }

    public func write(frame: EncodedSseFrame): Unit {
        let _ = frame.byteLength
        synchronized(mutex) {
            entered = true
            condition.notifyAll()
            while (!released) { condition.wait() }
        }
    }

    public func flush(): Unit {}

    public func close(): Unit {
        synchronized(mutex) {
            closes++
            match (capabilityValue) {
                case SseTransportAbortCapability.Immediate =>
                    abortRequests++
                    released = true
                case _ => ()
            }
            condition.notifyAll()
        }
    }

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

    func release(): Unit {
        synchronized(mutex) {
            released = true
            condition.notifyAll()
        }
    }
}

@Test
func capabilityAwareCloseEvictsImmediatelyButRetainsInflightBudget() {
    let hub = SseHub()
    let connection = SseConnection(
        "blocked",
        config: SseConnectionConfig(maxQueuedFrames: 1, maxQueuedBytes: 1024, transportAbortCapability: SseTransportAbortCapability.ServerWideOnly)
    )
    hub.attach(connection)
    @Assert(connection.send(SseFrame(data: Some("first"))).status == SseSendStatus.Accepted)

    let writer = BlockingCapabilityWriter(SseTransportAbortCapability.ServerWideOnly)
    connection.start(writer)
    @Assert(writer.awaitEntered())
    @Assert(connection.queuedFrameCount, 0)
    @Assert(connection.bufferedFrameCount, 1)
    @Assert(connection.inFlightByteCount > 0)

    let overflow = connection.send(SseFrame(data: Some("second")))
    @Assert(overflow.status == SseSendStatus.Disconnected)
    @Assert(connection.isClosed)
    @Assert(!connection.isTransportClosed)
    @Assert(hub.connectionCount, 0)
    @Assert(connection.queuedFrameCount, 0)
    @Assert(connection.bufferedFrameCount, 1)
    @Assert(writer.abortRequests, 0)

    writer.release()
    @Assert(connection.awaitClosed(1000))
    @Assert(connection.isTransportClosed)
    @Assert(connection.bufferedFrameCount, 0)
    @Assert(connection.bufferedByteCount, 0)
}

@Test
func immediateAbortCapabilityIsInvokedOnLogicalClose() {
    let connection = SseConnection("immediate", config: SseConnectionConfig(transportAbortCapability: SseTransportAbortCapability.Immediate))
    connection.send(SseFrame(data: Some("x")))
    let writer = BlockingCapabilityWriter(SseTransportAbortCapability.Immediate)
    connection.start(writer)
    @Assert(writer.awaitEntered())

    connection.closeNow()
    @Assert(connection.isClosed)
    @Assert(writer.abortRequests, 1)
    @Assert(connection.awaitClosed(1000))
    @Assert(connection.isTransportClosed)
}

@Test
func closeNowDropsPendingQueueBeforeWriterStarts() {
    let hub = SseHub()
    let connection = SseConnection(
        "pending",
        config: SseConnectionConfig(maxQueuedFrames: 4, maxQueuedBytes: 1024)
    )
    hub.attach(connection)
    connection.send(SseFrame(data: Some("one")))
    connection.send(SseFrame(data: Some("two")))
    @Assert(connection.bufferedFrameCount, 2)

    connection.closeNow()
    @Assert(hub.connectionCount, 0)
    @Assert(connection.bufferedFrameCount, 0)
    @Assert(connection.bufferedByteCount, 0)
    @Assert(connection.isTransportClosed)
    @Assert(connection.send(SseFrame(data: Some("three"))).status == SseSendStatus.Closed)
}
