package sse4cj

public class SseDecoder {
    private let limits: SseDecoderLimits
    private let line = SseByteBuffer()
    private let dataBuffer = SseByteBuffer()

    private var eventTypeBuffer: String = ""
    private var pendingLastEventId: String = ""
    private var hasPendingLastEventId: Bool = false
    private var currentLastEventId: String = ""
    private var reconnectionTime: Option<Int64> = None<Int64>

    private var pendingCr: Bool = false
    private var bomCount: Int64 = 0
    private var bomBytes: Array<Byte> = Array<Byte>(3, repeat: 0)
    private var bomResolved: Bool = false
    private var failed: Bool = false
    private var finished: Bool = false

    public init(limits!: SseDecoderLimits = SseDecoderLimits.DEFAULTS) {
        this.limits = limits
    }

    public prop lastEventId: String {
        get() {
            currentLastEventId
        }
    }

    public prop retryMillis: Option<Int64> {
        get() {
            reconnectionTime
        }
    }

    public func feed(bytes: Array<Byte>, sink: SseDecodeSink): Unit {
        feed(bytes, 0, bytes.size, sink)
    }

    public func feed(bytes: Array<Byte>, start: Int64, count: Int64, sink: SseDecodeSink): Unit {
        ensureUsable()
        if (start < 0 || count < 0 || start > bytes.size || count > bytes.size - start) {
            throw SseException(
                SseErrorKind.InvalidConfiguration,
                "SSE decoder feed range must be within the input byte array"
            )
        }

        let end = start + count
        var index = start
        while (index < end) {
            feedInitialByte(bytes[index], sink)
            index++
        }
    }

    public func finish(sink: SseDecodeSink): Unit {
        ensureUsable()
        if (!bomResolved && bomCount > 0) {
            flushBomPrefixAsData(sink)
        }
        if (pendingCr) {
            pendingCr = false
            completeLine(sink)
        }
        // EOF is not a blank-line dispatch. Decode any unfinished line only to
        // apply the documented replacement semantics deterministically, then
        // discard the incomplete event block.
        if (line.size > 0) {
            line.toStringReplacingInvalidUtf8()
        }
        discardIncompleteBlock()
        finished = true
    }

    public func reset(): Unit {
        line.clear()
        dataBuffer.clear()
        eventTypeBuffer = ""
        pendingLastEventId = ""
        hasPendingLastEventId = false
        currentLastEventId = ""
        reconnectionTime = None<Int64>
        pendingCr = false
        bomCount = 0
        bomResolved = false
        failed = false
        finished = false
    }

    private func ensureUsable(): Unit {
        if (failed) {
            throw SseException(SseErrorKind.Closed, "SseDecoder is failed; call reset() before reuse")
        }
        if (finished) {
            throw SseException(SseErrorKind.Closed, "SseDecoder is finished; call reset() before reuse")
        }
    }

    private func feedInitialByte(byte: Byte, sink: SseDecodeSink): Unit {
        if (bomResolved) {
            processByte(byte, sink)
            return
        }

        bomBytes[bomCount] = byte
        bomCount++
        if (bomCount == 1 && bomBytes[0] != 0xEF) {
            flushBomPrefixAsData(sink)
            return
        }
        if (bomCount == 2 && bomBytes[1] != 0xBB) {
            flushBomPrefixAsData(sink)
            return
        }
        if (bomCount == 3) {
            bomResolved = true
            if (!(bomBytes[0] == 0xEF && bomBytes[1] == 0xBB && bomBytes[2] == 0xBF)) {
                for (index in 0..bomCount) {
                    processByte(bomBytes[index], sink)
                }
            }
            bomCount = 0
        }
    }

    private func flushBomPrefixAsData(sink: SseDecodeSink): Unit {
        bomResolved = true
        for (index in 0..bomCount) {
            processByte(bomBytes[index], sink)
        }
        bomCount = 0
    }

    private func processByte(byte: Byte, sink: SseDecodeSink): Unit {
        if (pendingCr) {
            pendingCr = false
            completeLine(sink)
            if (byte == b'\n') {
                return
            }
        }

        if (byte == b'\r') {
            pendingCr = true
            return
        }
        if (byte == b'\n') {
            completeLine(sink)
            return
        }

        if (line.size >= limits.maxLineBytes) {
            fail(SseErrorKind.LineTooLarge, "SSE line exceeds maxLineBytes")
        }
        line.append(byte)
        enforceBufferedLimit()
    }

    private func completeLine(sink: SseDecodeSink): Unit {
        if (line.size == 0) {
            dispatchBlock(sink)
            return
        }
        parseLine(sink)
        line.clear()
    }

    private func parseLine(sink: SseDecodeSink): Unit {
        if (line.get(0) == b':') {
            return
        }

        let colon = line.firstIndexOf(b':')
        let fieldNameLength = if (colon < 0) {
            line.size
        } else {
            colon
        }
        var valueStart = if (colon < 0) {
            line.size
        } else {
            colon + 1
        }
        if (valueStart < line.size && line.get(valueStart) == b' ') {
            valueStart++
        }
        let valueLength = line.size - valueStart

        if (fieldNameLength > limits.maxFieldBytes || valueLength > limits.maxFieldBytes) {
            fail(SseErrorKind.FieldTooLarge, "SSE field exceeds maxFieldBytes")
        }

        if (line.equalsAscii("data", fieldNameLength)) {
            acceptData(valueStart, valueLength)
            return
        }
        if (line.equalsAscii("event", fieldNameLength)) {
            eventTypeBuffer = line.decodeRange(valueStart, valueLength)
            enforceEventLimit()
            enforceBufferedLimit()
            return
        }
        if (line.equalsAscii("id", fieldNameLength)) {
            if (!line.contains(0, valueStart, valueLength)) {
                pendingLastEventId = line.decodeRange(valueStart, valueLength)
                hasPendingLastEventId = true
                enforceEventLimit()
                enforceBufferedLimit()
            }
            return
        }
        if (line.equalsAscii("retry", fieldNameLength)) {
            let parsed = parseRetry(valueStart, valueLength)
            match (parsed) {
                case Some(value) =>
                    reconnectionTime = Some(value)
                    sink.onRetryChanged(value)
                case None => ()
            }
            return
        }
        // Unknown fields are ignored exactly.
    }

    private func acceptData(start: Int64, length: Int64): Unit {
        if (length > limits.maxDataBytes - dataBuffer.size - 1) {
            fail(SseErrorKind.DataTooLarge, "SSE data exceeds maxDataBytes")
        }
        dataBuffer.appendRangeFrom(line, start, length)
        dataBuffer.append(b'\n')
        enforceEventLimit()
        enforceBufferedLimit()
    }

    private func parseRetry(start: Int64, length: Int64): Option<Int64> {
        if (length == 0) {
            return None<Int64>
        }
        var value: Int64 = 0
        for (index in start..(start + length)) {
            let byte = line.get(index)
            if (byte < b'0' || byte > b'9') {
                return None<Int64>
            }
            let digit = Int64(byte - b'0')
            if (value > (Int64.Max - digit) / 10) {
                return None<Int64>
            }
            value = value * 10 + digit
        }
        Some(value)
    }

    private func dispatchBlock(sink: SseDecodeSink): Unit {
        if (hasPendingLastEventId) {
            if (pendingLastEventId != currentLastEventId) {
                currentLastEventId = pendingLastEventId
                sink.onLastEventIdChanged(currentLastEventId)
            } else {
                currentLastEventId = pendingLastEventId
            }
        }

        if (dataBuffer.size > 0) {
            let data = dataBuffer.decodeRange(0, dataBuffer.size - 1)
            let eventType = if (eventTypeBuffer.size == 0) {
                "message"
            } else {
                eventTypeBuffer
            }
            sink.onEvent(SseEvent(eventType, data, currentLastEventId))
        }

        dataBuffer.clear()
        eventTypeBuffer = ""
        pendingLastEventId = ""
        hasPendingLastEventId = false
        line.clear()
    }

    private func discardIncompleteBlock(): Unit {
        line.clear()
        dataBuffer.clear()
        eventTypeBuffer = ""
        pendingLastEventId = ""
        hasPendingLastEventId = false
    }

    private func enforceEventLimit(): Unit {
        var remaining = limits.maxEventBytes
        remaining = consumeLimit(remaining, dataBuffer.size, SseErrorKind.EventTooLarge,
            "SSE event exceeds maxEventBytes")
        remaining = consumeLimit(remaining, eventTypeBuffer.size, SseErrorKind.EventTooLarge,
            "SSE event exceeds maxEventBytes")
        if (hasPendingLastEventId) {
            consumeLimit(remaining, pendingLastEventId.size, SseErrorKind.EventTooLarge,
                "SSE event exceeds maxEventBytes")
        }
    }

    private func enforceBufferedLimit(): Unit {
        var remaining = limits.maxBufferedBytes
        remaining = consumeLimit(remaining, line.size, SseErrorKind.BufferLimitExceeded,
            "SSE decoder exceeds maxBufferedBytes")
        remaining = consumeLimit(remaining, dataBuffer.size, SseErrorKind.BufferLimitExceeded,
            "SSE decoder exceeds maxBufferedBytes")
        remaining = consumeLimit(remaining, eventTypeBuffer.size, SseErrorKind.BufferLimitExceeded,
            "SSE decoder exceeds maxBufferedBytes")
        remaining = consumeLimit(remaining, currentLastEventId.size, SseErrorKind.BufferLimitExceeded,
            "SSE decoder exceeds maxBufferedBytes")
        if (hasPendingLastEventId) {
            remaining = consumeLimit(remaining, pendingLastEventId.size, SseErrorKind.BufferLimitExceeded,
                "SSE decoder exceeds maxBufferedBytes")
        }
        consumeLimit(remaining, bomCount, SseErrorKind.BufferLimitExceeded, "SSE decoder exceeds maxBufferedBytes")
    }

    private func consumeLimit(remaining: Int64, amount: Int64, kind: SseErrorKind, message: String): Int64 {
        if (amount < 0 || amount > remaining) {
            fail(kind, message)
        }
        remaining - amount
    }

    private func fail(kind: SseErrorKind, message: String): Unit {
        failed = true
        throw SseException(kind, message)
    }
}
