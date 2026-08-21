package sse4cj

import std.collection.ArrayList

private class DifferentialSink <: SseDecodeSink {
    let events = ArrayList<SseEvent>()
    let ids = ArrayList<String>()
    let retries = ArrayList<Int64>()

    public func onEvent(event: SseEvent): Unit {
        events.add(event)
    }
    public func onLastEventIdChanged(id: String): Unit {
        ids.add(id)
    }
    public func onRetryChanged(retryMillis: Int64): Unit {
        retries.add(retryMillis)
    }
}

private func diffUtf8(text: String): Array<Byte> {
    let result = Array<Byte>(text.size, repeat: 0)
    unsafe { text.rawData().copyTo(result, 0, 0, text.size) }
    result
}

private func diffSlice(bytes: Array<Byte>, start: Int64, end: Int64): Array<Byte> {
    bytes[start..end].clone()
}

private func diffDecodeAll(text: String, chunkSize!: Int64 = 4096): DifferentialSink {
    let bytes = diffUtf8(text)
    return decodeBytes(bytes, chunkSize: chunkSize)
}

// Correctness-first test oracle. This intentionally consumes a complete valid
// UTF-8 byte array and does not share parsing code or mutable parser state with
// SseDecoder. It exists only for differential tests.
class ReferenceSseDecoder {
    private var dataLines = ArrayList<String>()
    private var eventType: String = ""
    private var pendingId: String = ""
    private var hasPendingId: Bool = false
    private var currentId: String = ""

    func decode(bytes: Array<Byte>, sink: SseDecodeSink): Unit {
        var start: Int64 = 0
        if (bytes.size >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF) {
            start = 3
        }

        var lineStart = start
        var index = start
        while (index < bytes.size) {
            let byte = bytes[index]
            if (byte == b'\r') {
                processLine(bytes, lineStart, index, sink)
                if (index + 1 < bytes.size && bytes[index + 1] == b'\n') {
                    index++
                }
                index++
                lineStart = index
                continue
            }
            if (byte == b'\n') {
                processLine(bytes, lineStart, index, sink)
                index++
                lineStart = index
                continue
            }
            index++
        }
        // Deliberately ignore a residual unterminated line: EOF is not a blank
        // line and cannot dispatch an incomplete event block.
    }

    private func processLine(bytes: Array<Byte>, start: Int64, end: Int64, sink: SseDecodeSink): Unit {
        if (start == end) {
            dispatch(sink)
            return
        }
        if (bytes[start] == b':') {
            return
        }

        var colon: Int64 = -1
        for (index in start..end) {
            if (bytes[index] == b':') {
                colon = index
                break
            }
        }
        let nameEnd = if (colon < 0) {
            end
        } else {
            colon
        }
        var valueStart = if (colon < 0) {
            end
        } else {
            colon + 1
        }
        if (valueStart < end && bytes[valueStart] == b' ') {
            valueStart++
        }
        let name = decodeValidUtf8(bytes, start, nameEnd)

        if (name == "data") {
            dataLines.add(decodeValidUtf8(bytes, valueStart, end))
            return
        }
        if (name == "event") {
            eventType = decodeValidUtf8(bytes, valueStart, end)
            return
        }
        if (name == "id") {
            var hasNul = false
            for (index in valueStart..end) {
                if (bytes[index] == 0) {
                    hasNul = true
                    break
                }
            }
            if (!hasNul) {
                pendingId = decodeValidUtf8(bytes, valueStart, end)
                hasPendingId = true
            }
            return
        }
        if (name == "retry") {
            match (parseRetry(bytes, valueStart, end)) {
                case Some(value) => sink.onRetryChanged(value)
                case None => ()
            }
        }
    }

    private func dispatch(sink: SseDecodeSink): Unit {
        if (hasPendingId) {
            if (pendingId != currentId) {
                currentId = pendingId
                sink.onLastEventIdChanged(currentId)
            } else {
                currentId = pendingId
            }
        }
        if (dataLines.size > 0) {
            var data = ""
            for (index in 0..dataLines.size) {
                if (index > 0) {
                    data += "\n"
                }
                data += dataLines[index]
            }
            let resolvedType = if (eventType.size == 0) {
                "message"
            } else {
                eventType
            }
            sink.onEvent(SseEvent(resolvedType, data, currentId))
        }
        dataLines = ArrayList<String>()
        eventType = ""
        pendingId = ""
        hasPendingId = false
    }

    private func parseRetry(bytes: Array<Byte>, start: Int64, end: Int64): Option<Int64> {
        if (start == end) {
            return None<Int64>
        }
        var value: Int64 = 0
        for (index in start..end) {
            let byte = bytes[index]
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

    private func decodeValidUtf8(bytes: Array<Byte>, start: Int64, end: Int64): String {
        if (start == end) {
            return ""
        }
        String.fromUtf8(bytes[start..end].clone())
    }
}

private func decodeBytes(bytes: Array<Byte>, chunkSize!: Int64 = 4096): DifferentialSink {
    let sink = DifferentialSink()
    let decoder = SseDecoder()
    var start: Int64 = 0
    while (start < bytes.size) {
        let end = if (start + chunkSize < bytes.size) {
            start + chunkSize
        } else {
            bytes.size
        }
        decoder.feed(diffSlice(bytes, start, end), sink)
        start = end
    }
    decoder.finish(sink)
    sink
}

private func decodeReference(bytes: Array<Byte>): DifferentialSink {
    let sink = DifferentialSink()
    ReferenceSseDecoder().decode(bytes, sink)
    sink
}

private func assertSameDecode(actual: DifferentialSink, expected: DifferentialSink): Unit {
    @Assert(actual.events.size, expected.events.size)
    @Assert(actual.ids.size, expected.ids.size)
    @Assert(actual.retries.size, expected.retries.size)
    for (index in 0..actual.events.size) {
        @Assert(actual.events[index].eventType, expected.events[index].eventType)
        @Assert(actual.events[index].data, expected.events[index].data)
        @Assert(actual.events[index].lastEventId, expected.events[index].lastEventId)
    }
    for (index in 0..actual.ids.size) {
        @Assert(actual.ids[index], expected.ids[index])
    }
    for (index in 0..actual.retries.size) {
        @Assert(actual.retries[index], expected.retries[index])
    }
}

@Test
func decoderDifferentialAgainstIndependentReference() {
    let fixtures: Array<String> = [
        "data:a\n\n",
        "data:a\r\r",
        "data:a\r\n\r\n",
        "id:7\nevent:tick\ndata:one\ndata: two\nretry:12\n\n",
        ":comment\nunknown:x\ndata:\ndata:\n\n",
        "id:\n\ndata:x\n\n",
        "event:\ndata:x\n\n",
        "data:中文🙂\r\ndata:尾行\r\n\r\n"
    ]
    for (fixture in fixtures) {
        let bytes = diffUtf8(fixture)
        let expected = decodeReference(bytes)
        let chunkSizes: Array<Int64> = [1, 2, 3, 7, 8, 31, 64, 255, 1024, 4096]
        for (chunkSize in chunkSizes) {
            assertSameDecode(decodeBytes(bytes, chunkSize: chunkSize), expected)
        }
    }
}

@Test
func decoderAllSingleSplitPointsMatchReference() {
    let bytes = diffUtf8("\u{FEFF}id:abc\r\nevent:t\ndata:中文🙂\rdata:two\nretry:9\r\n\r\n")
    let expected = decodeReference(bytes)
    for (split in 0..(bytes.size + 1)) {
        let sink = DifferentialSink()
        let decoder = SseDecoder()
        decoder.feed(diffSlice(bytes, 0, split), sink)
        decoder.feed(diffSlice(bytes, split, bytes.size), sink)
        decoder.finish(sink)
        assertSameDecode(sink, expected)
    }
}

@Test
func decoderIdContainingNulIsIgnored() {
    let bytes: Array<Byte> = [
        b'i',
        b'd',
        b':',
        b'o',
        b'l',
        b'd',
        b'\n',
        b'\n',
        b'i',
        b'd',
        b':',
        b'b',
        b'a',
        b'd',
        0,
        b'i',
        b'd',
        b'\n',
        b'\n',
        b'd',
        b'a',
        b't',
        b'a',
        b':',
        b'x',
        b'\n',
        b'\n'
    ]
    let sink = decodeBytes(bytes, chunkSize: 1)
    @Assert(sink.ids.size, 1)
    @Assert(sink.ids[0], "old")
    @Assert(sink.events[0].lastEventId, "old")
}

@Test
func decoderMultipleEmptyDataLinesArePreserved() {
    let sink = diffDecodeAll("data:\ndata:\ndata:x\ndata:\n\n", chunkSize: 1)
    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].data, "\n\nx\n")
}

@Test
func decoderFullWidthRetryDigitsAreIgnored() {
    let sink = diffDecodeAll("retry:１２３\nretry:7\n\n", chunkSize: 1)
    @Assert(sink.retries.size, 1)
    @Assert(sink.retries[0], 7)
}

private func assertDecodeLimitKind(input: String, limits: SseDecoderLimits, expectedKind: SseErrorKind): Unit {
    let sink = DifferentialSink()
    let decoder = SseDecoder(limits: limits)
    var matched = false
    try {
        decoder.feed(diffUtf8(input), sink)
    } catch (e: SseException) {
        matched = match (expectedKind) {
            case SseErrorKind.LineTooLarge => match (e.kind) {
                case SseErrorKind.LineTooLarge => true;
                case _ => false
            }
            case SseErrorKind.FieldTooLarge => match (e.kind) {
                case SseErrorKind.FieldTooLarge => true;
                case _ => false
            }
            case SseErrorKind.DataTooLarge => match (e.kind) {
                case SseErrorKind.DataTooLarge => true;
                case _ => false
            }
            case SseErrorKind.EventTooLarge => match (e.kind) {
                case SseErrorKind.EventTooLarge => true;
                case _ => false
            }
            case SseErrorKind.BufferLimitExceeded => match (e.kind) {
                case SseErrorKind.BufferLimitExceeded => true;
                case _ => false
            }
            case _ => false
        }
    }
    @Assert(matched)
}

@Test
func decoderResourceLimitsAreTyped() {
    assertDecodeLimitKind(
        "12345",
        SseDecoderLimits(maxLineBytes: 4, maxFieldBytes: 4, maxDataBytes: 8, maxEventBytes: 16, maxBufferedBytes: 32),
        SseErrorKind.LineTooLarge
    )
    assertDecodeLimitKind(
        "data:12345\n",
        SseDecoderLimits(maxLineBytes: 16, maxFieldBytes: 4, maxDataBytes: 8, maxEventBytes: 16, maxBufferedBytes: 32),
        SseErrorKind.FieldTooLarge
    )
    assertDecodeLimitKind(
        "data:1234\ndata:5678\n",
        SseDecoderLimits(maxLineBytes: 16, maxFieldBytes: 8, maxDataBytes: 8, maxEventBytes: 16, maxBufferedBytes: 32),
        SseErrorKind.DataTooLarge
    )
    assertDecodeLimitKind(
        "event:12345678\nid:12345678\n",
        SseDecoderLimits(maxLineBytes: 16, maxFieldBytes: 8, maxDataBytes: 8, maxEventBytes: 12, maxBufferedBytes: 32),
        SseErrorKind.EventTooLarge
    )
    assertDecodeLimitKind(
        "id:12345678\n\ndata:x",
        SseDecoderLimits(maxLineBytes: 16, maxFieldBytes: 8, maxDataBytes: 8, maxEventBytes: 8, maxBufferedBytes: 12),
        SseErrorKind.BufferLimitExceeded
    )
}

private class DeterministicRng {
    private var state: UInt64

    init(seed: UInt64) {
        state = seed
    }

    func next(): UInt64 {
        // Keep arithmetic below UInt64.Max because Cangjie integer arithmetic is
        // checked instead of implicitly wrapping.
        state = (state % 2147483648u64) * 1103515245u64 + 12345u64
        state %= 2147483648u64
        state
    }

    func bounded(bound: Int64): Int64 {
        Int64(next() % UInt64(bound))
    }
}

private func randomAscii(rng: DeterministicRng, maxLength: Int64): String {
    let length = rng.bounded(maxLength + 1)
    var value = ""
    for (_ in 0..length) {
        value += "${Rune(UInt32(b'a') + UInt32(rng.bounded(26)))}"
    }
    value
}

@Test
func encoderDecoderFixedSeedPropertyAndDifferential() {
    let rng = DeterministicRng(0x5EED_C0DEu64)
    for (_ in 0..200) {
        let id = randomAscii(rng, 12)
        let eventType = randomAscii(rng, 10)
        let lineCount = rng.bounded(5) + 1
        var data = ""
        for (lineIndex in 0..lineCount) {
            if (lineIndex > 0) {
                match (rng.bounded(3)) {
                    case 0 => data += "\n"
                    case 1 => data += "\r"
                    case _ => data += "\r\n"
                }
            }
            data += randomAscii(rng, 24)
        }
        let retry = rng.bounded(100000)
        let encoded = SseEncoder.encode(
            SseFrame(
                eventType: Some(eventType),
                id: Some(id),
                retryMillis: Some(retry),
                data: Some(data)
            )
        ).toBytes()
        let expected = decodeReference(encoded)
        let chunkSize = rng.bounded(17) + 1
        let actual = decodeBytes(encoded, chunkSize: chunkSize)
        assertSameDecode(actual, expected)
        @Assert(actual.events.size, 1)
        @Assert(actual.events[0].eventType, if (eventType.size == 0) { "message" } else { eventType })
        @Assert(actual.events[0].lastEventId, id)
    }
}
