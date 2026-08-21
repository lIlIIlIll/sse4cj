package sse4cj

import std.collection.ArrayList

private class CollectingSink <: SseDecodeSink {
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

private func utf8(text: String): Array<Byte> {
    let result = Array<Byte>(text.size, repeat: 0)
    unsafe {
        text.rawData().copyTo(result, 0, 0, text.size)
    }
    result
}

private func sliceBytes(bytes: Array<Byte>, start: Int64, end: Int64): Array<Byte> {
    bytes[start..end].clone()
}

private func decodeAll(text: String, chunkSize!: Int64 = 4096): CollectingSink {
    let sink = CollectingSink()
    let decoder = SseDecoder()
    let bytes = utf8(text)
    var start: Int64 = 0
    while (start < bytes.size) {
        let end = if (start + chunkSize < bytes.size) {
            start + chunkSize
        } else {
            bytes.size
        }
        decoder.feed(sliceBytes(bytes, start, end), sink)
        start = end
    }
    decoder.finish(sink)
    sink
}

@Test
func decoderSimpleLf() {
    let sink = decodeAll("data: hello\n\n")
    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].eventType, "message")
    @Assert(sink.events[0].data, "hello")
    @Assert(sink.events[0].lastEventId, "")
}

@Test
func decoderCrAndCrLfEquivalent() {
    let cr = decodeAll("data: a\r\r", chunkSize: 1)
    let crlf = decodeAll("data: a\r\n\r\n", chunkSize: 1)
    @Assert(cr.events.size, 1)
    @Assert(crlf.events.size, 1)
    @Assert(cr.events[0].data, "a")
    @Assert(crlf.events[0].data, "a")
}

@Test
func decoderMixedLineEndingsAndMultilineData() {
    let sink = decodeAll("event:custom\rdata: one\r\ndata:two\n\r", chunkSize: 1)
    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].eventType, "custom")
    @Assert(sink.events[0].data, "one\ntwo")
}

@Test
func decoderIdRetryAndIdOnlyBlock() {
    let sink = decodeAll("id: 42\nretry: 1500\n\ndata: x\n\n")
    @Assert(sink.ids.size, 1)
    @Assert(sink.ids[0], "42")
    @Assert(sink.retries.size, 1)
    @Assert(sink.retries[0], 1500)
    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].lastEventId, "42")
}

@Test
func decoderEmptyIdResetsState() {
    let sink = decodeAll("id: old\n\ndata:a\n\nid:\n\ndata:b\n\n")
    @Assert(sink.events.size, 2)
    @Assert(sink.events[0].lastEventId, "old")
    @Assert(sink.events[1].lastEventId, "")
    @Assert(sink.ids.size, 2)
    @Assert(sink.ids[1], "")
}

@Test
func decoderFieldsAreExactAndCommentsIgnored() {
    let sink = decodeAll(":comment\nEvent: wrong\ndatabase: nope\ndata: ok\n\n")
    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].eventType, "message")
    @Assert(sink.events[0].data, "ok")
}

@Test
func decoderFieldWithoutColonAndSingleSpaceRule() {
    let empty = decodeAll("data\n\n")
    @Assert(empty.events.size, 1)
    @Assert(empty.events[0].data, "")

    let spaces = decodeAll("data:  x\n\n")
    @Assert(spaces.events[0].data, " x")
}

@Test
func decoderRetryAcceptsOnlyAsciiDigits() {
    let sink = decodeAll("retry:0\nretry:-1\nretry:1.5\nretry:12 3\nretry:9223372036854775808\nretry:123\n\n")
    @Assert(sink.retries.size, 2)
    @Assert(sink.retries[0], 0)
    @Assert(sink.retries[1], 123)
}

@Test
func decoderEofDoesNotDispatchIncompleteEvent() {
    let sink = decodeAll("data: incomplete")
    @Assert(sink.events.size, 0)
}

@Test
func decoderUtf8BomAndChunkBoundaries() {
    let payload = "\u{FEFF}data: 中文🙂\n\n"
    let sink = decodeAll(payload, chunkSize: 1)
    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].data, "中文🙂")
}

@Test
func decoderLaterBomIsOrdinaryData() {
    let sink = decodeAll("data: a\u{FEFF}b\n\n", chunkSize: 1)
    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].data, "a\u{FEFF}b")
}

@Test
func decoderInvalidUtf8UsesDeterministicReplacement() {
    let bytes: Array<Byte> = [b'd', b'a', b't', b'a', b':', 0xC0, 0xAF, b'\n', b'\n']
    let sink = CollectingSink()
    let decoder = SseDecoder()
    for (byte in bytes) {
        decoder.feed([byte], sink)
    }
    decoder.finish(sink)
    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].data, "\u{FFFD}\u{FFFD}")
}

@Test
func decoderChunkSizeInvariant() {
    let input = "id:abc\r\nevent:tick\ndata:中文\rdata: two\nretry:9\r\n\r\n"
    let expected = decodeAll(input, chunkSize: 4096)
    let sizes: Array<Int64> = [1, 2, 3, 7, 8, 31, 64, 255, 1024, 4096]
    for (size in sizes) {
        let actual = decodeAll(input, chunkSize: size)
        @Assert(actual.events.size, expected.events.size)
        @Assert(actual.events[0].eventType, expected.events[0].eventType)
        @Assert(actual.events[0].data, expected.events[0].data)
        @Assert(actual.events[0].lastEventId, expected.events[0].lastEventId)
        @Assert(actual.retries[0], expected.retries[0])
    }
}

@Test
func decoderRangeFeedConsumesOnlyRequestedBytes() {
    let sink = CollectingSink()
    let decoder = SseDecoder()
    let bytes = utf8("ignoredata: ok\n\nignore")
    decoder.feed(bytes, 6, 10, sink)
    decoder.finish(sink)

    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].data, "ok")
}

private func assertInvalidDecoderFeedRange(bytes: Array<Byte>, start: Int64, count: Int64): Unit {
    let decoder = SseDecoder()
    let sink = CollectingSink()
    var caught = false
    try {
        decoder.feed(bytes, start, count, sink)
    } catch (e: SseException) {
        caught = match (e.kind) {
            case SseErrorKind.InvalidConfiguration => true
            case _ => false
        }
    }
    @Assert(caught)
}

@Test
func decoderRangeFeedRejectsInvalidBoundsWithoutOverflow() {
    let bytes: Array<Byte> = [b'a', b'b', b'c']
    assertInvalidDecoderFeedRange(bytes, -1, 1)
    assertInvalidDecoderFeedRange(bytes, 0, -1)
    assertInvalidDecoderFeedRange(bytes, 4, 0)
    assertInvalidDecoderFeedRange(bytes, 2, 2)
    assertInvalidDecoderFeedRange(bytes, Int64.Max, 1)
    assertInvalidDecoderFeedRange(bytes, 1, Int64.Max)
}

@Test
func decoderLineLimitFailsAndResetRecovers() {
    let limits = SseDecoderLimits(
        maxLineBytes: 4,
        maxFieldBytes: 4,
        maxDataBytes: 8,
        maxEventBytes: 16,
        maxBufferedBytes: 32
    )
    let decoder = SseDecoder(limits: limits)
    let sink = CollectingSink()
    var caught = false
    try {
        decoder.feed(utf8("12345"), sink)
    } catch (e: SseException) {
        caught = match (e.kind) {
            case SseErrorKind.LineTooLarge => true;
            case _ => false
        }
    }
    @Assert(caught)
    decoder.reset()
    decoder.feed(utf8("data\n\n"), sink)
    decoder.finish(sink)
    @Assert(sink.events.size, 1)
}

@Test
func encoderPreservesPresentEmptyAndCanonicalFields() {
    let frame = SseFrame(
        eventType: Some(""),
        id: Some(""),
        retryMillis: Some(0),
        data: Some("")
    )
    let encoded = SseEncoder.encode(frame)
    @Assert(String.fromUtf8(encoded.toBytes()), "event:\nid:\nretry:0\ndata:\n\n")
}

@Test
func encoderNormalizesLogicalNewlinesAndPreservesTrailingEmptyLine() {
    let frame = SseFrame(data: Some("a\r\nb\rc\n"), comment: Some("x\r\ny"))
    let encoded = String.fromUtf8(SseEncoder.encode(frame).toBytes())
    @Assert(encoded, "data:a\ndata:b\ndata:c\ndata:\n:x\n:y\n\n")
}

@Test
func encoderRejectsFieldInjection() {
    var eventCaught = false
    try {
        SseEncoder.encode(SseFrame(eventType: Some("x\ny")))
    } catch (e: SseException) {
        eventCaught = match (e.kind) {
            case SseErrorKind.InvalidOutboundField => true;
            case _ => false
        }
    }
    @Assert(eventCaught)

    var idCaught = false
    try {
        SseEncoder.encode(SseFrame(id: Some("x\u{0000}y")))
    } catch (e: SseException) {
        idCaught = match (e.kind) {
            case SseErrorKind.InvalidOutboundField => true;
            case _ => false
        }
    }
    @Assert(idCaught)
}

@Test
func encoderDecoderRoundTripData() {
    let encoded = SseEncoder.encode(SseFrame(eventType: Some("tick"), id: Some("7"), data: Some("a\n中文\n")))
    let sink = CollectingSink()
    let decoder = SseDecoder()
    let bytes = encoded.toBytes()
    for (byte in bytes) {
        decoder.feed([byte], sink)
    }
    decoder.finish(sink)
    @Assert(sink.events.size, 1)
    @Assert(sink.events[0].eventType, "tick")
    @Assert(sink.events[0].data, "a\n中文\n")
    @Assert(sink.events[0].lastEventId, "7")
}
