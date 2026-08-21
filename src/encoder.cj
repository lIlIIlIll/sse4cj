package sse4cj

public class EncodedSseFrame {
    private let encoded: Array<Byte>

    init(encoded: Array<Byte>) {
        this.encoded = encoded
    }

    public prop byteLength: Int64 {
        get() {
            encoded.size
        }
    }

    // Zero-copy access for the writer loop. The returned array must not be mutated.
    public func sharedBytes(): Array<Byte> {
        encoded
    }

    public func toBytes(): Array<Byte> {
        encoded.clone()
    }
}

public class SseEncoder {
    public static func encode(frame: SseFrame): EncodedSseFrame {
        validateFrame(frame)
        let out = SseByteBuffer(capacity: estimateCapacity(frame))

        match (frame.eventType) {
            case Some(value) =>
                out.appendString("event:")
                out.appendString(value)
                out.append(b'\n')
            case None => ()
        }
        match (frame.id) {
            case Some(value) =>
                out.appendString("id:")
                out.appendString(value)
                out.append(b'\n')
            case None => ()
        }
        match (frame.retryMillis) {
            case Some(value) =>
                out.appendString("retry:")
                out.appendString("${value}")
                out.append(b'\n')
            case None => ()
        }
        match (frame.data) {
            case Some(value) => writeLogicalLines(out, "data:", value)
            case None => ()
        }
        match (frame.comment) {
            case Some(value) => writeLogicalLines(out, ":", value)
            case None => ()
        }
        out.append(b'\n')
        EncodedSseFrame(out.toArray())
    }

    private static func validateFrame(frame: SseFrame): Unit {
        match (frame.eventType) {
            case Some(value) =>
                if (containsCrOrLf(value)) {
                    throw SseException(SseErrorKind.InvalidOutboundField, "eventType must not contain CR or LF")
                }
            case None => ()
        }
        match (frame.id) {
            case Some(value) =>
                if (containsCrLfOrNul(value)) {
                    throw SseException(SseErrorKind.InvalidOutboundField, "id must not contain CR, LF, or NUL")
                }
            case None => ()
        }
        match (frame.retryMillis) {
            case Some(value) =>
                if (value < 0) {
                    throw SseException(SseErrorKind.InvalidOutboundField, "retryMillis must be non-negative")
                }
            case None => ()
        }
    }

    private static func containsCrOrLf(value: String): Bool {
        unsafe {
            let bytes = value.rawData()
            for (index in 0..bytes.size) {
                if (bytes[index] == b'\r' || bytes[index] == b'\n') {
                    return true
                }
            }
        }
        false
    }

    private static func containsCrLfOrNul(value: String): Bool {
        unsafe {
            let bytes = value.rawData()
            for (index in 0..bytes.size) {
                if (bytes[index] == b'\r' || bytes[index] == b'\n' || bytes[index] == 0) {
                    return true
                }
            }
        }
        false
    }

    private static func estimateCapacity(frame: SseFrame): Int64 {
        var size: Int64 = 32
        match (frame.eventType) {
            case Some(v) => size += v.size + 7
            case None => ()
        }
        match (frame.id) {
            case Some(v) => size += v.size + 4
            case None => ()
        }
        match (frame.retryMillis) {
            case Some(_) => size += 32
            case None => ()
        }
        match (frame.data) {
            case Some(v) => size += v.size + 16
            case None => ()
        }
        match (frame.comment) {
            case Some(v) => size += v.size + 8
            case None => ()
        }
        size
    }

    private static func writeLogicalLines(out: SseByteBuffer, prefix: String, value: String): Unit {
        var start: Int64 = 0
        var index: Int64 = 0
        unsafe {
            let bytes = value.rawData()
            while (index < bytes.size) {
                let current = bytes[index]
                if (current == b'\r' || current == b'\n') {
                    out.appendString(prefix)
                    out.appendStringRange(value, start, index - start)
                    out.append(b'\n')
                    if (current == b'\r' && index + 1 < bytes.size && bytes[index + 1] == b'\n') {
                        index++
                    }
                    index++
                    start = index
                    continue
                }
                index++
            }
        }
        out.appendString(prefix)
        out.appendStringRange(value, start, value.size - start)
        out.append(b'\n')
    }
}
