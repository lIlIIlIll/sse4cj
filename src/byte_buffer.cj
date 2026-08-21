package sse4cj

private const SSE_INITIAL_BUFFER_CAPACITY: Int64 = 128

class SseByteBuffer {
    private var buffer: Array<Byte>
    private var used: Int64 = 0

    init(capacity!: Int64 = SSE_INITIAL_BUFFER_CAPACITY) {
        let initial = if (capacity > 0) {
            capacity
        } else {
            1
        }
        buffer = Array<Byte>(initial, repeat: 0)
    }

    prop size: Int64 {
        get() {
            used
        }
    }

    func clear(): Unit {
        used = 0
    }

    func append(value: Byte): Unit {
        ensureCapacity(used + 1)
        buffer[used] = value
        used++
    }

    func appendRangeFrom(source: SseByteBuffer, start: Int64, length: Int64): Unit {
        if (length <= 0) {
            return
        }
        ensureCapacity(used + length)
        source.buffer.copyTo(buffer, start, used, length)
        used += length
    }

    func appendString(value: String): Unit {
        if (value.size == 0) {
            return
        }
        ensureCapacity(used + value.size)
        unsafe {
            value.rawData().copyTo(buffer, 0, used, value.size)
        }
        used += value.size
    }

    func appendStringRange(value: String, start: Int64, length: Int64): Unit {
        if (length <= 0) {
            return
        }
        ensureCapacity(used + length)
        unsafe {
            value.rawData().copyTo(buffer, start, used, length)
        }
        used += length
    }

    func get(index: Int64): Byte {
        buffer[index]
    }

    func toArray(): Array<Byte> {
        buffer[0..used].clone()
    }

    func toStringReplacingInvalidUtf8(): String {
        decodeUtf8ReplacingRange(buffer, 0, used)
    }

    func decodeRange(start: Int64, length: Int64): String {
        decodeUtf8ReplacingRange(buffer, start, length)
    }

    func contains(value: Byte, start: Int64, length: Int64): Bool {
        for (index in start..(start + length)) {
            if (buffer[index] == value) {
                return true
            }
        }
        false
    }

    func firstIndexOf(value: Byte): Int64 {
        for (index in 0..used) {
            if (buffer[index] == value) {
                return index
            }
        }
        -1
    }

    func equalsAscii(value: String, length: Int64): Bool {
        if (value.size != length) {
            return false
        }
        unsafe {
            let raw = value.rawData()
            for (index in 0..length) {
                if (buffer[index] != raw[index]) {
                    return false
                }
            }
        }
        true
    }

    private func ensureCapacity(required: Int64): Unit {
        if (required <= buffer.size) {
            return
        }
        var next = buffer.size
        while (next < required) {
            if (next > Int64.Max / 2) {
                next = required
                break
            }
            next *= 2
        }
        let grown = Array<Byte>(next, repeat: 0)
        buffer.copyTo(grown, 0, 0, used)
        buffer = grown
    }
}

private func appendReplacement(out: SseByteBuffer): Unit {
    out.append(0xEF)
    out.append(0xBF)
    out.append(0xBD)
}

private func isContinuation(byte: Byte): Bool {
    byte >= 0x80 && byte <= 0xBF
}

func decodeUtf8ReplacingRange(bytes: Array<Byte>, start: Int64, size: Int64): String {
    let out = SseByteBuffer(capacity: size + 8)
    var i: Int64 = start
    let end = start + size
    while (i < end) {
        let b0 = bytes[i]
        if (b0 <= 0x7F) {
            out.append(b0)
            i++
            continue
        }

        if (b0 >= 0xC2 && b0 <= 0xDF && i + 1 < end) {
            let b1 = bytes[i + 1]
            if (isContinuation(b1)) {
                out.append(b0)
                out.append(b1)
                i += 2
                continue
            }
        }

        if (b0 >= 0xE0 && b0 <= 0xEF && i + 2 < end) {
            let b1 = bytes[i + 1]
            let b2 = bytes[i + 2]
            let secondOk = if (b0 == 0xE0) {
                b1 >= 0xA0 && b1 <= 0xBF
            } else if (b0 == 0xED) {
                b1 >= 0x80 && b1 <= 0x9F
            } else {
                isContinuation(b1)
            }
            if (secondOk && isContinuation(b2)) {
                out.append(b0)
                out.append(b1)
                out.append(b2)
                i += 3
                continue
            }
        }

        if (b0 >= 0xF0 && b0 <= 0xF4 && i + 3 < end) {
            let b1 = bytes[i + 1]
            let b2 = bytes[i + 2]
            let b3 = bytes[i + 3]
            let secondOk = if (b0 == 0xF0) {
                b1 >= 0x90 && b1 <= 0xBF
            } else if (b0 == 0xF4) {
                b1 >= 0x80 && b1 <= 0x8F
            } else {
                isContinuation(b1)
            }
            if (secondOk && isContinuation(b2) && isContinuation(b3)) {
                out.append(b0)
                out.append(b1)
                out.append(b2)
                out.append(b3)
                i += 4
                continue
            }
        }

        appendReplacement(out)
        i++
    }
    String.fromUtf8(out.toArray())
}
