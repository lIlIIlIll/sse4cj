package sse4cj

import std.io.InputStream

public class SseHeader {
    public let name: String
    public let value: String

    public init(name: String, value: String) {
        if (!isValidHttpToken(name)) {
            throw SseException(SseErrorKind.InvalidConfiguration, "Invalid HTTP header name")
        }
        if (containsHeaderControl(value)) {
            throw SseException(SseErrorKind.InvalidConfiguration, "HTTP header value must not contain CR, LF, or NUL")
        }
        this.name = name
        this.value = value
    }
}

public class SseHttpRequest {
    public let url: String
    public let connectTimeoutMillis: Int64
    public let readTimeoutMillis: Int64
    private let requestHeaders: Array<SseHeader>

    public init(
        url: String,
        headers!: Array<SseHeader> = [],
        connectTimeoutMillis!: Int64 = 10_000,
        readTimeoutMillis!: Int64 = 120_000
    ) {
        if (url.size == 0) {
            throw SseException(SseErrorKind.InvalidConfiguration, "SSE request URL must not be empty")
        }
        validateTimeout(connectTimeoutMillis, "connectTimeoutMillis")
        validateTimeout(readTimeoutMillis, "readTimeoutMillis")
        this.url = url
        this.requestHeaders = headers.clone()
        this.connectTimeoutMillis = connectTimeoutMillis
        this.readTimeoutMillis = readTimeoutMillis
    }

    public func headers(): Array<SseHeader> {
        requestHeaders.clone()
    }
}

public abstract class SseTransportResponse {
    public func status(): UInt16
    public func header(name: String): Option<String>
    public func body(): InputStream
    public func origin(): Option<String>
    public func close(): Unit
}

public abstract class SseClientTransport {
    public func send(
        request: SseHttpRequest,
        cancellation: Option<SseCancellationToken>
    ): SseTransportResponse
}

func isEventStreamContentType(value: String): Bool {
    var start: Int64 = 0
    var end = value.size
    unsafe {
        let bytes = value.rawData()
        while (start < end && (bytes[start] == b' ' || bytes[start] == b'\t')) {
            start++
        }
        var separator = end
        for (index in start..end) {
            if (bytes[index] == b';') {
                separator = index
                break
            }
        }
        end = separator
        while (end > start && (bytes[end - 1] == b' ' || bytes[end - 1] == b'\t')) {
            end--
        }
        return asciiRangeEqualsIgnoreCase(value, start, end, "text/event-stream")
    }
}

func originFromHttpUrl(url: String): Option<String> {
    unsafe {
        let bytes = url.rawData()
        var colon: Int64 = -1
        for (index in 0..bytes.size) {
            if (bytes[index] == b':') {
                colon = index
                break
            }
        }
        if (colon <= 0 || colon + 2 >= bytes.size || bytes[colon + 1] != b'/' || bytes[colon + 2] != b'/') {
            return None<String>
        }
        if (!asciiRangeEqualsIgnoreCase(url, 0, colon, "http") && !asciiRangeEqualsIgnoreCase(url, 0, colon, "https")) {
            return None<String>
        }
        var authorityEnd = bytes.size
        for (index in (colon + 3)..bytes.size) {
            if (bytes[index] == b'/' || bytes[index] == b'?' || bytes[index] == b'#') {
                authorityEnd = index
                break
            }
        }
        if (authorityEnd == colon + 3) {
            return None<String>
        }
        var authorityStart = colon + 3
        for (index in authorityStart..authorityEnd) {
            if (bytes[index] == b'@') {
                authorityStart = index + 1
            }
        }
        if (authorityStart >= authorityEnd) {
            return None<String>
        }
        let out = SseByteBuffer(capacity: colon + 3 + authorityEnd - authorityStart)
        out.appendStringRange(url, 0, colon + 3)
        out.appendStringRange(url, authorityStart, authorityEnd - authorityStart)
        Some(String.fromUtf8(out.toArray()))
    }
}

private func validateTimeout(value: Int64, name: String): Unit {
    if (value <= 0 || value > 24 * 60 * 60 * 1000) {
        throw SseException(
            SseErrorKind.InvalidConfiguration,
            "${name} must be in 1..86400000 milliseconds"
        )
    }
}

private func isValidHttpToken(value: String): Bool {
    if (value.size == 0) {
        return false
    }
    unsafe {
        let bytes = value.rawData()
        for (byte in bytes) {
            let alphaNumeric = (byte >= b'a' && byte <= b'z') || (byte >= b'A' && byte <= b'Z') || (byte >= b'0' &&
                byte <= b'9')
            let punctuation = byte == b'!' || byte == b'#' || byte == b'$' || byte == b'%' || byte == b'&' || byte ==
                b'\'' || byte == b'*' || byte == b'+' || byte == b'-' || byte == b'.' || byte == b'^' || byte == b'_' ||
                byte == b'`' || byte == b'|' || byte == b'~'
            if (!alphaNumeric && !punctuation) {
                return false
            }
        }
    }
    true
}

private func containsHeaderControl(value: String): Bool {
    unsafe {
        for (byte in value.rawData()) {
            if (byte == b'\r' || byte == b'\n' || byte == 0) {
                return true
            }
        }
    }
    false
}

private func asciiRangeEqualsIgnoreCase(value: String, start: Int64, end: Int64, expected: String): Bool {
    if (end - start != expected.size) {
        return false
    }
    unsafe {
        let left = value.rawData()
        let right = expected.rawData()
        for (index in 0..expected.size) {
            if (asciiLower(left[start + index]) != asciiLower(right[index])) {
                return false
            }
        }
    }
    true
}

private func asciiLower(value: Byte): Byte {
    if (value >= b'A' && value <= b'Z') {
        return value + (b'a' - b'A')
    }
    value
}
