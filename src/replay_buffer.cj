package sse4cj

import std.collection.*
import std.sync.*

private class MemoryReplayEntry {
    let id: String
    let frame: EncodedSseFrame

    init(id: String, frame: EncodedSseFrame) {
        this.id = id
        this.frame = frame
    }
}

/// Bounded in-memory replay storage for SSE frames that carry a non-empty id.
///
/// Storage is bounded by both frame count and encoded byte count. The ring grows
/// only up to maxFrames and evicts oldest entries before admitting a new frame.
public class MemoryReplayBuffer <: ReplayProvider {
    public let maxFrames: Int64
    public let maxBytes: Int64

    private let mutex = Mutex()
    private var entries: Array<Option<MemoryReplayEntry>>
    private var head: Int64 = 0
    private var count: Int64 = 0
    private var retainedBytes: Int64 = 0

    public init(maxFrames!: Int64 = 1024, maxBytes!: Int64 = 16 * 1024 * 1024) {
        if (maxFrames <= 0 || maxFrames > 1_000_000 || maxBytes <= 0 || maxBytes > 2 * 1024 * 1024 * 1024) {
            throw SseException(SseErrorKind.InvalidConfiguration, "replay limits must be finite and positive")
        }
        this.maxFrames = maxFrames
        this.maxBytes = maxBytes
        let initialCapacity = if (maxFrames < 16) { maxFrames } else { 16 }
        entries = Array<Option<MemoryReplayEntry>>(initialCapacity, repeat: None<MemoryReplayEntry>)
    }

    public prop frameCount: Int64 {
        get() { synchronized(mutex) { count } }
    }

    public prop byteCount: Int64 {
        get() { synchronized(mutex) { retainedBytes } }
    }

    /// Stores an already encoded frame under its SSE event id.
    /// Empty ids are not retained because they reset Last-Event-ID and therefore
    /// cannot be used as a future replay cursor. A frame larger than maxBytes is
    /// skipped rather than violating the configured memory bound.
    public func store(frame: EncodedSseFrame, id: String): Unit {
        if (id.size == 0 || frame.byteLength > maxBytes) { return }
        synchronized(mutex) {
            while (count >= maxFrames || frame.byteLength > maxBytes - retainedBytes) {
                if (count == 0) { return }
                retainedBytes -= removeFirstLocked().frame.byteLength
            }
            ensureSpaceLocked()
            let index = (head + count) % entries.size
            entries[index] = Some(MemoryReplayEntry(id, frame))
            count++
            retainedBytes += frame.byteLength
        }
    }

    /// Returns frames strictly after afterId. If the cursor has fallen out of the
    /// retained window, gapPolicy decides whether to start live, reject, or emit
    /// an id reset before switching to live delivery.
    public func replay(afterId: String, gapPolicy: ReplayGapPolicy): Array<EncodedSseFrame> {
        if (afterId.size == 0) { return [] }
        synchronized(mutex) {
            var found: Int64 = -1
            // Prefer the most recent duplicate id. SSE servers should normally
            // issue unique monotonically advancing ids, but this avoids replaying
            // older duplicate-id entries that the client may already have seen.
            for (logicalIndex in 0..count) {
                if (entryAtLocked(logicalIndex).id == afterId) { found = logicalIndex }
            }
            if (found < 0) {
                return gapResult(gapPolicy, afterId)
            }
            let result = ArrayList<EncodedSseFrame>()
            for (logicalIndex in (found + 1)..count) {
                result.add(entryAtLocked(logicalIndex).frame)
            }
            result.toArray()
        }
    }

    private func gapResult(policy: ReplayGapPolicy, afterId: String): Array<EncodedSseFrame> {
        match (policy) {
            case ReplayGapPolicy.StartLive => []
            case ReplayGapPolicy.Reset => [SseEncoder.encode(SseFrame(id: Some("")))]
            case ReplayGapPolicy.Reject => throw SseException(
                SseErrorKind.ReplayGap,
                "Last-Event-ID '${afterId}' is outside the retained replay window"
            )
        }
    }

    private func entryAtLocked(logicalIndex: Int64): MemoryReplayEntry {
        if (logicalIndex < 0 || logicalIndex >= count) {
            throw SseException(SseErrorKind.InternalInvariantViolation, "replay index outside retained range")
        }
        let index = (head + logicalIndex) % entries.size
        match (entries[index]) {
            case Some(value) => value
            case None => throw SseException(SseErrorKind.InternalInvariantViolation, "replay ring slot is empty")
        }
    }

    private func removeFirstLocked(): MemoryReplayEntry {
        if (count == 0) {
            throw SseException(SseErrorKind.InternalInvariantViolation, "replay ring is empty")
        }
        let index = head
        let value = match (entries[index]) {
            case Some(entry) => entry
            case None => throw SseException(SseErrorKind.InternalInvariantViolation, "replay ring slot is empty")
        }
        entries[index] = None<MemoryReplayEntry>
        head = (head + 1) % entries.size
        count--
        if (count == 0) { head = 0 }
        value
    }

    private func ensureSpaceLocked(): Unit {
        if (count < entries.size) { return }
        var nextCapacity = entries.size * 2
        if (nextCapacity > maxFrames) { nextCapacity = maxFrames }
        if (nextCapacity <= entries.size) {
            throw SseException(SseErrorKind.InternalInvariantViolation, "replay ring cannot grow")
        }
        let grown = Array<Option<MemoryReplayEntry>>(nextCapacity, repeat: None<MemoryReplayEntry>)
        for (logicalIndex in 0..count) {
            grown[logicalIndex] = Some(entryAtLocked(logicalIndex))
        }
        entries = grown
        head = 0
    }
}
