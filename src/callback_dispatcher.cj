package sse4cj

import std.sync.*

private class SseCallbackQueue {
    private let maxCapacity: Int64
    private var storage: Array<Option<() -> Unit>>
    private var head: Int64 = 0
    private var count: Int64 = 0

    init(maxCapacity: Int64) {
        this.maxCapacity = maxCapacity
        let initial = if (maxCapacity < 16) {
            maxCapacity
        } else {
            16
        }
        storage = Array<Option<() -> Unit>>(initial, repeat: None<() -> Unit>)
    }

    prop size: Int64 {
        get() {
            count
        }
    }

    func add(action: () -> Unit): Unit {
        if (count >= maxCapacity) {
            throw SseException(SseErrorKind.InternalInvariantViolation, "SSE callback queue capacity exceeded")
        }
        ensureSpace()
        storage[(head + count) % storage.size] = Some(action)
        count++
    }

    func removeFirst(): () -> Unit {
        if (count == 0) {
            throw SseException(SseErrorKind.InternalInvariantViolation, "SSE callback queue is empty")
        }
        let index = head
        let action = match (storage[index]) {
            case Some(value) => value
            case None => throw SseException(SseErrorKind.InternalInvariantViolation, "SSE callback queue slot is empty")
        }
        storage[index] = None<() -> Unit>
        head = (head + 1) % storage.size
        count--
        if (count == 0) {
            head = 0
        }
        action
    }

    func clear(): Unit {
        var remaining = count
        var index = head
        while (remaining > 0) {
            storage[index] = None<() -> Unit>
            index = (index + 1) % storage.size
            remaining--
        }
        head = 0
        count = 0
    }

    private func ensureSpace(): Unit {
        if (count < storage.size) {
            return
        }
        var next = storage.size * 2
        if (next > maxCapacity) {
            next = maxCapacity
        }
        let grown = Array<Option<() -> Unit>>(next, repeat: None<() -> Unit>)
        for (index in 0..count) {
            grown[index] = storage[(head + index) % storage.size]
        }
        storage = grown
        head = 0
    }
}

public abstract class SseCallbackDispatcher {
    // Non-blocking admission. False means the bounded dispatcher is full/closed.
    public func dispatch(action: () -> Unit): Bool
    // Stop accepting and discard work that has not started yet.
    public func closeNow(): Unit
    // Stop accepting, drain already accepted work, and join the worker.
    public func closeGracefully(): Unit
}

public class InlineSseCallbackDispatcher <: SseCallbackDispatcher {
    public init() {}
    public func dispatch(action: () -> Unit): Bool {
        action();
        true
    }
    public func closeNow(): Unit {}
    public func closeGracefully(): Unit {}
}

public class BoundedAsyncSseCallbackDispatcher <: SseCallbackDispatcher {
    public let maxPendingCallbacks: Int64
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private let queue: SseCallbackQueue
    private var accepting = true
    private var discardPending = false
    private var worker: Option<Future<Unit>> = None<Future<Unit>>

    public init(maxPendingCallbacks!: Int64 = 256) {
        if (maxPendingCallbacks <= 0 || maxPendingCallbacks > 1_000_000) {
            throw SseException(SseErrorKind.InvalidConfiguration, "maxPendingCallbacks must be in 1..1000000")
        }
        this.maxPendingCallbacks = maxPendingCallbacks
        this.queue = SseCallbackQueue(maxPendingCallbacks)
        let future: Future<Unit> = spawn {=> runLoop()}
        worker = Some(future)
    }

    public func dispatch(action: () -> Unit): Bool {
        synchronized(mutex) {
            if (!accepting || queue.size >= maxPendingCallbacks) {
                return false
            }
            queue.add(action)
            condition.notify()
            true
        }
    }

    public func closeNow(): Unit {
        synchronized(mutex) {
            accepting = false
            discardPending = true
            queue.clear()
            condition.notifyAll()
        }
    }

    public func closeGracefully(): Unit {
        synchronized(mutex) {
            accepting = false
            condition.notifyAll()
        }
        match (worker) {
            case Some(value) => value.get();
            case None => ()
        }
    }

    private func runLoop(): Unit {
        while (true) {
            let action = synchronized(mutex) {
                while (queue.size == 0 && accepting) {
                    condition.wait()
                }
                if (discardPending || (queue.size == 0 && !accepting)) {
                    None<() -> Unit>
                } else {
                    Some(queue.removeFirst())
                }
            }
            match (action) {
                case Some(value) => try {
                    value()
                } catch (_: Exception) {}
                case None => return
            }
        }
    }
}
