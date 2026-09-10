package sse4cj

import std.io.InputStream
import std.sync.*
import std.time.MonoTime
import wirestack.http.*

private enum WirestackSseBudgetPhase {
    Open | Read
}

private enum WirestackSseTerminalReason {
    Cancelled
    | OpenTimeout
    | ReadTimeout
    | Failure
    | Eof
    | Closed
}

private class WirestackSseRequestOwner {
    private let requestHandle = HttpRequestCancellationHandle()
    private let cancellation: Option<SseCancellationToken>
    private let mutex = Mutex()
    private let condition = synchronized(mutex) {
        mutex.condition()
    }
    private var client: Option<HttpClient> = None<HttpClient>
    private var response: Option<HttpResponse> = None<HttpResponse>
    private var interruptId: Int64 = 0
    private var watchdog: Option<Future<Unit>> = None<Future<Unit>>
    private var watchdogStopped = false
    private var nextGeneration: Int64 = 1
    private var activeGeneration: Int64 = 0
    private var activeDeadline: Option<MonoTime> = None<MonoTime>
    private var activePhase: Option<WirestackSseBudgetPhase> = None<WirestackSseBudgetPhase>
    private var timeoutClaim: Option<WirestackSseBudgetPhase> = None<WirestackSseBudgetPhase>
    private var terminalReason: Option<WirestackSseTerminalReason> = None<WirestackSseTerminalReason>
    private var cleanupClaimed = false
    private var eof = false

    init(cancellation: Option<SseCancellationToken>) {
        this.cancellation = cancellation
    }

    func registerInterrupt(): Unit {
        let id = match (cancellation) {
            case Some(token) => token.registerInterrupt({=> let _ = requestHandle.cancel()})
            case None => 0
        }
        synchronized(mutex) {
            interruptId = id
        }
    }

    func attachClient(value: HttpClient): Unit {
        synchronized(mutex) {
            client = Some(value)
        }
    }

    func attachResponse(value: HttpResponse): Unit {
        synchronized(mutex) {
            response = Some(value)
        }
    }

    func startWatchdog(): Unit {
        let worker: Future<Unit> = spawn {=> watchdogLoop()}
        synchronized(mutex) {
            watchdog = Some(worker)
        }
    }

    func armOpen(timeoutMillis: Int64): Int64 {
        arm(WirestackSseBudgetPhase.Open, timeoutMillis, "SSE HTTP request failed")
    }

    private func armRead(timeoutMillis: Int64): Int64 {
        arm(WirestackSseBudgetPhase.Read, timeoutMillis, "SSE HTTP body read failed")
    }

    private func arm(
        phase: WirestackSseBudgetPhase,
        timeoutMillis: Int64,
        failureMessage: String
    ): Int64 {
        synchronized(mutex) {
            if (cleanupClaimed) {
                throw SseException(SseErrorKind.TransportFailure, failureMessage)
            }
            let generation = nextGeneration
            nextGeneration++
            activeGeneration = generation
            activePhase = Some(phase)
            activeDeadline = Some(MonoTime.now() + Duration.millisecond * timeoutMillis)
            condition.notifyAll()
            generation
        }
    }

    func finishBudget(generation: Int64): Unit {
        var cancelNow = false
        synchronized(mutex) {
            if (activeGeneration == generation) {
                let expired = match (activeDeadline) {
                    case Some(value) => MonoTime.now() >= value
                    case None => false
                }
                if (expired) {
                    match (activePhase) {
                        case Some(value) => cancelNow = claimTimeoutLocked(value)
                        case None => ()
                    }
                }
                activeGeneration = 0
                activeDeadline = None<MonoTime>
                activePhase = None<WirestackSseBudgetPhase>
                condition.notifyAll()
            }
        }
        if (cancelNow) {
            let _ = requestHandle.cancel()
        }
    }

    func pendingFailure(fallbackMessage: String): Option<SseException> {
        if (isWirestackTransportCancelled(cancellation)) {
            synchronized(mutex) {
                terminalReason = Some(WirestackSseTerminalReason.Cancelled)
            }
            return Some(SseException(SseErrorKind.Cancelled, "SSE HTTP request cancelled"))
        }
        synchronized(mutex) {
            match (timeoutClaim) {
                case Some(WirestackSseBudgetPhase.Open) => return Some(
                    SseException(
                        SseErrorKind.TransportFailure,
                        "SSE HTTP response opening timed out"
                    )
                )
                case Some(WirestackSseBudgetPhase.Read) => return Some(
                    SseException(
                        SseErrorKind.TransportFailure,
                        "SSE HTTP body read timed out"
                    )
                )
                case None =>
                    if (cleanupClaimed) {
                        return Some(SseException(SseErrorKind.TransportFailure, fallbackMessage))
                    }
            }
            None<SseException>
        }
    }

    func failure(fallbackMessage: String): SseException {
        match (pendingFailure(fallbackMessage)) {
            case Some(value) => value
            case None =>
                claimTerminal(WirestackSseTerminalReason.Failure)
                SseException(SseErrorKind.TransportFailure, fallbackMessage)
        }
    }

    func read(value: HttpResponse, destination: Array<Byte>, timeoutMillis: Int64): Int64 {
        if (destination.isEmpty()) {
            return 0
        }
        let ended = synchronized(mutex) {
            eof
        }
        if (ended) {
            return 0
        }
        match (pendingFailure("SSE HTTP body read failed")) {
            case Some(error) =>
                let _ = cleanup()
                throw error
            case None => ()
        }

        let generation = armRead(timeoutMillis)
        let count = try {
            value.body.read(destination)
        } catch (_: Exception) {
            finishBudget(generation)
            let mapped = failure("SSE HTTP body read failed")
            let _ = cleanup()
            throw mapped
        }
        finishBudget(generation)
        match (pendingFailure("SSE HTTP body read failed")) {
            case Some(error) =>
                let _ = cleanup()
                throw error
            case None => ()
        }
        if (count == 0) {
            synchronized(mutex) {
                eof = true
                if (terminalReason.isNone()) {
                    terminalReason = Some(WirestackSseTerminalReason.Eof)
                }
            }
            if (cleanup()) {
                throw SseException(SseErrorKind.TransportFailure, "SSE HTTP response close failed")
            }
        }
        count
    }

    func close(): Unit {
        claimTerminal(WirestackSseTerminalReason.Closed)
        if (cleanup()) {
            throw SseException(SseErrorKind.TransportFailure, "SSE HTTP response close failed")
        }
    }

    func cleanup(): Bool {
        var shouldClean = false
        var responseToClose = None<HttpResponse>
        var clientToClose = None<HttpClient>
        var tokenToClear = None<SseCancellationToken>
        var idToClear: Int64 = 0
        var workerToJoin = None<Future<Unit>>
        synchronized(mutex) {
            if (!cleanupClaimed) {
                cleanupClaimed = true
                shouldClean = true
                activeGeneration = 0
                activeDeadline = None<MonoTime>
                activePhase = None<WirestackSseBudgetPhase>
                watchdogStopped = true
                responseToClose = response
                clientToClose = client
                tokenToClear = cancellation
                idToClear = interruptId
                workerToJoin = watchdog
                condition.notifyAll()
            }
        }
        if (!shouldClean) {
            return false
        }

        let _ = requestHandle.cancel()
        var closeFailed = false
        match (responseToClose) {
            case Some(value) => try {
                value.close()
            } catch (_: Exception) {
                closeFailed = true
            }
            case None => ()
        }
        match (clientToClose) {
            case Some(value) => try {
                value.close()
            } catch (_: Exception) {
                closeFailed = true
            }
            case None => ()
        }
        match (tokenToClear) {
            case Some(value) => value.clearInterrupt(idToClear)
            case None => ()
        }
        match (workerToJoin) {
            case Some(value) => try {
                value.get()
            } catch (_: Exception) {
                closeFailed = true
            }
            case None => ()
        }
        closeFailed
    }

    private func claimTerminal(value: WirestackSseTerminalReason): Unit {
        synchronized(mutex) {
            if (terminalReason.isNone()) {
                terminalReason = Some(value)
            }
        }
    }

    private func claimTimeoutLocked(phase: WirestackSseBudgetPhase): Bool {
        if (timeoutClaim.isSome() || cleanupClaimed) {
            return false
        }
        timeoutClaim = Some(phase)
        if (terminalReason.isNone()) {
            terminalReason = Some(match (phase) {
                case WirestackSseBudgetPhase.Open => WirestackSseTerminalReason.OpenTimeout
                case WirestackSseBudgetPhase.Read => WirestackSseTerminalReason.ReadTimeout
            })
        }
        true
    }

    private func watchdogLoop(): Unit {
        while (true) {
            var cancelNow = false
            let stopNow = synchronized(mutex) {
                while (!watchdogStopped && !cancelNow) {
                    match (activeDeadline) {
                        case Some(deadline) =>
                            let remaining = deadline - MonoTime.now()
                            if (remaining <= Duration.Zero) {
                                match (activePhase) {
                                    case Some(value) => cancelNow = claimTimeoutLocked(value)
                                    case None => ()
                                }
                                activeGeneration = 0
                                activeDeadline = None<MonoTime>
                                activePhase = None<WirestackSseBudgetPhase>
                            } else {
                                condition.wait(timeout: remaining)
                            }
                        case None => condition.wait()
                    }
                }
                watchdogStopped
            }
            if (cancelNow) {
                let _ = requestHandle.cancel()
            }
            if (stopNow) {
                return
            }
        }
    }

    func handles(): HttpCancellationHandles {
        HttpCancellationHandles(request: Some(requestHandle))
    }
}

private class WirestackSseBodyInput <: InputStream {
    private let owner: WirestackSseRequestOwner
    private let response: HttpResponse
    private let timeoutMillis: Int64

    init(owner: WirestackSseRequestOwner, response: HttpResponse, timeoutMillis: Int64) {
        this.owner = owner
        this.response = response
        this.timeoutMillis = timeoutMillis
    }

    public func read(destination: Array<Byte>): Int64 {
        owner.read(response, destination, timeoutMillis)
    }
}

private class WirestackSseTransportResponse <: SseTransportResponse {
    private let response: HttpResponse
    private let owner: WirestackSseRequestOwner
    private let responseOrigin: Option<String>
    private let input: InputStream

    init(
        response: HttpResponse,
        owner: WirestackSseRequestOwner,
        origin: Option<String>,
        readTimeoutMillis: Int64
    ) {
        this.response = response
        this.owner = owner
        this.responseOrigin = origin
        input = WirestackSseBodyInput(owner, response, readTimeoutMillis)
    }

    public func status(): UInt16 {
        UInt16(response.status)
    }

    public func header(name: String): Option<String> {
        response.headers.getFirst(name)
    }

    public func body(): InputStream {
        input
    }

    public func origin(): Option<String> {
        responseOrigin
    }

    public func close(): Unit {
        owner.close()
    }
}

public class WirestackSseClientTransport <: SseClientTransport {
    private let tls: Option<HttpClientTlsConfig>
    private let resolver: Option<Resolver>

    public init() {
        tls = None<HttpClientTlsConfig>
        resolver = None<Resolver>
    }

    init(tls: HttpClientTlsConfig, resolver: Resolver) {
        this.tls = Some(tls)
        this.resolver = Some(resolver)
    }

    public func send(
        request: SseHttpRequest,
        cancellation: Option<SseCancellationToken>
    ): SseTransportResponse {
        let owner = WirestackSseRequestOwner(cancellation)
        owner.registerInterrupt()
        match (owner.pendingFailure("SSE HTTP request failed")) {
            case Some(error) =>
                let _ = owner.cleanup()
                throw error
            case None => ()
        }

        try {
            var builder = HttpClient
                .builder()
                .redirectPolicy(HttpRedirectPolicy.noFollow())
                .retryPolicy(HttpRetryPolicy(maximumAttempts: 1))
            match (tls) {
                case Some(value) => builder = builder.tls(value)
                case None => ()
            }
            match (resolver) {
                case Some(value) => builder = builder.resolver(value)
                case None => ()
            }
            let client = builder.build()
            owner.attachClient(client)
            owner.startWatchdog()

            let parsedUrl = HttpUrl.parse(request.url)
            let headers = HttpHeaders.builder()
            for (header in request.headers()) {
                headers.add(header.name, header.value)
            }
            headers.add("Accept", "text/event-stream")
            let outbound = HttpRequest(
                HttpMethod("GET"),
                parsedUrl,
                headers: headers.build(),
                retrySafety: HttpRetrySafety.Disabled
            )

            let generation = owner.armOpen(request.openTimeoutMillis)
            let response = try {
                client.sendControlled(
                    outbound,
                    owner.handles(),
                    context: OperationContext.background()
                )
            } catch (_: Exception) {
                owner.finishBudget(generation)
                throw owner.failure("SSE HTTP request failed")
            }
            owner.attachResponse(response)
            owner.finishBudget(generation)
            match (owner.pendingFailure("SSE HTTP request failed")) {
                case Some(error) =>
                    let _ = owner.cleanup()
                    throw error
                case None => ()
            }
            WirestackSseTransportResponse(
                response,
                owner,
                Some("${parsedUrl.scheme}://${parsedUrl.authority}"),
                request.readTimeoutMillis
            )
        } catch (error: SseException) {
            let _ = owner.cleanup()
            throw error
        } catch (_: Exception) {
            let mapped = owner.failure("SSE HTTP request failed")
            let _ = owner.cleanup()
            throw mapped
        }
    }
}

private func isWirestackTransportCancelled(cancellation: Option<SseCancellationToken>): Bool {
    match (cancellation) {
        case Some(token) => token.isCancelled
        case None => false
    }
}
