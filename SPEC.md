# 从零实现完整、高性能的仓颉 SSE 库

你当前需要创建一个全新的仓颉 SSE 项目。项目暂定名：

```text
sse4cj

```

本任务是 **greenfield implementation**，不是重构任何现有仓库。

必须遵守：

- 不修改 `eventsource4cj`。
- 不复制 `eventsource4cj` 的代码、类型设计或错误语义。
- 不提供旧 API 兼容层。
- 不以现有仓库作为实现基础。
- 允许查看仓颉 SDK、stdx 源码、官方文档和网络协议规范。
- 新库的 API、状态机、错误模型、并发模型和测试体系都从零设计。
- 不考虑向后兼容，优先保证规范正确性、资源安全和性能。
- 不要只输出设计文档或实施计划，必须直接创建代码、测试、示例、基准和文档。
- 不要在阶段之间等待确认。
- 遇到非关键设计选择时，自行选择更安全、更有界、更容易验证的方案。

---

## 一、创建独立项目

首先检查当前工作目录。

### 情况 A：当前目录是空的新仓库

直接在当前目录创建项目。

### 情况 B：当前目录包含已有项目

不要修改已有项目。在当前 workspace 下创建独立目录：

```text
sse4cj/

```

所有新增文件必须位于这个独立项目内。

创建有效的仓颉项目配置，使用当前环境实际安装的：

```text
cjc
cjpm
stdx

```

不要预设旧版仓颉 API。开始前执行并记录：

```bash
pwd
git status --short
cjc -v
cjpm -v
echo "$CANGJIE_HOME"
echo "$CANGJIE_STDX_PATH"

```

检查实际可用的：

- HTTP Client API；
- HTTP Server API；
- response streaming API；
- flush 行为；
- socket 和 cancellation API；
- Future、spawn、Mutex、Condition、Atomic；
- 并发队列；
- monotonic clock；
- Timer；
- UTF-8 编解码 API。

如果文档与编译行为冲突，以当前 SDK 实际可编译行为为准。

---

## 二、项目目标

实现一个生产级仓颉 SSE 库，包含四个相互独立的核心能力：

```text
1. SSE wire codec
2. 有限 SSE 响应读取器
3. 自动重连 EventSource 客户端
4. SSE 服务端及广播 Hub

```

必须严格区分：

```text
SseDecoder / SseEncoder
    只处理 SSE wire format。

SseStreamReader
    读取一次有限的 HTTP SSE 响应。
    EOF 表示正常结束。
    不自动重连。
    适合 LLM streaming API。

EventSourceClient
    实现长期 EventSource 连接。
    EOF 和可恢复网络错误后自动重连。
    支持 Last-Event-ID、retry、204 停止和 close。

SseServer / SseHub
    管理服务端连接、单播、广播、心跳、背压、重放和关闭。

```

不能使用一个类型同时模糊表达有限流和自动重连 EventSource。

---

## 三、建议的项目结构

根据仓颉实际包规则调整目录，但职责必须保持分离：

```text
sse4cj/
├── cjpm.toml
├── README.md
├── AGENTS.md
├── CHANGELOG.md
├── src/
│   ├── package.cj
│   ├── wire/
│   │   ├── event.cj
│   │   ├── frame.cj
│   │   ├── decoder.cj
│   │   ├── encoder.cj
│   │   ├── utf8_decoder.cj
│   │   ├── limits.cj
│   │   └── error.cj
│   ├── client/
│   │   ├── stream_reader.cj
│   │   ├── event_source.cj
│   │   ├── event_source_config.cj
│   │   ├── reconnect_policy.cj
│   │   ├── dispatcher.cj
│   │   └── client_transport.cj
│   ├── server/
│   │   ├── server.cj
│   │   ├── endpoint.cj
│   │   ├── hub.cj
│   │   ├── connection.cj
│   │   ├── backpressure.cj
│   │   ├── replay.cj
│   │   └── server_transport.cj
│   └── observability/
│       └── metrics.cj
├── test/
│   ├── wire/
│   ├── client/
│   ├── server/
│   ├── integration/
│   ├── concurrency/
│   └── property/
├── benchmark/
│   ├── decoder_bench.cj
│   ├── encoder_bench.cj
│   ├── fanout_bench.cj
│   ├── loopback_bench.cj
│   └── README.md
├── examples/
│   ├── finite_stream_client/
│   ├── event_source_client/
│   ├── broadcast_server/
│   └── replay_server/
└── doc/
    ├── design.md
    ├── protocol-compliance.md
    ├── performance.md
    └── security.md

```

如仓颉包系统不允许这种布局，调整物理文件位置，但不要重新混合模块职责。

---

## 四、核心公开 API

下面是固定语义，不是必须逐字照抄的仓颉语法。最终 API 必须使用当前 SDK 中真实可编译的类型。

### 1. 入站事件

设计外部不可变的事件类型：

```text
SseEvent
- eventType: String
- data: String
- lastEventId: String
- origin: Option<String>

```

要求：

- 没有 `event` 字段时，`eventType` 为 `"message"`。
- 多个 `data:` 行合并为使用 `\n` 分隔的字符串。
- 不暴露内部可变集合。
- 不把 `retry` 当作业务事件字段。
- `origin` 由客户端层填写，纯 decoder 可为空。

### 2. 出站事件帧

设计外部不可变的：

```text
SseFrame
- eventType: Option<String>
- id: Option<String>
- retryMillis: Option<Int64>
- data: Option<String>
- comment: Option<String>

```

必须区分以下状态：

```text
字段缺失
字段存在但值为空
字段存在且有内容

```

尤其：

```text
id 缺失
!=
id:

```

因为 `id:` 会重置 Last-Event-ID。

同样：

```text
没有 data 字段
!=
data:

```

后者表示一个空字符串事件。

### 3. Decoder 输出

Decoder 除了业务事件，还必须能报告控制状态变化。设计类似：

```text
SseDecodeSink
- onEvent(event: SseEvent)
- onLastEventIdChanged(id: String)
- onRetryChanged(retryMillis: Int64)

```

也可以使用等价的枚举输出：

```text
SseDecodeItem
- Event
- LastEventIdChanged
- RetryChanged

```

不能因为一个只包含 `id:` 的事件块没有 data，就丢失 ID 状态更新。

### 4. 错误模型

定义明确的错误类型，至少覆盖：

```text
InvalidUtf8
InvalidOutboundField
LineTooLarge
EventTooLarge
DataTooLarge
BufferLimitExceeded
InvalidConfiguration
HttpResponseRejected
TransportFailure
Cancelled
Closed
ReconnectExhausted
SlowConsumer
ReplayGap
InternalInvariantViolation

```

库内部禁止默认调用：

```text
printStackTrace()
println()

```

日志通过可注入 logger、diagnostic sink 或 metrics hook 输出。

---

## 五、从零实现增量 SSE Decoder

不得使用以下实现方式：

```text
把全部输入转换成 String
String.lines()
split(":")
startsWith("data")
寻找 "\n\n"

```

实现真正的增量状态机。

### 1. 输入要求

Decoder 必须支持：

- 任意大小字节块；
- 单字节输入；
- 一个 UTF-8 字符跨多个 `feed()`；
- `CRLF` 的 CR 和 LF 跨两个 `feed()`；
- 空行跨多个 `feed()`；
- 一个字段名跨多个 `feed()`；
- 一个字段值跨多个 `feed()`；
- 一个事件跨任意数量的 `feed()`。

提供类似接口：

```text
feed(bytes, sink)
finish(sink)
reset()

```

优先使用 sink/callback，避免每个 chunk 创建临时事件数组。

### 2. UTF-8

必须满足：

- SSE 输入按 UTF-8 解码；
- 仅忽略整个流开头的一个 UTF-8 BOM；
- 后续 BOM 作为普通字符；
- 支持 UTF-8 序列跨 chunk；
- 无效 UTF-8 使用确定且文档化的 replacement 语义；
- 不 panic；
- 不越界；
- 不把半个 UTF-8 字符提前转换成字符串。

如果标准库 UTF-8 API不能完成增量 replacement 解码，则实现独立的小型增量 UTF-8 decoder，并对其做完整测试。

### 3. 换行

以下三种换行必须等价：

```text
LF
CR
CRLF

```

不能只识别：

```text
\n\n

```

必须正确处理：

```text
CR 与 LF 分属两个 chunk
连续 CR
连续 LF
CR 后直接 EOF
混合 CR、LF、CRLF

```

### 4. 行解析

对于每一行：

```text
如果包含冒号：
    fieldName = 第一个冒号之前
    fieldValue = 第一个冒号之后
    如果 fieldValue 的第一个字符是一个空格，仅移除这一个空格

如果不包含冒号：
    fieldName = 整行
    fieldValue = 空字符串

```

字段名精确匹配且区分大小写：

```text
event
data
id
retry

```

以下都不是合法字段名：

```text
Event
EVENT
database
eventual
identifier
retry-after

```

未知字段直接忽略。

以冒号开头的行是 comment，不能分发成业务事件。

### 5. 字段状态机

维护至少以下状态：

```text
dataBuffer
eventTypeBuffer
lastEventIdBuffer
currentLastEventId
reconnectionTime

```

处理规则：

#### `data`

```text
dataBuffer += fieldValue
dataBuffer += "\n"

```

#### `event`

覆盖当前 event type buffer。

#### `id`

仅当值不包含 NUL 时覆盖 last event ID buffer。

空字符串是合法值，表示重置。

#### `retry`

仅当值满足以下条件时接受：

```text
非空或允许 0
全部字符均为 ASCII 0-9
能够安全解析为非负 Int64

```

以下必须忽略：

```text
retry:
retry: -1
retry: 1.5
retry: 100ms
retry: 12 3
retry: １２３
retry: 超出 Int64

```

禁止使用 `UInt16`。

### 6. 空行分发规则

遇到空行：

1. 使用 last event ID buffer 更新当前 Last-Event-ID。
2. 即使没有 data，也必须报告 ID 状态变化。
3. 如果 data buffer 为空，不分发业务事件。
4. 如果 data buffer 非空：
   - 删除最后追加的一个 LF；
   - event type 为空时使用 `"message"`；
   - 分发事件；
   - 事件携带当前 Last-Event-ID。
5. 清空：
   - data buffer；
   - event type buffer。
6. 不清空：
   - Last-Event-ID 状态；
   - retry 状态。

### 7. EOF

EOF 不能替代空行。

以下输入不得分发事件：

```text
data: incomplete

```

只有事件以空行结束时才分发。

`finish()` 必须丢弃未终止的事件，但要正确完成 UTF-8 decoder 状态并返回确定错误或 replacement 结果。

### 8. 资源限制

配置至少提供：

```text
maxLineBytes
maxFieldBytes
maxDataBytes
maxEventBytes
maxBufferedBytes

```

默认值必须：

- 有限；
- 合理；
- 文档化；
- 可覆盖。

攻击者持续发送无换行数据时，内存不能无限增长。

长度累加必须防止整数溢出。

达到限制后返回有类型错误，Decoder 的后续可用状态必须明确：

```text
进入 failed 状态
或
调用 reset 后重新使用

```

---

## 六、实现高性能 SSE Encoder

### 1. Canonical 编码

统一输出 UTF-8 和 LF：

```text
event:<value>\n
id:<value>\n
retry:<digits>\n
data:<value>\n
\n

```

不要求冒号后添加空格。

字段是否输出取决于 `Option` 是否存在，不根据字符串是否为空判断。

### 2. data 和 comment 多行处理

输入 data 中的：

```text
CR
LF
CRLF

```

都应视为逻辑换行。

每个逻辑行编码为单独的：

```text
data:<line>\n

```

comment 每行编码为：

```text
:<line>\n

```

必须保留尾部空行语义。

例如：

```text
"a\n"

```

经过 encode 和 decode 后仍应得到：

```text
"a\n"

```

### 3. 注入防护

禁止调用方通过 CR/LF 注入 SSE 字段或事件边界。

规则：

```text
eventType 禁止 CR 和 LF
id 禁止 CR、LF 和 NUL
retry 必须为非负 Int64
data 和 comment 中的换行由 encoder 拆行

```

非法输入返回：

```text
InvalidOutboundField

```

不能静默修改 `eventType` 或 `id`。

### 4. 编码结果复用

实现内部不可变的：

```text
EncodedSseFrame

```

要求：

- frame 只编码一次；
- 广播给多个客户端时共享同一份只读字节；
- 不允许每连接重新创建 StringBuilder；
- 外部无法修改编码字节；
- 能够高效获得 encoded byte length，用于队列 byte limit。

---

## 七、实现有限 SSE 流读取器

实现：

```text
SseStreamReader

```

它用于：

- LLM streaming API；
- 一次性增量响应；
- 服务端完成后正常 EOF。

语义：

```text
EOF = 当前响应正常完成
不重连

```

要求：

- 使用同一个 `SseDecoder`；
- 可以读取已有 response body；
- 也可以通过可注入 client transport 发起 HTTP 请求；
- 支持自定义 header；
- 支持取消；
- 支持读取超时；
- 支持所有 decoder resource limits；
- 支持事件 callback；
- callback 异常处理策略明确；
- EOF 后关闭 response body；
- 所有错误路径关闭资源；
- 不泄漏 Future、socket 或 HTTP response。

提供一个完整的 LLM SSE 示例，但不要绑定任何具体模型供应商。

---

## 八、实现完整 EventSource 客户端

实现：

```text
EventSourceClient

```

### 1. 状态

公开只读状态：

```text
CONNECTING
OPEN
CLOSED

```

状态转换：

```text
start
→ CONNECTING

收到合法 SSE response
→ OPEN

EOF 或可恢复 transport error
→ CONNECTING
→ 等待
→ 重连

close
→ CLOSED

HTTP 204
→ CLOSED

```

### 2. HTTP 请求

请求至少包含：

```http
GET
Accept: text/event-stream

```

当前 Last-Event-ID 非空时：

```http
Last-Event-ID: ...

```

支持配置：

```text
URL
headers
Authorization
connect timeout
read buffer size
decoder limits
reconnect policy
HTTP Client 或 transport
callback dispatcher

```

禁止日志输出：

```text
Authorization
Cookie
Proxy-Authorization
完整敏感 query

```

### 3. HTTP 响应检查

默认严格策略：

```text
200 + MIME essence 为 text/event-stream
    → OPEN

204
    → 永久 CLOSED

EOF
    → 重连

可恢复网络错误
    → 重连

其他状态
    → HttpResponseRejected

无效 Content-Type
    → HttpResponseRejected

```

必须接受：

```text
text/event-stream
text/event-stream; charset=utf-8

```

不能使用整个 header 字符串直接相等比较。

所有响应路径必须关闭 response body。

### 4. 重连

提供：

```text
ReconnectPolicy

```

至少支持：

```text
Fixed
ExponentialBackoff

```

配置：

```text
initialDelay
minDelay
maxDelay
jitter
maxAttempts: Option

```

规则：

- 合法的 `retry:` 更新基础重连延迟；
- 使用单调时钟；
- `close()` 可以立即打断 reconnect sleep；
- 重连时发送当前非空 Last-Event-ID；
- 收到空 `id:` 后不再发送该 header；
- 不产生 reconnect storm；
- 同一客户端同时只能有一个连接尝试；
- 同一客户端同时只能有一个读取循环。

### 5. close

`close()` 必须：

- 幂等；
- 中止当前 HTTP 读取；
- 中止连接建立；
- 中止重连等待；
- 阻止后续回调；
- 阻止后续重连；
- 释放全部资源。

测试 callback 内调用 `close()`。

### 6. 事件分发

提供：

```text
onOpen
onMessage
onEvent
onAnyEvent
onError
onRetry
onClosed

```

语义：

- `"message"` 事件调用 `onMessage`。
- 具名事件只调用对应 handler 和可选 `onAnyEvent`。
- 没有具名 handler 时，不得回退调用 `onMessage`。
- 保持单连接事件顺序。
- 不在内部锁中执行用户 callback。
- callback 抛异常不能破坏 decoder 状态。
- callback 异常策略可配置：
  - Continue；
  - Close；
  - ForwardToErrorHandler。
- 异步 dispatch 队列必须有界。

---

## 九、实现完整 SSE 服务端

实现：

```text
SseServer
SseEndpoint
SseHub
SseConnection

```

### 0. 传输层约束（stdx HTTP 限制）

经源码验证（stdx `HttpResponseWriter`、`HttpContext`、`HttpEngineConn1`、`HttpEngineConn2`）：

```text
HttpResponseWriter 仅有 write(buf) 一个公开数据 API。
无 close/abort/interrupt/flush/end。
write() 在 synchronized(writerMtx) 中同步阻塞直到底层 socket 写完成。
HttpContext 公开面不含 writerMtx、responded、upgraded 等内部状态。
HttpEngineConn 是 abstract class（非 public），HttpEngineConn1/2 是 class（非 public）。
外部包无法获取底层 socket 句柄或触发连接关闭。
```

writeTimeout 行为（stdx bug，非设计意图）：

```text
HTTP/1.1 writeResponseByWriter:
    首次 flush header 时启动 writeTimer（一次性）。
    后续每次 write() 不重置、不取消、不重启 writeTimer。
    writeTimer 到期后 close()，无论连接是否健康。
    → 会误杀活跃 SSE 长连接。

HTTP/2 writeResponseByWriter:
    无 writeTimer。
    → 完全无写超时兜底。
```

设计约束：

- 不得依赖 writeTimeout 作为慢消费者触发器（会误杀健康连接，HTTP/2 无覆盖）。
- 慢消费者检测和断连决策必须在 write 返回前（enqueue 阶段）完成。
- writer task 阻塞在 write() 中时，Hub 侧标记 dead connection 并停止 enqueue。
- writer task 的协程释放依赖底层 socket 断开（客户端断连或引擎超时），sse4cj 无法主动 kill 协程。
- 必须限制最大并发连接数，防止慢消费者攻击下协程累积。
- 文档必须声明上述限制为已知 stdx 约束，而非 sse4cj 设计缺陷。
- 如果未来 stdx 提供公开的 per-connection abort/close API，应立即采用并移除此限制声明。

### 1. 所有权模型

禁止使用静态全局注册表。

所有权固定为：

```text
SseServer
  owns endpoint registry

SseEndpoint
  owns SseHub

SseHub
  owns connection registry

SseConnection
  owns outbound queue and writer loop

```

两个不同的 Server 即使都有：

```text
/events

```

也必须完全隔离。

### 2. 接入顺序

连接建立流程：

```text
解析请求
→ 解析 Last-Event-ID
→ 鉴权和接入回调
→ Accept 或 Reject
→ Accept 后注册 connection
→ replay
→ live stream
→ finally deregister
→ close

```

禁止：

```text
先注册
再鉴权

```

鉴权失败、接入回调异常、response 初始化失败时，连接不得进入 Hub。

### 3. HTTP 响应

至少设置：

```http
Content-Type: text/event-stream; charset=utf-8
Cache-Control: no-cache, no-transform

```

禁止应用层硬编码：

```http
Transfer-Encoding: chunked
Connection: keep-alive
Keep-Alive: ...
Content-Length: ...

```

HTTP/1.1 和 HTTP/2 framing 由底层 HTTP 实现负责。

可选提供：

```text
X-Accel-Buffering: no

```

但必须显式配置。

检查底层 response writer 是否需要：

- 显式 flush；
- 首个 comment；
- 特定 streaming API。
- stdx `HttpResponseWriter` 无公开 flush API；
- stdx `HttpResponseWriter` 无公开 close/abort API；
- 首次 write 自动触发 header flush（chunked 或 content-length 由 stdx 检查）；
- 后续 write 逐次写入 body，无显式 flush。

通过实际 loopback 测试证明事件会立即逐条发送，而不是响应结束后一次性输出。

### 4. 单写者模型

每个连接只能有一个 writer fiber/task 操作 response writer。

生产者只能将：

```text
EncodedSseFrame

```

提交到该连接的队列。

禁止多个线程直接并发写同一个 HTTP response。

Writer 负责：

```text
顺序写出
flush
heartbeat
write error 处理（write 抛异常后 deregister）
close（response writer 无公开 close，由底层 socket 断开或引擎 writeTimeout 完成）
finally deregister

```

Writer 不可中断性（见 §0 传输层约束）：

```text
stdx HttpResponseWriter.write 不可从外部中断。
一旦 writer task 进入 write 调用，只能等待其完成或抛异常。
Hub 侧通过队列背压在 write 之前决策，不依赖中断进行中的 write。
writer task write 异常后从 Hub deregister 并退出循环。
```

### 5. 有界队列

每连接队列必须同时限制：

```text
maxQueuedFrames
maxQueuedBytes

```

不能只限制消息数量。

默认值必须合理，禁止创建接近十万容量的队列。

### 6. 背压策略

实现：

```text
DisconnectSlowConsumer
DropNewest
DropOldest
RejectSend
BlockWithTimeout

```

默认：

```text
DisconnectSlowConsumer

```

所有背压决策在 enqueue 阶段执行，不在 write 阶段执行。

原因：stdx `HttpResponseWriter.write` 不可中断（见 §0 传输层约束）。
一旦帧进入 write 调用，无法从外部取消该调用。
因此慢消费者检测必须在帧入队前完成。

广播路径必须非阻塞或有明确超时。

一个慢客户端队列满时，不得阻塞其他客户端。

广播返回结构化统计：

```text
accepted
dropped
disconnected
closed
failed

```

writer task 阻塞时的处理：

```text
writer task 阻塞在 write() 中
→ Hub 侧队列积满
→ 后续 send 返回 Disconnected 或 Failed
→ Hub 从 fan-out 列表移除该 connection
→ 其他 connection 不受影响
→ writer task 协程在底层 socket 断开后自然退出
```

协程泄漏防护：

```text
最大并发连接数可配置。
达到上限时拒绝新连接（503）。
定期扫描 dead connection（队列长时间未消费）。
dead connection 标记后停止 enqueue。
```

### 7. 广播性能

要求：

- 每个逻辑事件编码一次；
- 所有客户端共享同一个 `EncodedSseFrame`；
- registry 只在短时间快照或变更时加锁；
- 不持锁执行网络 IO；
- 不持锁执行用户 callback；
- 不持锁执行过滤 predicate；
- 一个客户端失败不影响其他客户端；
- 不使用全局大锁；
- 不为每个客户端复制大型 payload；
- 不在广播调用线程写 socket。

提供：

```text
connection.send(frame)
hub.broadcast(frame)
hub.sendWhere(frame, predicate)

```

### 8. 心跳

配置：

```text
enabled
interval
comment
onlyWhenIdle

```

使用 SSE comment：

```text
:heartbeat


```

要求：

- comment 不产生业务事件；
- 由连接 writer 发送；
- 不破坏业务事件顺序；
- 连接关闭后立即停止；
- Timer/fiber 可回收；
- 不为每条连接留下永久泄漏的 Timer。

### 9. 关闭

实现：

```text
closeNow()
closeGracefully(deadline)

```

优雅关闭：

```text
停止接收新连接
→ endpoint 标记 closing
→ 可选 drain 队列
→ deadline 到达后标记全部 connection 为 dead
→ 停止向所有 connection enqueue
→ 关闭 HTTP server（触发底层 socket 断开）
→ writer task 在 socket 断开后 write 抛异常退出
→ deregister
→ 清空 registry
```

关闭必须幂等。

stdx 限制（见 §0 传输层约束）：

```text
closeNow 无法主动中断正在 write 的 writer task。
closeNow 做的是：停止 enqueue + 关闭 HTTP server。
底层 socket 断开后 write 抛异常，writer task 才能退出。
如果 writer task 阻塞在 write 中且 socket 未断（TCP 窗口为零但连接活跃），
writer task 协程会挂起直到 TCP 超时或客户端断连。
这是 stdx 的限制，不是 sse4cj 的设计选择。
```

---

## 十、实现 Last-Event-ID Replay

提供独立扩展：

```text
ReplayProvider
ReplayResult
ReplayGapPolicy

```

实现一个默认的有界内存版本：

```text
MemoryReplayBuffer

```

限制：

```text
maxFrames
maxBytes

```

支持：

- 保存具有 ID 的事件；
- 根据客户端 Last-Event-ID 找到位置；
- 重放该 ID 之后的事件；
- ID 不存在时执行配置策略；
- 重放完成后无缝进入 live；
- replay 和 live 之间不能丢事件；
- replay 不能晚于后续 live 事件写出；
- replay 同样受到连接 byte/frame 队列限制。

Gap 策略：

```text
StartLive
Reject
Reset

```

文档明确：

```text
SSE + replay 通常只能构建 at-least-once 语义，
不能宣称 exactly-once。

```

---

## 十一、安全要求

必须处理：

```text
event/id CRLF 注入
id 中 NUL
无换行无限流
超长行
超大事件
超大 data
retry 整数溢出
队列按消息数有限但按字节无限
慢客户端阻塞广播
鉴权失败 ghost connection
callback 异常导致未清理
Server 之间全局状态串流
关闭后继续 callback
响应未关闭
重连风暴
日志泄漏敏感 header

```

默认配置必须是：

```text
有限
保守
可预测
可观测

```

---

## 十二、测试体系

不要只写示例或打印输出。所有测试必须有断言和确定退出条件。

### 1. Reference Decoder

先在测试代码中实现一个简单、以正确性优先的 reference decoder。

它可以低性能，但必须易读。

优化 decoder 与 reference decoder 做差分测试。

禁止两个 decoder 共用核心解析代码。

### 2. 协议测试

覆盖：

```text
LF
CR
CRLF
混合换行
UTF-8 BOM
UTF-8 多字节字符
无效 UTF-8
字段无冒号
字段冒号后一个空格
字段冒号后多个空格
未知字段
字段大小写敏感
comment
仅 comment 的事件块
多行 data
空 data
多个空 data
尾随空行
空 id
id 持久化
id 含 NUL
合法 retry
retry=0
非法 retry
超大 retry
EOF 未终止事件
多个连续事件

```

### 3. Chunk 边界测试

每个核心 fixture 使用以下 chunk size：

```text
1
2
3
7
8
31
64
255
1024
4096
完整输入

```

对较短输入枚举所有单切分点。

覆盖：

- UTF-8 字符中间切分；
- CR/LF 中间切分；
- 冒号前后切分；
- 空行中间切分；
- BOM 中间切分。

无论如何切块，结果必须一致。

### 4. Property 测试

使用固定 seed：

- 随机合法 frame；
- encode 后 decode；
- 随机 chunking；
- optimized decoder 与 reference decoder 差分；
- 随机无效字节；
- 随机超限输入。

必须保证：

```text
不 panic
不越界
不无限循环
不无限分配
结果确定

```

### 5. 客户端测试

使用 mock transport 和 loopback HTTP server：

```text
200 建立连接
MIME 带 charset
无效 MIME
204 停止
EOF 重连
网络错误重连
retry 更新延迟
Last-Event-ID header
空 id 重置
close 中止 read
close 中止 sleep
close 幂等
callback 内 close
具名事件不调用 onMessage
callback 异常
maxAttempts
response 必须关闭

```

重连测试使用 fake clock 或 fake sleeper，不要真实等待数秒。

### 6. 服务端测试

覆盖：

```text
两个 Server 相同路径完全隔离
鉴权失败不注册
鉴权异常不注册
response 初始化失败不注册
断开后 deregister
write error 后 deregister
慢客户端不阻塞其他客户端
frame limit
byte limit
所有背压策略
广播部分失败
并发 producer
每连接事件顺序
heartbeat
idle heartbeat
关闭停止 heartbeat
graceful shutdown
shutdown deadline
broadcast 与 shutdown 竞争
send 与 close 竞争
replay
replay gap
replay/live 顺序

```

禁止测试访问公网。

禁止依赖固定公网 IP。

禁止通过任意 sleep 猜测 server 已启动；使用同步信号。

---

## 十三、性能要求

### 1. Decoder

必须：

- O(n)；
- 每个输入字节只处理常数次；
- 不重复扫描累计缓冲区；
- 不按字节分配对象；
- 不使用正则表达式；
- 不为每一行调用通用 `split`；
- 使用可复用缓冲区；
- 在 byte domain 中查找 CR、LF 和冒号；
- 只在字段或事件完成时构造必要字符串；
- resource limit 保证内存上界。

### 2. Encoder

必须：

- 预估容量；
- 避免重复字符串拼接；
- 直接构造 UTF-8 输出；
- frame 只编码一次；
- 广播共享编码结果。

### 3. Server

必须：

- fan-out 不执行 socket write；
- fan-out 非阻塞 enqueue；
- 单客户端故障隔离；
- 无全局大锁；
- 连接 registry 快照时间短；
- 每连接单 writer；
- 队列同时限制 frame 和 byte；
- 大 payload 不进行 N 份复制。

### 4. Client

必须：

- read buffer 可配置；
- decoder 重用；
- 不为每个 read chunk 创建完整字符串；
- 不泄漏连接、Future 或 timer；
- callback dispatcher 有界。

不要在没有 profiler 证据时引入复杂对象池。

---

## 十四、Benchmark

创建可重复执行的 release benchmark。

### 1. Decoder benchmark

事件大小：

```text
32 B
256 B
1 KiB
64 KiB
1 MiB

```

数据类型：

```text
ASCII
中文 UTF-8
多行 data
comment-heavy
id/retry-heavy

```

chunk size：

```text
1
7
64
1024
4096
16384
完整输入

```

报告：

```text
MB/s
events/s
ns/event
median
P95

```

### 2. Encoder benchmark

覆盖：

```text
小事件
多行事件
中文事件
64 KiB data
1 MiB data

```

### 3. Fan-out benchmark

使用内存 fake writer：

```text
1 client
10 clients
100 clients
1000 clients

```

报告：

```text
broadcast/s
deliveries/s
P50/P95/P99 enqueue latency
encoded frame count
dropped/disconnected count

```

必须通过 instrumentation 证明：

```text
广播 N 个客户端时只编码一次。

```

加入一个永久慢消费者，证明它不会阻塞其他连接。

### 4. Loopback benchmark

测试：

```text
单连接持续流
100 连接广播
首事件延迟
P50/P95/P99

```

### 5. Benchmark 规则

- release 优化；
- 预热；
- 多轮；
- 固定输入；
- 记录 SDK 和硬件环境；
- 保存原始 CSV 或 JSON；
- 不使用单次结果；
- 不伪造 allocation 数；
- 使用当前可用 profiler 检查热点；
- 至少完成一次基于 profiler 的优化。

输出：

```text
benchmark/results/*.json
benchmark/README.md
doc/performance.md

```

---

## 十五、文档

### README

必须包含：

- 项目定位；
- 安装方法；
- Decoder/Encoder 示例；
- 有限 SSE 流示例；
- EventSource 客户端示例；
- SSE Server 示例；
- 广播示例；
- Replay 示例；
- 背压说明；
- 取消和关闭；
- 默认资源限制；
- 已验证平台和 SDK；
- 已知限制。

### design.md

说明：

```text
wire 状态机
增量 UTF-8
client 状态机
server ownership
connection lifecycle
single-writer
backpressure
replay/live ordering
cancellation
graceful shutdown
HTTP/1.1 和 HTTP/2
内存上界

```

### protocol-compliance.md

建立：

```text
协议要求
实现文件
测试文件
状态
备注

```

### security.md

说明：

```text
CRLF injection
资源限制
敏感日志
慢消费者
鉴权顺序
重连风暴
replay 风险

```

### AGENTS.md

记录后续维护必须遵守的：

```text
模块边界
测试命令
性能约束
禁止行为
协议事实来源

```

---

## 十六、构建质量

运行当前环境可用的：

```bash
cjpm check
cjpm build
cjpm test
cjpm build --release
cjfmt
cjlint
cjcov

```

具体命令根据实际 SDK 调整。

要求：

- 不通过全局关闭 warning 隐藏问题；
- 无无用 import；
- 无调试 println；
- 无公网依赖；
- 无无限循环测试；
- 无泄漏 Future；
- 无泄漏 Timer；
- 无硬编码端口冲突；
- 测试失败后继续定位和修复；
- `git diff --check` 通过。

如果某工具不存在，记录真实错误，不得伪造通过。

---

## 十七、实施顺序

连续执行，不等待确认：

1. 创建全新仓颉项目。
2. 审计 SDK/stdx API。
3. 确定公开 API 和错误模型。
4. 编写 reference decoder。
5. 编写协议 fixtures。
6. 实现增量 UTF-8 decoder。
7. 实现 SSE decoder。
8. 实现 encoder。
9. 完成全部 wire tests。
10. 实现 `SseStreamReader`。
11. 实现 client transport abstraction。
12. 实现 `EventSourceClient`。
13. 实现 server transport abstraction。
14. 实现 `SseServer`、`SseHub` 和 `SseConnection`。
15. 实现背压。
16. 实现心跳。
17. 实现关闭与取消。
18. 实现 replay。
19. 完成 integration/concurrency/property tests。
20. 建立 benchmark。
21. 运行 profiler 并优化热点。
22. 完成文档。
23. 执行完整质量门槛。
24. 检查最终 diff 和项目边界。

---

## 十八、禁止行为

禁止：

- 修改旧 `eventsource4cj`；
- 复制旧实现；
- 创建兼容旧 API 的 wrapper；
- 使用静态全局连接注册表；
- 使用 `\n\n` 搜索代替协议状态机；
- 将全部响应转换为一个 String；
- 使用 `startsWith("data")` 判断字段；
- 使用 `UInt16` 保存 retry；
- 默认无限重连且无法取消；
- 默认无限 read timeout；
- 无界事件缓冲；
- 无界 callback 队列；
- 只按消息数限制队列而不限制字节；
- 在广播线程写网络；
- 在持锁状态下执行 callback；
- 在鉴权前注册连接；
- 硬编码 `Transfer-Encoding`；
- 硬编码 `Connection: keep-alive`；
- 测试访问公网；
- 只写示例而没有断言；
- 只写计划而不实现；
- 将未测试能力写成已支持。

---

## 十九、完成条件

只有以下全部成立才可以报告 `COMPLETE`：

1. 项目是全新创建的独立仓库或目录。
2. 没有修改或复制旧 `eventsource4cj`。
3. CR、LF、CRLF 全部正确。
4. 任意 chunk boundary 结果一致。
5. UTF-8 和开头 BOM 正确。
6. 字段名精确匹配。
7. 无冒号字段正确。
8. comment 不分发事件。
9. 多行 data 正确。
10. 空 id 正确重置。
11. id 中 NUL 被忽略。
12. retry 只接受 ASCII digits。
13. retry 使用足够大的整数类型。
14. EOF 不分发未终止事件。
15. 有限流 EOF 正常完成。
16. EventSource EOF 自动重连。
17. HTTP 204 永久停止。
18. Content-Type 参数正确解析。
19. 具名事件不回退到 onMessage。
20. close 可中止 read、connect 和 sleep。
21. close 幂等。
22. 服务端不存在静态全局 Hub。
23. 鉴权成功前连接不可见。
24. 每连接最终都 deregister。
25. 每连接单 writer。
26. 队列同时限制 frame 和 byte。
27. 慢客户端不会永久阻塞广播。
28. 广播事件只编码一次。
29. HTTP framing 由底层 transport 管理。
30. graceful shutdown 有测试。
31. replay 和 live 不乱序。
32. 所有核心测试有断言。
33. 所有适用构建和测试通过。
34. 有真实 benchmark 数据。
35. 文档完整。
36. 没有将未验证能力声称为已支持。
37. stdx writeTimeout 限制已在文档中声明（见 §0 传输层约束）。
38. 背压决策在 enqueue 阶段完成，不依赖中断进行中的 write。
39. 最大并发连接数可配置，防止协程累积。
40. writer task 阻塞时 Hub 侧正确标记 dead connection 并从 fan-out 移除。

如果由于当前 stdx API 限制无法完成某项：

- 继续完成其他全部内容；
- 建立 transport abstraction；
- 提供最小复现；
- 精确指出缺少的 API；
- 最终状态使用 `INCOMPLETE`；
- 不得用模拟代码冒充生产实现。

---

## 二十、最终报告

最终回复使用：

```text
# COMPLETE

```

或：

```text
# INCOMPLETE

```

并严格包含：

```text
## A. Project Creation
- 项目路径
- 是否为全新目录
- 是否修改旧仓库
- 初始和最终 git status

## B. Toolchain
- cjc
- cjpm
- stdx
- OS
- 已验证协议

## C. Architecture
- 模块
- 公开 API
- ownership
- client state machine
- server lifecycle
- backpressure
- replay

## D. Protocol Compliance
- 已完成语义
- compliance matrix 路径
- 未完成语义

## E. Tests
- 实际执行命令
- test 数量
- pass/fail
- property tests
- integration tests
- concurrency tests
- 覆盖率

## F. Performance
- benchmark 命令
- decoder
- encoder
- fan-out
- loopback
- profiler 热点
- 优化结果

## G. Resource Guarantees
- line/event/data limits
- queue limits
- cancellation
- slow consumer
- memory bounds
- stdx writeTimeout 限制（不可靠的慢消费者触发器，误杀健康连接）
- writer task 不可中断性（enqueue 阶段背压，无法中断进行中的 write）
- 最大并发连接数限制

## H. Security
- injection
- sensitive logging
- authentication ordering
- reconnect storm
- replay

## I. Files Created
- 每个文件及作用

## J. Remaining Limitations
- stdx `HttpResponseWriter` 无公开 close/abort/interrupt API
- writer task 阻塞在 write 中时无法主动 kill 协程
- writeTimeout 是 stdx bug（一次性定时器，不重置），不可作为可靠触发器
- HTTP/2 路径无 writeTimer 兜底
- 协程释放依赖底层 socket 断开
- 只写真实限制

## K. Verification
- cjpm check
- cjpm build
- cjpm test
- release build
- benchmark
- git diff --check

```

最终报告必须包含真实命令和真实输出摘要，不得只写“已完成”。
