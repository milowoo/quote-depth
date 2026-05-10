# Quote-Depth — 行情深度服务

消费撮合引擎推送的深度（Order Book Depth）信息，在本地缓存 orderbook 全量状态，并通过 WebSocket 推送给下游客户端。

```
撮合引擎 (match)  →  Kafka (depth-updates topic)  →  Quote-Depth  →  WebSocket (/depth)
      PbDepthUpdate (Protobuf)                                │
                                                              ├  DepthCache (per-symbol)
                                                              └  DepthWebSocketHandler (订阅管理)
```

---

## 架构

### 组件

| 模块 | 职责 |
|------|------|
| `DepthUpdateConsumer` | Kafka 消费者，接收 `PbDepthUpdate` protobuf 消息 |
| `DepthService` | 反序列化 → 更新 DepthCache → 广播给订阅者 |
| `DepthCache` | 每个交易对一个，`ConcurrentSkipListMap` 存储 bids/asks，线程安全 |
| `DepthWebSocketHandler` | WebSocket 订阅管理，新连接自动发送 snapshot + 增量 catch-up |
| `KafkaConfig` | Kafka 消费者配置（`latest` offset、3 并发、protobuf bytes） |
| `WebSocketConfig` | 注册 `/depth` 端点 |

### 数据流

```
Kafka (depth-updates topic)
    │
    ▼
DepthUpdateConsumer.onDepthUpdate(byte[] payload)
    │
    ▼
DepthService.processUpdate()
    │  ├─ PbDepthUpdate.parseFrom(payload)    // protobuf 解码
    │  ├─ cache.applySnapshot/applyIncrement()  // 更新 orderbook
    │  └─ DepthMessage                          // 构造广播消息
    │
    ▼
DepthWebSocketHandler.broadcast(message)
    │  ├─ 已订阅该 symbol 的 session → 实时推送
    │  └─ 处于 catch-up 中的 session  → 暂存 buffer，完成后回放
```

### 新连接订阅流程

```
客户端: {"action":"subscribe","symbol":"BTCUSDT"}
    │
    ▼
subscribe(session, "BTCUSDT")
    │
    ├─ synchronized (per-symbol lock) {
    │     snapshot = DepthService.snapshotWhileLocked()  ← 一致快照
    │     catchingUp.add(session)                        ← 标记 catch-up
    │     subscriptions.add(session)                     ← 加入订阅集
    │   }
    │
    ├─ sendToSession(snapshot)                          ← 发送快照（锁外 I/O）
    │     (此时到达的 live 消息被 buffer)
    │
    └─ catchingUp.remove(session)
       flushBuffer(session)                             ← 回放 catch-up 期间的消息
```

**保证**：快照反映的是 lock 释放时的最后一个一致状态。任何在此之后的增量都会被 buffer，在快照发送完成后按序回放。客户端收到的顺序永远是 `Snapshot → Increment(T+1) → Increment(T+2) → ...`。

---

## 多节点部署

本服务设计为**无状态水平扩展**，每个节点独立运行完整的 DepthCache，节点之间无需共享状态。

### 部署拓扑

```
                        ┌──────────────┐
                        │   Kafka      │
                        │ depth-updates│
                        └──────┬───────┘
                  ┌──────────────┼──────────────┐
                  ▼              ▼              ▼
           ┌──────────┐  ┌──────────┐  ┌──────────┐
           │ Node 1   │  │ Node 2   │  │ Node 3   │
           │ :8081    │  │ :8082    │  │ :8083    │
           └────┬─────┘  └────┬─────┘  └────┬─────┘
                │             │             │
                └─────────────┴─────────────┘
                     LB (负载均衡)
                      │
                 WebSocket 客户端
```

### Consumer Group 策略

**每个节点必须使用独立的 consumer group**（通过 `depth.kafka.consumer.group-id` 或环境变量 `$INSTANCE_ID` 区分），否则 Kafka 会将 partition 分散到不同节点，导致每个节点只收到部分 symbol 的数据：

```yaml
depth:
  kafka:
    consumer:
      group-id: quote-depth-group-${INSTANCE_ID:0}
```

```bash
# 启动节点 1
INSTANCE_ID=1 java -jar target/quote-depth.jar

# 启动节点 2
INSTANCE_ID=2 java -jar target/quote-depth.jar
```

启动后每个节点都会消费 **depth-updates topic 的所有 partition**，各自维护一份完整的 DepthCache。

> **为什么不能用共享 group？** Kafka 的 consumer group 机制会将 topic partition 分配给组内成员。如果两个节点使用同一个 group id，Kafka 会将 partition 分散到两个节点，每个节点只消费部分 symbol 的数据，导致客户端随机连到一个节点时可能查不到目标 symbol 的 depth。

### 服务启动与 Warmup

服务启动后**不会立即对外服务**，而是等待 Kafka 消费到首轮 SNAPSHOT 消息后才标记为 READY：

```
启动                         首轮 SNAPSHOT 到达           后续 SNAPSHOT
  │                               │                           │
  ▼                               ▼                           ▼
┌──────┐   health=DOWN        ┌──────┐   health=UP         ┌──────┐
│WARMUP│─────────────────────►│ LIVE │────────────────────►│ LIVE │
└──────┘   WS 拒绝订阅         └──────┘   WS 正常订阅        └──────┘
```

- **Warmup 期间**：`GET /actuator/health` 返回 `{"status":"DOWN","reason":"waiting for first SNAPSHOT from Kafka"}`，WebSocket 订阅返回 `{"error":"service warming up ..."}`
- **Warmup 完成**：首轮 SNAPSHOT 被消费后立即切为 UP，开始正常服务
- **典型等待时间**：≤ 撮合引擎的 snapshot 推送间隔（默认 30 秒）

此机制确保 LB 不会将流量分发到尚未就绪的节点：

```
# 配合 K8s readinessProbe
readinessProbe:
  httpGet:
    path: /actuator/health
    port: 8081
  initialDelaySeconds: 0
  periodSeconds: 5
```

由于撮合引擎定时推送 SNAPSHOT，单节点重启的不可用窗口被严格限制在 snapshot 间隔之内。多节点部署时，滚动重启不会影响整体服务可用性。

### 负载均衡

WebSocket 连接建议使用 **4 层（TCP）负载均衡**，避免 7 层解析增加延迟：

```
# HAProxy 示例
frontend depth-ws
    bind *:8081
    default_backend depth-nodes

backend depth-nodes
    balance roundrobin
    server node1 10.0.0.1:8081 check
    server node2 10.0.0.2:8081 check
    server node3 10.0.0.3:8081 check
```

### 节点扩缩容

- **扩容**：直接启动新节点（独立 group id），无需预热，启动后自动从 Kafka 消费 SNAPSHOT 恢复
- **缩容**：直接停止节点，客户端 WS 连接断开后重连到其他节点即可
- 无状态设计使扩缩容对现有连接无影响（断开连接的客户端由 LB 分发到存活节点自动重建 DepthCache）

## 启动

### 依赖

- JDK 21
- Kafka（至少 `depth-updates` topic 已创建）
- Maven 3.8+

### 配置

```yaml
# application.yml 或环境变量
server.port: 8081

spring.kafka.bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

depth.kafka.topic: ${DEPTH_KAFKA_TOPIC:depth-updates}
depth.kafka.consumer.group-id: quote-depth-group
depth.snapshot.max-levels: 100
```

### 编译 & 启动

```bash
# 编译（含 protobuf）
mvn clean package -DskipTests

# 运行测试
mvn test

# 启动
java -jar target/quote-depth.jar

# 或通过 Maven
mvn spring-boot:run
```

---

## WebSocket API

### 端点

```
ws://<host>:8081/depth
```

### 订阅

支持单个或批量订阅（逗号分隔）：

```json
{"action": "subscribe", "symbols": "BTCUSDT,ETHUSDT,SOLUSDT"}
```

```json
{"action": "subscribe", "symbols": "BTCUSDT"}
```

### 取消订阅

```json
{"action": "unsubscribe", "symbols": "BTCUSDT,ETHUSDT"}
```

### 收到消息格式

```json
{
  "symbol": "BTCUSDT",
  "type": "SNAPSHOT",
  "bids": [["1000000", "150"], ["999900", "200"]],
  "asks": [["1000100", "80"], ["1000200", "120"]],
  "timestamp": 1715234567890,
  "traceId": 10042,
  "bidLevels": 2,
  "askLevels": 2
}
```

| 字段 | 说明 |
|------|------|
| `symbol` | 交易对 |
| `type` | `SNAPSHOT`（全量）或 `INCREMENTAL`（增量） |
| `bids` / `asks` | 价格档位，`[["价格", "数量"], ...]`，bids 降序、asks 升序 |
| `timestamp` | 毫秒时间戳 |
| `traceId` | 单调递增 ID，用于下游去重 / 排序 |
| `bidLevels` / `askLevels` | 当前返回的档位数 |

**增量消息**中，数量为 `"0"` 表示该价格档位已被移除。

---

## 使用示例

### 命令行 (websocat)

```bash
# 连接并订阅 BTCUSDT
websocat ws://localhost:8081/depth
{"action":"subscribe","symbols":"BTCUSDT"}
```

### JavaScript (浏览器)

```javascript
const ws = new WebSocket('ws://localhost:8081/depth');

ws.onopen = () => {
    ws.send(JSON.stringify({action: 'subscribe', symbols: 'BTCUSDT'}));
};

ws.onmessage = (event) => {
    const depth = JSON.parse(event.data);
    console.log(`${depth.type} | bids: ${depth.bidLevels}, asks: ${depth.askLevels}`);
};

ws.onclose = () => console.log('disconnected');
```

### Python

```python
import asyncio
import websockets
import json

async def subscribe():
    async with websockets.connect('ws://localhost:8081/depth') as ws:
        await ws.send(json.dumps({"action": "subscribe", "symbols": "BTCUSDT"}))
        async for msg in ws:
            data = json.loads(msg)
            print(f"{data['type']} | bids={data['bidLevels']} asks={data['askLevels']}")
            if data.get('type') == 'INCREMENTAL':
                for bid in data['bids']:
                    price, qty = bid
                    if qty == "0":
                        print(f"  bid {price} removed")

asyncio.run(subscribe())
```

### Java (Spring WebSocket)

```java
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.socket.TextMessage;

WebSocketClient client = new StandardWebSocketClient();
client.doHandshake(new TextWebSocketHandler() {
    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        session.sendMessage(new TextMessage(
            "{\"action\":\"subscribe\",\"symbols\":\"BTCUSDT\"}"
        ));
    }
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage msg) {
        System.out.println("depth: " + msg.getPayload());
    }
}, "ws://localhost:8081/depth");
```

---

## 协议 (Protobuf)

Kafka 消息使用 protobuf 序列化，定义在 `src/main/proto/matching.proto`：

```protobuf
message PbDepthUpdate {
  string symbol_id = 1;
  string type = 2;               // SNAPSHOT or INCREMENTAL
  repeated PbPriceLevel bids = 3;
  repeated PbPriceLevel asks = 4;
  int64 timestamp = 5;
  int64 trace_id = 6;            // monotonically increasing
}

message PbPriceLevel {
  string price = 1;
  string quantity = 2;
  int32 order_count = 3;
}
```

---

## 测试

```bash
mvn test
```

现有 14 个单元测试覆盖 DepthCache：快照/增量、价格排序、档位限制、traceId 记录、并发安全。

---

## 监控 & 可观测性

本服务集成 **Micrometer** 指标库，通过 Spring Boot Actuator 暴露度量数据，支持 Prometheus 拉取。

### 端点

| 路径 | 说明 |
|------|------|
| `GET /actuator/health` | 健康检查（含 warmup 状态） |
| `GET /actuator/metrics` | 查看所有可用指标 |
| `GET /actuator/prometheus` | Prometheus 格式指标（需配置 scrape） |

### 指标清单

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `depth.updates` | Counter | `symbol`, `type` | 已处理的 depth 更新总数，按交易对和类型（SNAPSHOT / INCREMENTAL）区分 |
| `depth.update.latency` | Timer | `symbol` | 从 Kafka 消息到达到广播完成的端到端处理延迟，含 P50/P95/P99 百分位 |
| `depth.cache.bid.levels` | Gauge | `symbol` | 当前缓存中买盘价格档位数 |
| `depth.cache.ask.levels` | Gauge | `symbol` | 当前缓存中卖盘价格档位数 |
| `depth.subscribers` | Gauge | `symbol` | 当前订阅该交易对的 WebSocket 连接数 |
| `depth.symbols.cached` | Gauge | 无 | 有活跃 depth 缓存的交易对总数 |

### Prometheus 配置

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'quote-depth'
    scrape_interval: 10s
    metrics_path: /actuator/prometheus
    static_configs:
      - targets:
          - 'node1:8081'
          - 'node2:8081'
          - 'node3:8081'
```

### Grafana 面板建议

以下 PromQL 查询可用于构建实时监控面板：

```promql
# 各交易对每秒更新速率
sum by (symbol) (rate(depth_updates_total[1m]))

# P99 处理延迟
depth_update_latency_seconds{p99="0.99"}

# 订阅数 TOP10 的交易对
topk(10, depth_subscribers)

# 各交易对买卖盘深度
depth_cache_bid_levels - depth_cache_ask_levels

# 缓存的交易对总数
depth_symbols_cached
```

### 关键告警规则

```yaml
# alerts.yml
groups:
  - name: quote-depth
    rules:
      - alert: HighDepthLatency
        expr: depth_update_latency_seconds{p99="0.99"} > 1
        for: 1m
        annotations:
          summary: "Depth update P99 latency > 1s"
      - alert: ZeroSubscribers
        expr: depth_symbols_cached > 0 and sum(depth_subscribers) == 0
        for: 30s
        annotations:
          summary: "No WebSocket subscribers despite cached data"
```
