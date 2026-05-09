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

```json
{"action": "subscribe", "symbol": "BTCUSDT"}
```

### 取消订阅

```json
{"action": "unsubscribe", "symbol": "BTCUSDT"}
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
{"action":"subscribe","symbol":"BTCUSDT"}
```

### JavaScript (浏览器)

```javascript
const ws = new WebSocket('ws://localhost:8081/depth');

ws.onopen = () => {
    ws.send(JSON.stringify({action: 'subscribe', symbol: 'BTCUSDT'}));
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
        await ws.send(json.dumps({"action": "subscribe", "symbol": "BTCUSDT"}))
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
            "{\"action\":\"subscribe\",\"symbol\":\"BTCUSDT\"}"
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
