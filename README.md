# netty-mq-demo

用 Netty 实现的**纯内存简易消息中间件** Demo —— 一个 Java 项目。

定位：**学习 Netty + 理解消息中间件原理**，不是生产级 MQ。

---

## 一、架构

```text
Producer -> Netty Client -> Broker(Netty Server) -> Topic -> Consumer
```

Broker 内部：`协议解码 -> 路由 -> 内存队列 / 订阅关系 -> 推送或拉取 -> ACK`

核心数据结构（都在 `BrokerHandler`，全部 `static`）：

| 结构 | 作用 |
|---|---|
| `topicSubscribers: Map<String, Set<Channel>>` | 广播消费，谁订阅了哪个 Topic |
| `topicQueues: Map<String, BlockingQueue<MqMessage>>` | 集群消费，消息进队列，消费者 PULL |
| `channelTopics: Map<Channel, Set<String>>` | 连接断开时清理订阅关系 |

## 二、协议

```text
4 字节长度 + JSON 消息体
```

由 Netty 的 `LengthFieldBasedFrameDecoder` / `LengthFieldPrepender` 自动处理 TCP 粘包拆包。

消息类型：

| type | 说明 |
|---|---|
| `SUBSCRIBE` | 订阅主题，Broker 回 `SUBACK` |
| `PUBLISH` | 发布消息：入内存队列（供 PULL）+ 推送给全部订阅者（广播） |
| `PULL` | 从队列取一条消息 |
| `ACK` | 消费确认（简化版仅记录，不做重投） |
| `PING` | 心跳，Broker 回 `PONG` |

pipeline（客户端与服务端一致）：

```java
LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4)
LengthFieldPrepender(4)
StringDecoder(UTF_8)
StringEncoder(UTF_8)
IdleStateHandler(60, 0, 0)   // 服务端：读空闲 60s 则断开
BrokerHandler                 // 客户端为各自的打印 handler
```

客户端每 30s 发一次 `PING`，保证长连接不被 `IdleStateHandler` 判为读空闲而断开。

## 三、运行

环境要求：JDK 17+、Maven。

```bash
mvn clean compile
```

按顺序开三个终端：

```bash
# 1. 启动 Broker（监听 8888）
mvn exec:java -Dexec.mainClass=com.example.mq.MqBroker

# 2. 启动 Consumer（自动订阅 topic1）
mvn exec:java -Dexec.mainClass=com.example.mq.Consumer

# 3. 启动 Producer（发送 hello-0 ~ hello-9）
mvn exec:java -Dexec.mainClass=com.example.mq.Producer
```

Consumer 控制台应输出：

```text
Consumer 收到: hello-0
...
Consumer 收到: hello-9
```

## 四、能力边界

**能支持**：Broker 单机、Topic 发布/订阅、生产者发送、消费者订阅接收、心跳与断线清理、简单 ACK、内存队列、广播消费。

**不适合生产**：

- 重启消息全丢，无持久化、无副本、无高可用
- 队列无限增长会 OOM
- Offset 不持久，消费者重启丢进度
- 无事务、无顺序保证、无集群

## 五、后续可扩展方向

1. **集群消费**：消费者用 `PULL`，一条消息只给一个消费者
2. **消费组 + Offset**：`Map<group, Map<topic, offset>>`
3. **长轮询**：`PULL` 无消息时挂起，有消息再返回
4. **有界队列 + 丢弃策略**：防止 OOM
5. **文件持久化**：WAL、CommitLog、RocksDB
6. **延时消息、死信队列、顺序消息**
7. **主从副本、集群路由、注册中心**

## 六、代码结构

```text
src/main/java/com/example/mq/
├── MqMessage.java       # 协议消息模型
├── MqBroker.java        # Broker 启动类（Netty Server，8888）
├── BrokerHandler.java   # Broker 核心：路由 / 订阅 / 推送 / PULL / ACK / 心跳
├── Producer.java        # 生产者客户端
└── Consumer.java        # 消费者客户端
```

设计参考见 [`plan.md`](plan.md)。
