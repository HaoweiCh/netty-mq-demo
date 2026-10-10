package com.example.mq;

/**
 * 协议消息模型。
 *
 * <p>线上格式：4 字节长度（LengthFieldPrepender 添加） + 本对象的 JSON 体。
 *
 * <p>注意：fastjson2 默认通过 getter/setter 读写属性，因此 getter/setter 不可省略，
 * 否则序列化结果为空对象、协议直接不通。
 */
public class MqMessage {

    /** 消息类型：CONNECT / SUBSCRIBE / SUBACK / PUBLISH / PULL / ACK / PING / PONG */
    private String type;
    /** 主题 */
    private String topic;
    /** 消息体 */
    private String body;
    /** 消息唯一 ID，ACK 时回传 */
    private String msgId;
    /** 消费位点，简化版暂不使用（预留） */
    private long offset;

    public MqMessage() {
    }

    public MqMessage(String type, String topic, String body, String msgId, long offset) {
        this.type = type;
        this.topic = topic;
        this.body = body;
        this.msgId = msgId;
        this.offset = offset;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "MqMessage{type='" + type + "', topic='" + topic + "', body='" + body
                + "', msgId='" + msgId + "', offset=" + offset + '}';
    }
}
