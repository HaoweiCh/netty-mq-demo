package com.example.mq;

import com.alibaba.fastjson2.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Broker 核心处理器：协议路由 + 订阅关系 + 内存队列。
 *
 * <p>pipeline 前置了 LengthFieldBasedFrameDecoder / StringDecoder，
 * 因此这里拿到的已经是「一条完整的 JSON 字符串」。
 */
public class BrokerHandler extends SimpleChannelInboundHandler<String> {

    /** 广播消费：topic -> 订阅了该 topic 的所有连接 */
    private static final Map<String, Set<Channel>> TOPIC_SUBS = new ConcurrentHashMap<>();

    /** 集群消费：topic -> 内存队列，消费方通过 PULL 逐条领取 */
    private static final Map<String, BlockingQueue<MqMessage>> TOPIC_QUEUES = new ConcurrentHashMap<>();

    /** 连接 -> 该连接订阅过的 topic 集合，用于断线时清理订阅关系 */
    private static final Map<Channel, Set<String>> CHANNEL_TOPICS = new ConcurrentHashMap<>();

    /** 单 topic 队列上限，防止无限增长 OOM */
    private static final int QUEUE_CAPACITY = 10000;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String json) {
        MqMessage msg = JSON.parseObject(json, MqMessage.class);
        if (msg == null) {
            return;
        }

        switch (msg.getType()) {
            case "SUBSCRIBE":
                TOPIC_SUBS.computeIfAbsent(msg.getTopic(), k -> ConcurrentHashMap.newKeySet())
                        .add(ctx.channel());
                CHANNEL_TOPICS.computeIfAbsent(ctx.channel(), k -> ConcurrentHashMap.newKeySet())
                        .add(msg.getTopic());
                ctx.writeAndFlush(JSON.toJSONString(
                        new MqMessage("SUBACK", msg.getTopic(), null, msg.getMsgId(), 0)));
                break;

            case "PUBLISH":
                // 存内存队列，供 PULL 模式使用
                TOPIC_QUEUES.computeIfAbsent(msg.getTopic(), k -> new LinkedBlockingQueue<>(QUEUE_CAPACITY))
                        .offer(msg);

                // 推送给所有订阅者，广播模式
                Set<Channel> subs = TOPIC_SUBS.get(msg.getTopic());
                if (subs != null) {
                    for (Channel ch : subs) {
                        ch.writeAndFlush(JSON.toJSONString(msg));
                    }
                }
                break;

            case "PULL":
                BlockingQueue<MqMessage> queue = TOPIC_QUEUES.get(msg.getTopic());
                if (queue != null) {
                    MqMessage m = queue.poll();
                    if (m != null) {
                        ctx.writeAndFlush(JSON.toJSONString(m));
                    }
                }
                break;

            case "ACK":
                // 简化版：忽略，或记录 offset
                break;

            case "PING":
                ctx.writeAndFlush(JSON.toJSONString(
                        new MqMessage("PONG", null, null, null, 0)));
                break;

            default:
                // CONNECT 等：简化版不做处理
                break;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        Set<String> topics = CHANNEL_TOPICS.remove(ctx.channel());
        if (topics != null) {
            for (String topic : topics) {
                Set<Channel> subs = TOPIC_SUBS.get(topic);
                if (subs != null) {
                    subs.remove(ctx.channel());
                }
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            // 读空闲超时：认为对端已死，关闭连接（channelInactive 会清理订阅）
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
