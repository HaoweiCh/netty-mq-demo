package com.example.mq;

import com.alibaba.fastjson2.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 消费者客户端：连上 Broker 后订阅 topic1，收到 PUBLISH 打印并按 msgId 回 ACK。
 */
public class Consumer {

    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4))
                                    .addLast(new LengthFieldPrepender(4))
                                    .addLast(new StringDecoder(CharsetUtil.UTF_8))
                                    .addLast(new StringEncoder(CharsetUtil.UTF_8))
                                    .addLast(new SimpleChannelInboundHandler<String>() {
                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) {
                                            MqMessage sub = new MqMessage(
                                                    "SUBSCRIBE", "topic1", null,
                                                    UUID.randomUUID().toString(), 0);
                                            ctx.writeAndFlush(JSON.toJSONString(sub));
                                            startHeartbeat(ctx.channel());
                                        }

                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, String s) {
                                            MqMessage msg = JSON.parseObject(s, MqMessage.class);
                                            if (msg == null) {
                                                return;
                                            }
                                            if ("PUBLISH".equals(msg.getType())) {
                                                System.out.println("Consumer 收到: " + msg.getBody());

                                                MqMessage ack = new MqMessage(
                                                        "ACK", msg.getTopic(), null,
                                                        msg.getMsgId(), msg.getOffset());
                                                ctx.writeAndFlush(JSON.toJSONString(ack));
                                            }
                                        }
                                    });
                        }
                    });

            Channel ch = b.connect("localhost", MqBroker.PORT).sync().channel();
            System.out.println("Consumer 已连接并订阅 topic1");

            ch.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    /** 每 30s 发一次 PING，避免被服务端 IdleStateHandler(60s) 判为读空闲而断开。 */
    private static void startHeartbeat(Channel ch) {
        ch.eventLoop().scheduleAtFixedRate(
                () -> ch.writeAndFlush(JSON.toJSONString(new MqMessage("PING", null, null, null, 0))),
                30, 30, TimeUnit.SECONDS);
    }
}
