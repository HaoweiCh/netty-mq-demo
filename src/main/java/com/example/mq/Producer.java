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
 * 生产者客户端：连上 Broker 后向 topic1 发送 10 条消息。
 */
public class Producer {

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
                                            startHeartbeat(ctx.channel());
                                        }

                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, String s) {
                                            System.out.println("Producer 收到: " + s);
                                        }
                                    });
                        }
                    });

            Channel ch = b.connect("localhost", MqBroker.PORT).sync().channel();

            for (int i = 0; i < 10; i++) {
                MqMessage msg = new MqMessage(
                        "PUBLISH", "topic1", "hello-" + i,
                        UUID.randomUUID().toString(), 0);
                ch.writeAndFlush(JSON.toJSONString(msg));
            }
            System.out.println("Producer 已发送 10 条消息到 topic1");

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
