package com.turbospaces.poc;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class Misc {
    public static final ObjectMapper mapper = new ObjectMapper();
    private static final LengthFieldPrepender lfp = new LengthFieldPrepender( 4 );

    static {}

    public static ChannelInitializer<Channel> channelInitializer(final ChannelInboundHandlerAdapter h) {
        return new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(Channel channel) {
                ChannelPipeline p = channel.pipeline();

                p.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( 1048576, 0, 4, 0, 4 ) );
                p.addLast( "frameEncoder", lfp );

                p.addLast( h );
            };
        };
    }
}
