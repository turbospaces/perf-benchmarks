package com.turbospaces.poc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class Misc {
    static final ObjectMapper mapper = new ObjectMapper();

    static final LengthFieldPrepender lfp = new LengthFieldPrepender( 4 );
    static final JsonEncoder jsonEncoder = new JsonEncoder();
    static final JsonDecoder jsonDecoder = new JsonDecoder();

    public static ChannelInitializer<Channel> channelInitializer(final ChannelInboundHandlerAdapter h) {
        return new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(Channel channel) {
                ChannelPipeline p = channel.pipeline();

                p.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( 1048576, 0, 4, 0, 4 ) );
                p.addLast( "jsonDecoder", jsonDecoder );

                p.addLast( "frameEncoder", lfp );
                p.addLast( "jsonEncoder", jsonEncoder );
                p.addLast( h );
            };
        };
    }

    @Sharable
    private static class JsonEncoder extends MessageToMessageEncoder<Messages> {
        @Override
        protected void encode(ChannelHandlerContext ctx, Messages msg, List<Object> out) throws Exception {
            byte[] asBytes = Misc.mapper.writeValueAsBytes( msg );
            ByteBuf b = ctx.alloc().ioBuffer( asBytes.length ).writeBytes( asBytes );
            out.add( b );
        }
    }

    @Sharable
    private static class JsonDecoder extends MessageToMessageDecoder<ByteBuf> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
            byte[] array = new byte[msg.readableBytes()];
            msg.getBytes( 0, array );
            Messages cmd = Misc.mapper.readValue( array, Messages.class );
            out.add( cmd );
        }
    }
}
