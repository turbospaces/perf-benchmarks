package com.turbospaces.poc2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ExtensionRegistry;
import com.turbospaces.protobuf.My;
import com.turbospaces.protobuf.My.*;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class ProtoServer {
    public static final int PORT = 8089;

    public static void main(String[] args) throws InterruptedException {
        final Logger logger = LoggerFactory.getLogger( ProtoServer.class );
        final EpollEventLoopGroup eventLoopGroup = new EpollEventLoopGroup();
        final ExtensionRegistry registry = ExtensionRegistry.newInstance();
        final ServerBootstrap bootstrap = new ServerBootstrap();

        try {
            My.registerAllExtensions( registry );

            bootstrap.option( ChannelOption.SO_BACKLOG, 1024 );
            bootstrap.option( ChannelOption.SO_RCVBUF, 1048576 );
            bootstrap.option( ChannelOption.SO_SNDBUF, 1048576 );
            bootstrap.option( ChannelOption.TCP_NODELAY, true );
            bootstrap.option( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT );
            bootstrap.group();
            bootstrap.channel( EpollServerSocketChannel.class );
            bootstrap.handler( new LoggingHandler( LogLevel.INFO ) ).childHandler( new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(final SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast( new ProtobufVarint32FrameDecoder() );
                    p.addLast( new ProtobufDecoder( BaseCommand.getDefaultInstance(), registry ) );

                    p.addLast( new ProtobufVarint32LengthFieldPrepender() );
                    p.addLast( new ProtobufEncoder() );
                    p.addLast( new SimpleChannelInboundHandler<BaseCommand>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, BaseCommand msg) throws Exception {
                            BaseCommand.CommandType type = msg.getType();
                            if ( type == BaseCommand.CommandType.GetPlayerInfoRequest ) {
                                GetPlayerInfoRequest req = msg.getExtension( GetPlayerInfoRequest.body );
                                logger.debug( "server got {}", req );

                                Player player = Player.newBuilder().build();
                                GetPlayerInfoResponse resp = GetPlayerInfoResponse.newBuilder().setPlayer( player ).build();

                                ctx.writeAndFlush( BaseCommand.newBuilder()
                                                              .setCorrId( msg.getCorrId() )
                                                              .setType( BaseCommand.CommandType.GetPlayerInfoResponse )
                                                              .setQualifier( GetPlayerInfoResponse.class.getName() )
                                                              .setExtension( GetPlayerInfoResponse.body, resp )
                                                              .build() );
                            }
                        }
                    } );
                }
            } );
            bootstrap.bind( PORT ).sync().channel().closeFuture().sync();
        }
        finally {
            eventLoopGroup.shutdownGracefully();
        }
    }
}
