package com.turbospaces.poc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turbospaces.poc.Messages.UserCommand;

public class NettyTcpServer implements Server {
    private static final Logger LOGGER = LoggerFactory.getLogger( NettyTcpServer.class );

    private final ServerBootstrap bootstrap = new ServerBootstrap();
    private MultithreadEventLoopGroup workerEventGroup;
    private Class<? extends ServerChannel> channelClass;

    @Override
    public void start(BenchmarkOptions options) throws Exception {
        if ( Server.EPOLL_MODE ) {
            workerEventGroup = new EpollEventLoopGroup( options.ioWorkerThreads );
            channelClass = EpollServerSocketChannel.class;
        }
        else {
            workerEventGroup = new NioEventLoopGroup( options.ioWorkerThreads );
            channelClass = NioServerSocketChannel.class;
        }

        ServerMessageHandler smh = new ServerMessageHandler();

        bootstrap.option( ChannelOption.SO_RCVBUF, Server.SO_RCVBUF );
        bootstrap.option( ChannelOption.SO_SNDBUF, Server.SO_SNDBUF );
        bootstrap.option( ChannelOption.SO_BACKLOG, Server.SO_BACKLOG );
        bootstrap.option( ChannelOption.TCP_NODELAY, true );
        bootstrap.childOption( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT );

        bootstrap.group( workerEventGroup );
        bootstrap.channel( channelClass );

        bootstrap.childHandler( Misc.channelInitializer( smh ) );
        bootstrap.handler( new LoggingHandler( LogLevel.TRACE ) );
        bootstrap.bind( Server.BIND_ADDRESS ).sync();
    }

    @Override
    public void stop() {
        workerEventGroup.shutdownGracefully();
    }

    public static void main(String... args) throws Exception {
        NettyTcpServer tcpServer = new NettyTcpServer();
        tcpServer.start( new BenchmarkOptions() );
    }

    @Sharable
    private static class ServerMessageHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            try {
                ByteBuf buf = (ByteBuf) msg;
                byte[] array = new byte[buf.readableBytes()];
                buf.getBytes( 0, array );

                Messages cmd = Misc.mapper.readValue( array, Messages.class );
                LOGGER.trace( "IN: cmd={}", cmd );

                if ( cmd instanceof UserCommand ) {
                    UserCommand ucmd = (UserCommand) cmd;
                    ucmd.processed = true;
                    byte[] asBytes = Misc.mapper.writeValueAsBytes( ucmd );
                    ByteBuf b = ctx.alloc().ioBuffer( asBytes.length ).writeBytes( asBytes );
                    ctx.write( b, ctx.channel().voidPromise() );
                }
            }
            finally {
                ReferenceCountUtil.release( msg );
            }
        }
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
            super.channelReadComplete( ctx );
        }
    }
}