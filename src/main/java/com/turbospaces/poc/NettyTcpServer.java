package com.turbospaces.poc;

import io.netty.bootstrap.ServerBootstrap;
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
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turbospaces.poc.Messages.UserCommand;

public class NettyTcpServer implements IOWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger( NettyTcpServer.class );

    private final ServerBootstrap bootstrap = new ServerBootstrap();
    private MultithreadEventLoopGroup workerEventGroup;
    private Class<? extends ServerChannel> channelClass;

    @Override
    public void start(final BenchmarkOptions options) throws Exception {
        if ( IOWorker.EPOLL_MODE ) {
            workerEventGroup = new EpollEventLoopGroup( options.ioWorkerThreads );
            channelClass = EpollServerSocketChannel.class;
        }
        else {
            workerEventGroup = new NioEventLoopGroup( options.ioWorkerThreads );
            channelClass = NioServerSocketChannel.class;
        }

        ServerMessageHandler smh = new ServerMessageHandler();
        DefaultEventExecutorGroup executor = new DefaultEventExecutorGroup( Runtime.getRuntime().availableProcessors() );

        bootstrap.option( ChannelOption.SO_BACKLOG, IOWorker.SO_BACKLOG );
        //
        bootstrap.option( ChannelOption.SO_RCVBUF, IOWorker.SO_RCVBUF );
        bootstrap.childOption( ChannelOption.SO_RCVBUF, IOWorker.SO_RCVBUF );
        //
        bootstrap.option( ChannelOption.SO_SNDBUF, IOWorker.SO_SNDBUF );
        bootstrap.childOption( ChannelOption.SO_SNDBUF, IOWorker.SO_SNDBUF );
        //
        bootstrap.option( ChannelOption.TCP_NODELAY, true );
        bootstrap.childOption( ChannelOption.TCP_NODELAY, true );
        //
        bootstrap.option( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT );
        bootstrap.childOption( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT );
        //
        bootstrap.group( workerEventGroup );
        bootstrap.channel( channelClass );
        bootstrap.childHandler( Misc.channelInitializer( executor, smh ) );
        //
        bootstrap.bind( IOWorker.BIND_ADDRESS ).sync();
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
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            LOGGER.trace( "IN: cmd={}", msg );

            UserCommand ucmd = (UserCommand) msg;
            UserCommand resp = ucmd;
            ucmd.processed = true;
            ucmd.headers.timestamp = System.currentTimeMillis();

            ctx.write( resp, ctx.voidPromise() ); // just write response
        }
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
            super.channelReadComplete( ctx );
        }
        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            LOGGER.warn( "channel writeability changed to={}", ctx.channel().isWritable() );
            super.channelWritabilityChanged( ctx );
        }
    }
}
