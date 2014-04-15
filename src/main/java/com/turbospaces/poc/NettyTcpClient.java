package com.turbospaces.poc;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turbospaces.poc.Messages.UserCommand;

public class NettyTcpClient implements IOWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger( NettyTcpClient.class );

    private MultithreadEventLoopGroup eventGroup;
    private Class<? extends SocketChannel> channelClass;

    @Override
    public void start(final BenchmarkOptions options) throws Exception {
        if ( IOWorker.EPOLL_MODE ) {
            eventGroup = new EpollEventLoopGroup( options.ioWorkerThreads );
            channelClass = EpollSocketChannel.class;
        }
        else {
            eventGroup = new NioEventLoopGroup( options.ioWorkerThreads );
            channelClass = NioSocketChannel.class;
        }

        int totalOperations = options.batchesPerSocket * options.operationsPerBatch * options.socketConnections;
        CountDownLatch responseCount = new CountDownLatch( totalOperations );
        CountDownLatch cl = new CountDownLatch( options.socketConnections );
        ClientMessageHandler[] cmhs = new ClientMessageHandler[options.socketConnections];

        for ( int i = 0; i < options.socketConnections; i++ ) {
            ClientMessageHandler cmh = new ClientMessageHandler( cl, responseCount );
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.option( ChannelOption.SO_RCVBUF, IOWorker.SO_RCVBUF );
            bootstrap.option( ChannelOption.SO_SNDBUF, IOWorker.SO_SNDBUF );
            bootstrap.option( ChannelOption.TCP_NODELAY, true );
            bootstrap.option( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT );

            bootstrap.group( eventGroup );
            bootstrap.channel( channelClass );

            bootstrap.handler( Misc.channelInitializer( cmh ) );
            bootstrap.connect( BIND_ADDRESS );
            cmhs[i] = cmh;
        }
        cl.await(); // wait for all to connect

        long now = System.currentTimeMillis();
        for ( int b = 1; b <= options.batchesPerSocket; b++ ) {
            for ( int i = 0; i < options.socketConnections; i++ ) {
                cmhs[i].execute( options );
            }
            Thread.sleep( options.sleepBetweenBatches );
        }
        responseCount.await(); // wait for all responses
        long took = System.currentTimeMillis() - now;
        LOGGER.info( "took = {} ms, TPS = {}, totalOps = {}", took, (int) ( 1000 * ( ( (double) totalOperations ) / took ) ), totalOperations );
        stop();
    }
    @Override
    public void stop() {
        eventGroup.shutdownGracefully();
    }

    @Sharable
    private static class ClientMessageHandler extends ChannelInboundHandlerAdapter {
        private final CountDownLatch cl;
        private final CountDownLatch responseCount;
        private Channel channel;

        public ClientMessageHandler(CountDownLatch allConnected, CountDownLatch responseCount) {
            this.cl = allConnected;
            this.responseCount = responseCount;
        }
        public void execute(final BenchmarkOptions options) {
            channel.eventLoop().execute( new Runnable() {
                @Override
                public void run() {
                    for ( int j = 1; j <= options.operationsPerBatch; j++ ) {
                        UserCommand userCommand = UserCommand.some( System.currentTimeMillis() );
                        channel.write( userCommand, channel.voidPromise() );
                    }
                    channel.flush();
                }
            } );
        }
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            cl.countDown();
            channel = ctx.channel();
            super.channelActive( ctx );
        }
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            checkArgument( msg instanceof Messages );
            try {
                LOGGER.trace( "IN: cmd={}", msg );
                responseCount.countDown();
            }
            finally {
                ReferenceCountUtil.release( msg );
            }
        }
    }

    public static void main(String... args) throws Exception {
        BenchmarkOptions options = new BenchmarkOptions();
        options.ioWorkerThreads = options.ioWorkerThreads * 2;
        NettyTcpClient tcpServer = new NettyTcpClient();
        tcpServer.start( options );
    }
}
