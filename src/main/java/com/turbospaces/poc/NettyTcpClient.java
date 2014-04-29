package com.turbospaces.poc;

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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;
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
        MetricRegistry metrics = new MetricRegistry();
        Timer latency = metrics.timer( "responses-latency" );

        for ( int i = 0; i < options.socketConnections; i++ ) {
            ClientMessageHandler cmh = new ClientMessageHandler( options, cl, responseCount, latency );
            Bootstrap bootstrap = new Bootstrap();
            //
            bootstrap.option( ChannelOption.SO_RCVBUF, IOWorker.SO_RCVBUF );
            bootstrap.option( ChannelOption.SO_SNDBUF, IOWorker.SO_SNDBUF );
            //
            bootstrap.option( ChannelOption.TCP_NODELAY, true );
            //
            bootstrap.option( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT );

            bootstrap.group( eventGroup );
            bootstrap.channel( channelClass );

            bootstrap.handler( Misc.channelInitializer( null, cmh ) );
            bootstrap.connect( BIND_ADDRESS );
            cmhs[i] = cmh;
        }
        cl.await(); // wait for all to connect

        long now = System.currentTimeMillis();
        for ( int b = 1; b <= options.batchesPerSocket; b++ ) {
            for ( int i = 0; i < options.socketConnections; i++ ) {
                cmhs[i].execute();
            }
            Thread.sleep( options.sleepBetweenBatches );
        }
        responseCount.await(); // wait for all responses

        long took = System.currentTimeMillis() - now;
        LOGGER.info( "took = {} ms, TPS = {}, totalOps = {}", took, (int) ( 1000 * ( ( (double) totalOperations ) / took ) ), totalOperations );
        Slf4jReporter reporter = Slf4jReporter
                .forRegistry( metrics )
                .outputTo( LOGGER )
                .convertRatesTo( TimeUnit.MILLISECONDS )
                .convertDurationsTo( TimeUnit.MILLISECONDS )
                .build();
        reporter.report();

        stop();
    }
    @Override
    public void stop() {
        eventGroup.shutdownGracefully();
    }

    @Sharable
    private static class ClientMessageHandler extends ChannelInboundHandlerAdapter {
        private final CountDownLatch allConnected;
        private final CountDownLatch responseCount;
        private final Timer latency;
        private final AtomicLong respCount;
        private final ConcurrentMap<String, UserCommand> corr;
        private final BenchmarkOptions options;
        private Channel channel;

        public ClientMessageHandler(BenchmarkOptions options, CountDownLatch allConnected, CountDownLatch responseCount, Timer latency) {
            this.options = options;
            this.allConnected = allConnected;
            this.responseCount = responseCount;
            this.latency = latency;
            this.corr = Maps.newConcurrentMap();
            this.respCount = new AtomicLong();
        }
        public void execute() {
            channel.eventLoop().execute( new Runnable() {
                @Override
                public void run() {
                    for ( int j = 1; j <= options.operationsPerBatch; j++ ) {
                        UserCommand cmd = UserCommand.some();
                        corr.put( cmd.headers.correlationId, cmd );
                        channel.write( cmd, channel.voidPromise() );
                    }
                    channel.flush();
                }
            } );
        }
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            allConnected.countDown();
            super.channelActive( ctx );
        }
        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            super.handlerAdded(ctx);
            channel = ctx.channel();
        }
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            LOGGER.trace( "IN: cmd={}", msg );
            responseCount.countDown();
            long currRespCount = respCount.incrementAndGet();

            UserCommand resp = (UserCommand) msg;
            UserCommand req = corr.remove( resp.headers.correlationId );

            if ( currRespCount > options.batchesPerSocket * options.operationsPerBatch * 0.10 ) {
                long took = System.currentTimeMillis() - req.headers.timestamp;
                latency.update( took, TimeUnit.MILLISECONDS );
            }
        }
    }

    public static void main(String... args) throws Exception {
        BenchmarkOptions options = new BenchmarkOptions();
        NettyTcpClient tcpServer = new NettyTcpClient();
        tcpServer.start( options );
    }
}
