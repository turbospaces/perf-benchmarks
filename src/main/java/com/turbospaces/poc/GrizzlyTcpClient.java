package com.turbospaces.poc;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.EmptyCompletionHandler;
import org.glassfish.grizzly.connectionpool.SingleEndpointPool;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChain;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.transport.TCPNIOConnectorHandler;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.strategies.SameThreadIOStrategy;
import org.glassfish.grizzly.utils.Charsets;
import org.glassfish.grizzly.utils.StringFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.turbospaces.poc.Messages.UserCommand;

public class GrizzlyTcpClient implements IOWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger( GrizzlyTcpClient.class );
    private SingleEndpointPool<SocketAddress> pool;
    private TCPNIOTransport transport;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void start(final BenchmarkOptions options) throws IOException, InterruptedException {
        final int totalOperations = options.batchesPerSocket * options.operationsPerBatch * options.socketConnections;
        final MetricRegistry metrics = new MetricRegistry();
        final Timer latency = metrics.timer( "responses-latency" );
        final CountDownLatch responseCount = new CountDownLatch( totalOperations );
        final ConcurrentMap<String, UserCommand> corr = Maps.newConcurrentMap();
        final AtomicLong respCount = new AtomicLong();

        FilterChainBuilder clientFilterChain = FilterChainBuilder.stateless();
        TCPNIOTransportBuilder builder = TCPNIOTransportBuilder.newInstance();

        builder.setOptimizedForMultiplexing( true );
        builder.setIOStrategy( SameThreadIOStrategy.getInstance() );
        builder.setTcpNoDelay( true );
        builder.setReadBufferSize( IOWorker.SO_RCVBUF );
        builder.setWriteBufferSize( IOWorker.SO_SNDBUF );

        transport = builder.build();
        FilterChain filterChain = clientFilterChain
                .add( new TransportFilter() )
                .add( new StringFilter( Charsets.UTF8_CHARSET ) )
                .add( new BaseFilter() {
                    @Override
                    public NextAction handleRead(FilterChainContext ctx) {
                        long currVal = respCount.getAndIncrement();
                        String json = ctx.getMessage();

                        try {
                            UserCommand resp = Misc.mapper.readValue( json, Messages.UserCommand.class );
                            UserCommand req = corr.remove( resp.headers.correlationId );

                            if ( currVal > totalOperations * 0.25 ) {
                                long took = System.currentTimeMillis() - req.headers.timestamp;
                                latency.update( took, TimeUnit.MILLISECONDS );
                            }
                        }
                        catch ( Exception e ) {
                            Throwables.propagate( e );
                        }

                        responseCount.countDown();
                        pool.release( ctx.getConnection() );
                        return ctx.getStopAction();
                    }
                } )
                .build();
        transport.start();

        TCPNIOConnectorHandler connectorHandler = TCPNIOConnectorHandler.builder( transport ).processor( filterChain ).build();

        pool = SingleEndpointPool
                .builder( SocketAddress.class )
                .connectorHandler( connectorHandler )
                .endpointAddress( IOWorker.BIND_ADDRESS )
                .corePoolSize( options.socketConnections )
                .maxPoolSize( options.socketConnections )
                .build();

        long now = System.currentTimeMillis();

        for ( int i = 0; i < options.socketConnections; i++ ) {
            Thread thread = new Thread( new Runnable() {
                @Override
                public void run() {
                    for ( int i = 0; i < options.operationsPerBatch * options.batchesPerSocket; i++ ) {
                        pool.take( new EmptyCompletionHandler<Connection>() {
                            @Override
                            public void completed(Connection c) {
                                UserCommand cmd = UserCommand.some();
                                corr.put( cmd.headers.correlationId, cmd );
                                try {
                                    c.write( Misc.mapper.writeValueAsString( cmd ) );
                                }
                                catch ( JsonProcessingException e ) {
                                    Throwables.propagate( e );
                                }
                            }
                        } );
                    }
                }
            } );
            thread.start();
        }

        responseCount.await();
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
        pool.close();
        transport.shutdown();
    }

    public static void main(String... args) throws Exception {
        BenchmarkOptions options = new BenchmarkOptions();
        GrizzlyTcpClient tcpServer = new GrizzlyTcpClient();
        tcpServer.start( options );
    }
}
