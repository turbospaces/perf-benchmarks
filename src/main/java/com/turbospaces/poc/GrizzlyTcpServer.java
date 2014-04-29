package com.turbospaces.poc;

import java.io.IOException;
import java.nio.charset.Charset;

import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.strategies.SameThreadIOStrategy;
import org.glassfish.grizzly.utils.StringFilter;

import com.turbospaces.poc.Messages.UserCommand;

public class GrizzlyTcpServer implements IOWorker {
    private TCPNIOTransport transport;

    @Override
    public void start(BenchmarkOptions options) throws Exception {
        FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();
        TCPNIOTransportBuilder builder = TCPNIOTransportBuilder.newInstance();

        filterChainBuilder.add( new TransportFilter() );
        filterChainBuilder.add( new StringFilter( Charset.forName( "UTF-8" ) ) );
        filterChainBuilder.add( new BaseFilter() {
            @Override
            public NextAction handleRead(FilterChainContext ctx) throws IOException {
                String json = ctx.getMessage();
                UserCommand cmd = Misc.mapper.readValue( json, Messages.UserCommand.class );
                cmd.processed = true;
                cmd.headers.timestamp = System.currentTimeMillis();
                ctx.write( Misc.mapper.writeValueAsString( cmd ) );
                return ctx.getStopAction();
            }
        } );

        builder.setOptimizedForMultiplexing( true );
        builder.setIOStrategy( SameThreadIOStrategy.getInstance() );
        builder.setTcpNoDelay( true );
        builder.setReadBufferSize( IOWorker.SO_RCVBUF );
        builder.setWriteBufferSize( IOWorker.SO_SNDBUF );
        builder.setServerConnectionBackLog( IOWorker.SO_BACKLOG );
        transport = builder.build();

        transport.setProcessor( filterChainBuilder.build() );
        transport.bind( IOWorker.BIND_ADDRESS );
        transport.start();
    }

    @Override
    public void stop() {
        transport.shutdown();
    }

    public static void main(String... args) throws Exception {
        GrizzlyTcpServer tcpServer = new GrizzlyTcpServer();
        tcpServer.start( new BenchmarkOptions() );
        synchronized ( tcpServer ) {
            tcpServer.wait();
        }
    }
}
