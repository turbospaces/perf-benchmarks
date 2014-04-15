package com.turbospaces.poc;

import io.netty.util.NetUtil;

import java.net.InetSocketAddress;

public interface IOWorker {
    boolean EPOLL_MODE = false;
    int SO_RCVBUF = 128 * 1024;
    int SO_SNDBUF = 128 * 1024;
    int SO_BACKLOG = 1024;
    InetSocketAddress BIND_ADDRESS = new InetSocketAddress( NetUtil.LOCALHOST, 9143 );

    void start(BenchmarkOptions options) throws Exception;
    void stop() throws Exception;

    public static class BenchmarkOptions {

        public int ioWorkerThreads = Runtime.getRuntime().availableProcessors();

        public int socketConnections = 512;
        public int batchesPerSocket = 1024;
        public int operationsPerBatch = 16;

        public int sleepBetweenBatches = 1 * operationsPerBatch;
    }
}
