package org.logstash.beats;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.netty.SslSimpleBuilder;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class Server {
    private final static Logger logger = LogManager.getLogger(Server.class);

    private final int port;
    private final NioEventLoopGroup workGroup;
    private final String host;
    private final int beatsHeandlerThreadCount;
    private final int maxPendingBatchesPerEventLoop;
    private IMessageListener messageListener = new MessageListener();
    private SslSimpleBuilder sslBuilder;
    private BeatsInitializer beatsInitializer;

    private final int clientInactivityTimeoutSeconds;

    static long lastRejectMessage = System.currentTimeMillis();

    public Server(String host, int p, int timeout, int threadCount, int maxPendingBatches) {
        this.host = host;
        port = p;
        clientInactivityTimeoutSeconds = timeout;
        beatsHeandlerThreadCount = threadCount;
        workGroup = new NioEventLoopGroup();
        maxPendingBatchesPerEventLoop = maxPendingBatches / beatsHeandlerThreadCount;

    }

    public void enableSSL(SslSimpleBuilder builder) {
        sslBuilder = builder;
    }

    public Server listen() throws InterruptedException {
        try {
            logger.info("Starting server on port: " +  this.port);

            beatsInitializer = new BeatsInitializer(isSslEnable(), messageListener, clientInactivityTimeoutSeconds, beatsHeandlerThreadCount, maxPendingBatchesPerEventLoop);

            ServerBootstrap server = new ServerBootstrap();
            server.group(workGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_LINGER, 0) // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
                    .childHandler(beatsInitializer);

            Channel channel = server.bind(host, port).sync().channel();
            channel.closeFuture().sync();
        } finally {
            shutdown();
        }

        return this;
    }

    public void stop() throws InterruptedException {
        logger.debug("Server shutting down");
        shutdown();
        logger.debug("Server stopped");
    }

    private void shutdown(){
        try {
            workGroup.shutdownGracefully().sync();
            beatsInitializer.shutdownEventExecutor();
        } catch (InterruptedException e){
            throw new IllegalStateException(e);
        }
    }

    public void setMessageListener(IMessageListener listener) {
        messageListener = listener;
    }

    public boolean isSslEnable() {
        return this.sslBuilder != null;
    }

    private class BeatsInitializer extends ChannelInitializer<SocketChannel> {
        private final String SSL_HANDLER = "ssl-handler";
        private final String IDLESTATE_HANDLER = "idlestate-handler";
        private final String KEEP_ALIVE_HANDLER = "keep-alive-handler";
        private final String BEATS_ACKER = "beats-acker";


        private final int DEFAULT_IDLESTATEHANDLER_THREAD = 4;
        private final int IDLESTATE_WRITER_IDLE_TIME_SECONDS = 5;

        private final EventExecutorGroup idleExecutorGroup;
        private final EventExecutorGroup beatsHandlerExecutorGroup;
        private final IMessageListener message;
        private int clientInactivityTimeoutSeconds;

        private boolean enableSSL = false;

        public BeatsInitializer(Boolean secure, IMessageListener messageListener, int clientInactivityTimeoutSeconds, int beatsHandlerThread, int maxPendingBatchesPerEventLoop) {
            enableSSL = secure;
            this.message = messageListener;
            this.clientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
            idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD);
            // Best attempt to reject events past a threshold of pending batches.
            // Profiling shows once the channel is connected, 1 tasks per handler and 1 for the Netty task to handle memory management.
            final int BATCH_TO_TASK_MULTIPLIER = 3;  //2 handlers, 1 netty task
            beatsHandlerExecutorGroup = new DefaultEventExecutorGroup(beatsHandlerThread, null, maxPendingBatchesPerEventLoop * BATCH_TO_TASK_MULTIPLIER
                    , new LoggingRejectedExecutionHandler());
        }

        public void initChannel(SocketChannel socket) throws IOException, NoSuchAlgorithmException, CertificateException {
            ChannelPipeline pipeline = socket.pipeline();

            if(enableSSL) {
                SslHandler sslHandler = sslBuilder.build(socket.alloc());
                pipeline.addLast(SSL_HANDLER, sslHandler);
            }
            pipeline.addLast(idleExecutorGroup, IDLESTATE_HANDLER, new IdleStateHandler(clientInactivityTimeoutSeconds, IDLESTATE_WRITER_IDLE_TIME_SECONDS , 0));
            pipeline.addLast(BEATS_ACKER, new AckEncoder());
            pipeline.addLast(KEEP_ALIVE_HANDLER, new ConnectionHandler());
            //if changing the number of handlers in the beatsHandlerExecutorGroup, update the BATCH_TO_TASK_MULTIPLIER
            pipeline.addLast(beatsHandlerExecutorGroup, new BeatsParser(), new BeatsHandler(this.message));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Exception caught in channel initializer", cause);
            try {
                this.message.onChannelInitializeException(ctx, cause);
            } finally {
                super.exceptionCaught(ctx, cause);
            }
        }

        public void shutdownEventExecutor() {
            try {
                idleExecutorGroup.shutdownGracefully().sync();
                beatsHandlerExecutorGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    class LoggingRejectedExecutionHandler implements RejectedExecutionHandler {


        @Override
        public void rejected(Runnable task, SingleThreadEventExecutor executor) {

            //TODO: Figure out the proper the downcast here...I am pretty sure there is a way!
            ((DefaultChannelPipeline)task).channel().close();

          //  long now = System.currentTimeMillis();
            final String MESSAGE = "Rejected beats batch due to lack of resources. Try to increase the max_pending_batches and/or executor_threads. " +
                    "Additional memory and CPU may be required when increasing those values.";
          //  if (now - lastRejectMessage > 1) {
                logger.warn(MESSAGE);
         //       lastRejectMessage = now;
          //  }else{
          //      logger.trace(MESSAGE);
         //   }
        }
    }

}
