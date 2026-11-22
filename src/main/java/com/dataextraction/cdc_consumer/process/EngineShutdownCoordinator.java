package com.dataextraction.cdc_consumer.process;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EngineShutdownCoordinator {

    private static final Logger log = LoggerFactory.getLogger(EngineShutdownCoordinator.class);

    private final AutoCloseable engine;
    private final AutoCloseable consumer;
    private final ExecutorService executor;
    private final CountDownLatch shutdownLatch;
    private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);

    public EngineShutdownCoordinator(
            AutoCloseable engine,
            AutoCloseable consumer,
            ExecutorService executor,
            CountDownLatch shutdownLatch) {
        this.engine = engine;
        this.consumer = consumer;
        this.executor = executor;
        this.shutdownLatch = shutdownLatch;
    }

    public void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "cdc-shutdown"));
    }

    public void shutdown() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return;
        }
        log.info("Shutdown requested");
        try {
            engine.close();
        } catch (Exception e) {
            log.warn("Error closing engine", e);
        }
        try {
            consumer.close();
        } catch (Exception e) {
            log.warn("Error shutting down consumer", e);
        }
        executor.shutdown();
        shutdownLatch.countDown();
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
        executor.awaitTermination(30, TimeUnit.SECONDS);
    }
}
