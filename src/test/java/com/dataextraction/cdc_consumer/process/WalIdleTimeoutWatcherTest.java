package com.dataextraction.cdc_consumer.process;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class WalIdleTimeoutWatcherTest {

    @Test
    void shutsDownAfterIdleWithoutAWalRecord() throws Exception {
        CountDownLatch closed = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        EngineShutdownCoordinator coordinator = coordinator(closed, executor);
        WalIdleTimeoutWatcher watcher = new WalIdleTimeoutWatcher(1, coordinator);
        watcher.start();
        try {
            assertTrue(closed.await(3, TimeUnit.SECONDS), "countdown starts when the watcher starts");
        } finally {
            watcher.cancel();
            executor.shutdownNow();
        }
    }

    @Test
    void defersShutdownUntilHighWaterMarkIsPassed() throws Exception {
        CountDownLatch closed = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        EngineShutdownCoordinator coordinator = coordinator(closed, executor);
        WalIdleTimeoutWatcher watcher = new WalIdleTimeoutWatcher(1, coordinator);
        watcher.deferIdleShutdownUntilPastHighWaterMark();
        watcher.start();
        try {
            assertFalse(closed.await(1500, TimeUnit.MILLISECONDS), "idle expiry must wait until the high water mark");
            watcher.markPastHighWaterMark();
            assertTrue(closed.await(3, TimeUnit.SECONDS), "expected shutdown after the post-mark idle timeout");
        } finally {
            watcher.cancel();
            executor.shutdownNow();
        }
    }

    @Test
    void shutsDownWhenSenderWaitReleasesTheHeldTimeout() throws Exception {
        CountDownLatch closed = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        EngineShutdownCoordinator coordinator = coordinator(closed, executor);
        WalIdleTimeoutWatcher watcher = new WalIdleTimeoutWatcher(1, coordinator);
        watcher.deferIdleShutdownUntilPastHighWaterMark();
        watcher.start();
        try {
            watcher.allowIdleShutdown();
            assertTrue(closed.await(3, TimeUnit.SECONDS), "caught-up walsender must let the idle timeout finish");
        } finally {
            watcher.cancel();
            executor.shutdownNow();
        }
    }

    private static EngineShutdownCoordinator coordinator(CountDownLatch closed, ExecutorService executor) {
        return new EngineShutdownCoordinator(
                () -> {},
                closed::countDown,
                executor,
                new CountDownLatch(1));
    }
}
