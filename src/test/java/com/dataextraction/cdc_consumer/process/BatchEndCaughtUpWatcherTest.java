package com.dataextraction.cdc_consumer.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.dataextraction.pg.meta.PostgresLsn;

class BatchEndCaughtUpWatcherTest {

    @Test
    void shutsDownWhenConfirmedFlushReachesBatchEnd() throws Exception {
        PostgresLsn batchEnd = new PostgresLsn("10/100");
        AtomicReference<PostgresLsn> flush = new AtomicReference<>(new PostgresLsn("10/10"));
        CountDownLatch consumerClosed = new CountDownLatch(1);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        EngineShutdownCoordinator coordinator = new EngineShutdownCoordinator(
                () -> {},
                consumerClosed::countDown,
                executor,
                shutdownLatch);

        BatchEndCaughtUpWatcher watcher = new BatchEndCaughtUpWatcher(batchEnd, flush::get, coordinator);
        watcher.start();
        try {
            flush.set(new PostgresLsn("10/100"));
            assertTrue(consumerClosed.await(5, TimeUnit.SECONDS), "expected shutdown after catch-up");
        } finally {
            watcher.cancel();
            executor.shutdownNow();
        }
    }

    @Test
    void invokesCaughtUpActionInsteadOfShutdown() throws Exception {
        PostgresLsn batchEnd = new PostgresLsn("10/100");
        AtomicReference<PostgresLsn> flush = new AtomicReference<>(new PostgresLsn("10/10"));
        CountDownLatch consumerClosed = new CountDownLatch(1);
        CountDownLatch caughtUp = new CountDownLatch(1);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        EngineShutdownCoordinator coordinator = new EngineShutdownCoordinator(
                () -> {},
                consumerClosed::countDown,
                executor,
                shutdownLatch);

        BatchEndCaughtUpWatcher watcher = new BatchEndCaughtUpWatcher(
                batchEnd, flush::get, coordinator, caughtUp::countDown);
        watcher.start();
        try {
            flush.set(batchEnd);
            assertTrue(caughtUp.await(5, TimeUnit.SECONDS), "expected catch-up action");
            assertEquals(1, consumerClosed.getCount(), "catch-up action must not stop the engine");
        } finally {
            watcher.cancel();
            executor.shutdownNow();
        }
    }

    @Test
    void catchesUpWhenWalSenderPassesBatchEndWithoutFlushMoving() throws Exception {
        PostgresLsn batchEnd = new PostgresLsn("14/B4132CE8");
        AtomicReference<PostgresLsn> flush = new AtomicReference<>(new PostgresLsn("14/B4116418"));
        AtomicReference<PostgresLsn> sent = new AtomicReference<>(new PostgresLsn("14/B4116418"));
        CountDownLatch consumerClosed = new CountDownLatch(1);
        CountDownLatch caughtUp = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        EngineShutdownCoordinator coordinator = new EngineShutdownCoordinator(
                () -> {},
                consumerClosed::countDown,
                executor,
                new CountDownLatch(1));

        BatchEndCaughtUpWatcher watcher = new BatchEndCaughtUpWatcher(
                batchEnd, flush::get, sent::get, coordinator, caughtUp::countDown);
        watcher.start();
        try {
            sent.set(batchEnd);
            assertTrue(caughtUp.await(5, TimeUnit.SECONDS), "expected catch-up from sent_lsn");
            assertEquals(1, consumerClosed.getCount(), "catch-up action must not stop the engine");
        } finally {
            watcher.cancel();
            executor.shutdownNow();
        }
    }

    @Test
    void releasesIdleTimeoutWhenWalsenderWaitsForWal() throws Exception {
        PostgresLsn batchEnd = new PostgresLsn("14/B4132CE8");
        AtomicReference<PostgresLsn> flush = new AtomicReference<>(new PostgresLsn("14/B4116418"));
        AtomicBoolean waiting = new AtomicBoolean(false);
        CountDownLatch consumerClosed = new CountDownLatch(1);
        CountDownLatch senderWaiting = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        EngineShutdownCoordinator coordinator = new EngineShutdownCoordinator(
                () -> {},
                consumerClosed::countDown,
                executor,
                new CountDownLatch(1));

        BatchEndCaughtUpWatcher watcher = new BatchEndCaughtUpWatcher(
                batchEnd, flush::get, () -> null, coordinator, () -> {});
        watcher.setWalSenderWaitingForWal(waiting::get, senderWaiting::countDown);
        watcher.start();
        try {
            waiting.set(true);
            assertTrue(senderWaiting.await(5, TimeUnit.SECONDS), "expected idle timeout release");
            assertEquals(1, consumerClosed.getCount(), "sender wait must not stop the engine directly");
        } finally {
            watcher.cancel();
            executor.shutdownNow();
        }
    }
}
