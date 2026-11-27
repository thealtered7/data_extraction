package com.dataextraction.cdc_consumer.process;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stops the engine after a quiet stretch. The countdown starts when the watcher starts. A WAL
 * change resets it. When catch-up is bounded by a high water mark, expiry shuts down only after
 * that mark has been passed.
 */
public final class WalIdleTimeoutWatcher {

    private static final Logger log = LoggerFactory.getLogger(WalIdleTimeoutWatcher.class);
    private static final long CHECK_INTERVAL_SECONDS = 1;

    private final long timeoutSeconds;
    private final EngineShutdownCoordinator coordinator;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong lastWalDataAtMs = new AtomicLong(0);
    private final AtomicBoolean idleShutdownEnabled = new AtomicBoolean(true);
    private ScheduledFuture<?> checkTask;

    public WalIdleTimeoutWatcher(long timeoutSeconds, EngineShutdownCoordinator coordinator) {
        if (timeoutSeconds <= 0) {
            throw new IllegalArgumentException("timeoutSeconds must be positive");
        }
        this.timeoutSeconds = timeoutSeconds;
        this.coordinator = coordinator;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "wal-idle-timeout");
            thread.setDaemon(true);
            return thread;
        });
    }

    public static WalIdleTimeoutWatcher createOptional(String timeoutProperty, EngineShutdownCoordinator coordinator) {
        long timeoutSeconds = parseTimeoutSeconds(timeoutProperty);
        if (timeoutSeconds <= 0) {
            return null;
        }
        return new WalIdleTimeoutWatcher(timeoutSeconds, coordinator);
    }

    static long parseTimeoutSeconds(String timeoutProperty) {
        if (timeoutProperty == null || timeoutProperty.isBlank()) {
            return 0;
        }
        try {
            return Long.parseLong(timeoutProperty.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid app.wal.idle.timeout.seconds value '{}'; disabling idle timeout", timeoutProperty);
            return 0;
        }
    }

    public void start() {
        lastWalDataAtMs.set(System.currentTimeMillis());
        log.info("WAL idle timeout enabled: {} seconds", timeoutSeconds);
        checkTask = scheduler.scheduleAtFixedRate(
                this::checkIdle, CHECK_INTERVAL_SECONDS, CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * High-water-mark runs keep reading through the initial WAL scan. Idle expiry cannot stop
     * the process until {@link #markPastHighWaterMark()} runs.
     */
    public void deferIdleShutdownUntilPastHighWaterMark() {
        idleShutdownEnabled.set(false);
        log.info("Idle timeout will not shut down until the high water mark is passed");
    }

    /**
     * The walsender has read every flushed WAL record and is waiting. The countdown that started
     * with the watcher can now stop the process; this does not reset that clock.
     */
    public void allowIdleShutdown() {
        if (idleShutdownEnabled.compareAndSet(false, true)) {
            log.info("Walsender is caught up; idle timeout can shut down");
        }
    }

    public void onWalDataRead() {
        lastWalDataAtMs.set(System.currentTimeMillis());
    }

    /**
     * The slot has reached the batch-end LSN. Further WAL data shuts the engine down immediately;
     * this countdown shuts it down when no further data arrives.
     */
    public void markPastHighWaterMark() {
        lastWalDataAtMs.set(System.currentTimeMillis());
        idleShutdownEnabled.set(true);
        log.info(
                "Passed high water mark; shutting down on further WAL data or after {} seconds with no further data",
                timeoutSeconds);
    }

    private void checkIdle() {
        if (!idleShutdownEnabled.get()) {
            return;
        }
        long elapsedMs = System.currentTimeMillis() - lastWalDataAtMs.get();
        if (elapsedMs >= TimeUnit.SECONDS.toMillis(timeoutSeconds)) {
            log.info("No WAL data for {} seconds; shutting down", timeoutSeconds);
            cancel();
            coordinator.shutdown();
        }
    }

    public void cancel() {
        if (checkTask != null) {
            checkTask.cancel(false);
            checkTask = null;
        }
        scheduler.shutdown();
    }
}
