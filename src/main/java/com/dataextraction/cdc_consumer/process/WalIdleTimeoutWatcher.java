package com.dataextraction.cdc_consumer.process;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WalIdleTimeoutWatcher {

    private static final Logger log = LoggerFactory.getLogger(WalIdleTimeoutWatcher.class);
    private static final long CHECK_INTERVAL_SECONDS = 1;

    private final long timeoutSeconds;
    private final EngineShutdownCoordinator coordinator;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong lastCdcEventAtMs;
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
        this.lastCdcEventAtMs = new AtomicLong(System.currentTimeMillis());
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
        log.info("WAL idle timeout enabled: {} seconds", timeoutSeconds);
        checkTask = scheduler.scheduleAtFixedRate(
                this::checkIdle, CHECK_INTERVAL_SECONDS, CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public void resetOnCdcEvent() {
        lastCdcEventAtMs.set(System.currentTimeMillis());
    }

    private void checkIdle() {
        long elapsedMs = System.currentTimeMillis() - lastCdcEventAtMs.get();
        if (elapsedMs >= TimeUnit.SECONDS.toMillis(timeoutSeconds)) {
            log.info("No CDC record received for {} seconds; shutting down", timeoutSeconds);
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
