package com.dataextraction.cdc_consumer.process;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.pg.meta.PostgresLsn;

/**
 * Notices when the replication slot's {@code confirmed_flush_lsn} has reached the batch-end
 * (high-water) LSN captured at start. The default action stops the engine. A caller-supplied
 * action can open the post-mark idle window instead.
 *
 * <p>A change at the batch-end LSN stops the run. {@code confirmed_flush_lsn} stays put when the
 * tail has no row changes, so {@code pg_stat_replication.sent_lsn} is also a catch-up signal:
 * the walsender has read through the batch end. The idle timeout then shuts the process down
 * if no further WAL data arrives.
 */
public final class BatchEndCaughtUpWatcher {

    private static final Logger log = LoggerFactory.getLogger(BatchEndCaughtUpWatcher.class);
    private static final long CHECK_INTERVAL_SECONDS = 1;

    private final PostgresLsn batchEndLsn;
    private final Supplier<PostgresLsn> confirmedFlushLsn;
    private final Supplier<PostgresLsn> walSentLsn;
    private Supplier<Boolean> walSenderWaitingForWal;
    private Runnable onSenderWaitingForWal;
    private final EngineShutdownCoordinator coordinator;
    private final Runnable onCaughtUp;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> checkTask;
    private boolean shutdownRequested;
    private boolean senderWaitHandled;

    /**
     * When the walsender is in {@code WalSenderWaitForWAL} it has read every flushed record and
     * is idle. {@code sent_lsn} is hidden from roles without {@code pg_read_all_stats}, so this
     * wait event is the catch-up signal that lets the idle timeout stop the process.
     */
    public void setWalSenderWaitingForWal(Supplier<Boolean> walSenderWaitingForWal, Runnable onSenderWaitingForWal) {
        this.walSenderWaitingForWal = walSenderWaitingForWal;
        this.onSenderWaitingForWal = onSenderWaitingForWal;
    }

    public BatchEndCaughtUpWatcher(
            PostgresLsn batchEndLsn,
            Supplier<PostgresLsn> confirmedFlushLsn,
            EngineShutdownCoordinator coordinator) {
        this(batchEndLsn, confirmedFlushLsn, null, coordinator, null);
    }

    /**
     * @param onCaughtUp when non-null, invoked once instead of shutting the engine down. The
     *     caller decides whether to stop immediately or open the post-mark idle window.
     */
    public BatchEndCaughtUpWatcher(
            PostgresLsn batchEndLsn,
            Supplier<PostgresLsn> confirmedFlushLsn,
            EngineShutdownCoordinator coordinator,
            Runnable onCaughtUp) {
        this(batchEndLsn, confirmedFlushLsn, null, coordinator, onCaughtUp);
    }

    /**
     * @param walSentLsn walsender {@code sent_lsn}, or a supplier that returns null when the
     *     slot has no active sender. Reaching the batch end here means the unread WAL has been
     *     scanned, including a tail that produced no change events.
     */
    public BatchEndCaughtUpWatcher(
            PostgresLsn batchEndLsn,
            Supplier<PostgresLsn> confirmedFlushLsn,
            Supplier<PostgresLsn> walSentLsn,
            EngineShutdownCoordinator coordinator,
            Runnable onCaughtUp) {
        this.batchEndLsn = Objects.requireNonNull(batchEndLsn, "batchEndLsn");
        this.confirmedFlushLsn = Objects.requireNonNull(confirmedFlushLsn, "confirmedFlushLsn");
        this.walSentLsn = walSentLsn;
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.onCaughtUp = onCaughtUp;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "batch-end-caught-up");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        log.info(
                "Batch-end catch-up watch enabled; stop when confirmed_flush_lsn or sent_lsn >= {}",
                batchEndLsn);
        checkTask = scheduler.scheduleAtFixedRate(
                this::checkCaughtUp, CHECK_INTERVAL_SECONDS, CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void checkCaughtUp() {
        if (shutdownRequested) {
            return;
        }
        PostgresLsn flushLsn = readProgress(confirmedFlushLsn, "confirmed_flush_lsn");
        if (flushLsn != null && flushLsn.compareTo(batchEndLsn) >= 0) {
            log.info(
                    "Replication slot confirmed_flush_lsn {} reached batch end {}",
                    flushLsn,
                    batchEndLsn);
            finishCaughtUp();
            return;
        }
        if (walSentLsn != null) {
            PostgresLsn sentLsn = readProgress(walSentLsn, "sent_lsn");
            if (sentLsn != null && sentLsn.compareTo(batchEndLsn) >= 0) {
                log.info(
                        "Walsender sent_lsn {} reached batch end {}; unread WAL produced no further slot ack",
                        sentLsn,
                        batchEndLsn);
                finishCaughtUp();
                return;
            }
        }
        if (walSenderWaitingForWal == null || senderWaitHandled) {
            return;
        }
        Boolean waiting = readFlag(walSenderWaitingForWal, "walsender wait");
        if (Boolean.TRUE.equals(waiting)) {
            log.info("Walsender is waiting for new WAL; idle timeout applies");
            senderWaitHandled = true;
            if (onSenderWaitingForWal != null) {
                onSenderWaitingForWal.run();
            }
        }
    }

    private Boolean readFlag(Supplier<Boolean> source, String name) {
        try {
            return source.get();
        } catch (Exception e) {
            log.warn("Failed to read {} for batch-end check: {}", name, e.toString());
            return null;
        }
    }

    private PostgresLsn readProgress(Supplier<PostgresLsn> source, String name) {
        try {
            return source.get();
        } catch (Exception e) {
            log.warn("Failed to read {} for batch-end check: {}", name, e.toString());
            return null;
        }
    }

    private void finishCaughtUp() {
        if (shutdownRequested) {
            return;
        }
        shutdownRequested = true;
        cancel();
        if (onCaughtUp != null) {
            onCaughtUp.run();
        } else {
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
