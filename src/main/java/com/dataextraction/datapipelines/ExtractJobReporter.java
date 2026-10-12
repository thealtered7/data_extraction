package com.dataextraction.datapipelines;

import com.dataextraction.datapipelines.requests.ExtractRequest;
import com.dataextraction.notify.events.FileWriteFlushEvent;

import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports extract job lifecycle to datapipelines: POST on start ({@code STARTING}),
 * accumulate flushed-file metrics, then PUT once on finish ({@code FINISHED} or {@code FAILED}).
 * HTTP failures are logged and do not fail the extract. If POST never returned an id, PUT is skipped.
 */
public final class ExtractJobReporter {

    private static final Logger log = LoggerFactory.getLogger(ExtractJobReporter.class);

    public static final String STATUS_STARTING = "STARTING";
    public static final String STATUS_FINISHED = "FINISHED";
    public static final String STATUS_FAILED = "FAILED";

    private final DatapipelinesHttpClient client;
    private final String extractJobId;
    private final String extractType;
    private final String catalogName;
    private final String databaseName;
    private final String sourceInstanceName;

    private final AtomicReference<Instant> extractStartAt = new AtomicReference<>();
    private final AtomicReference<Long> extractId = new AtomicReference<>();
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicBoolean finished = new AtomicBoolean(false);

    private final AtomicLong rowCount = new AtomicLong();
    private final AtomicLong bytesCount = new AtomicLong();
    private final AtomicReference<Long> sourceMinLsn = new AtomicReference<>();
    private final AtomicReference<Long> sourceMaxLsn = new AtomicReference<>();
    private final Set<String> tables = new HashSet<>();
    private final Object tablesLock = new Object();

    public ExtractJobReporter(
            DatapipelinesHttpClient client,
            String extractJobId,
            String extractType,
            String catalogName,
            String databaseName,
            String sourceInstanceName) {
        this.client = Objects.requireNonNull(client, "client");
        this.extractJobId = Objects.requireNonNull(extractJobId, "extractJobId");
        this.extractType = Objects.requireNonNull(extractType, "extractType");
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.sourceInstanceName = sourceInstanceName;
    }

    /**
     * Returns a reporter when the HTTP client is non-null; otherwise null (reporting disabled).
     */
    public static ExtractJobReporter createOptional(
            DatapipelinesHttpClient client,
            String extractJobId,
            String extractType,
            String catalogName,
            String databaseName,
            String sourceInstanceName) {
        if (client == null) {
            return null;
        }
        return new ExtractJobReporter(
                client, extractJobId, extractType, catalogName, databaseName, sourceInstanceName);
    }

    /**
     * POSTs an extract with status STARTING. Safe to call once at process start.
     */
    public void start() {
        Instant startAt = Instant.now();
        extractStartAt.set(startAt);
        ExtractRequest request = new ExtractRequest(
                extractJobId,
                extractType,
                startAt,
                null,
                STATUS_STARTING,
                catalogName,
                null,
                databaseName,
                sourceInstanceName,
                null,
                null,
                0L,
                0L,
                0L);
        try {
            long id = client.postExtract(request);
            extractId.set(id);
        } catch (RuntimeException e) {
            log.error("Failed to POST extract start for job {}", extractJobId, e);
        }
    }

    /**
     * Accumulates metrics from a flushed file. Thread-safe.
     */
    public void recordFlush(FileWriteFlushEvent event) {
        if (event == null) {
            return;
        }
        rowCount.addAndGet(event.rowCount());
        bytesCount.addAndGet(event.fileSize());
        Long flushMinLsn = event.sourceMinLsn();
        if (flushMinLsn != null) {
            sourceMinLsn.updateAndGet(current -> current == null || flushMinLsn < current ? flushMinLsn : current);
        }
        Long flushMaxLsn = event.sourceMaxLsn();
        if (flushMaxLsn != null) {
            sourceMaxLsn.updateAndGet(current -> current == null || flushMaxLsn > current ? flushMaxLsn : current);
        }
        String tableFqn = event.databaseName() + "." + event.schemaName() + "." + event.tableName();
        synchronized (tablesLock) {
            tables.add(tableFqn);
        }
    }

    /**
     * Marks the extract as failed so {@link #finish()} sends status FAILED.
     * Call from the Debezium completion callback on a real engine error
     * (not {@code StopEngineException}).
     */
    public void markFailed() {
        failed.set(true);
    }

    /**
     * PUTs the extract once with FINISHED or FAILED, metrics, and extract_end_at.
     * Skipped if POST never returned an id. Idempotent.
     */
    public void finish() {
        if (!finished.compareAndSet(false, true)) {
            return;
        }
        Long id = extractId.get();
        if (id == null) {
            log.warn("Skipping extract finish PUT for job {}; no extract id from start POST", extractJobId);
            return;
        }
        Instant startAt = extractStartAt.get();
        if (startAt == null) {
            startAt = Instant.now();
        }
        Instant endAt = Instant.now();
        String status = failed.get() ? STATUS_FAILED : STATUS_FINISHED;
        long tableCount;
        synchronized (tablesLock) {
            tableCount = tables.size();
        }
        ExtractRequest request = new ExtractRequest(
                extractJobId,
                extractType,
                startAt,
                endAt,
                status,
                catalogName,
                null,
                databaseName,
                sourceInstanceName,
                sourceMinLsn.get(),
                sourceMaxLsn.get(),
                tableCount,
                rowCount.get(),
                bytesCount.get());
        try {
            client.putExtract(id, request);
        } catch (RuntimeException e) {
            log.error("Failed to PUT extract finish for job {} id {}", extractJobId, id, e);
        }
    }
}
