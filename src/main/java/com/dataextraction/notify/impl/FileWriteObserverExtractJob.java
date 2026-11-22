package com.dataextraction.notify.impl;

import java.util.Objects;

import com.dataextraction.datapipelines.ExtractJobReporter;
import com.dataextraction.notify.FileWrittenObserver;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;

/**
 * Accumulates flushed-file metrics for the extract job. The HTTP update is sent once
 * from {@link ExtractJobReporter#finish()}, not from each flush.
 */
public final class FileWriteObserverExtractJob implements FileWrittenObserver {

    private final ExtractJobReporter reporter;
    private final Observability observability;

    public FileWriteObserverExtractJob(ExtractJobReporter reporter, Observability observability) {
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.observability = Objects.requireNonNull(observability, "observability");
    }

    @Override
    public void observe(FileWriteFlushEvent event) {
        if (event == null) {
            return;
        }
        try {
            observability.observeCallableVoid(
                    Observability.FILE_WRITES_PREFIX,
                    "extract_metrics",
                    Observability.sourceTags(
                            event.getSourceInstanceName(),
                            event.getSourceDatabaseName(),
                            event.getSourceSchemaName(),
                            event.getSourceTableName()),
                    () -> {
                        reporter.recordFlush(event);
                        return "success";
                    });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to record extract flush metrics", e);
        }
    }

    @Override
    public void flush() {
    }
}
