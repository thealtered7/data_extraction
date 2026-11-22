package com.dataextraction.cdc_consumer.impl;

import com.dataextraction.buffer.ExtractJsonBuffer;
import com.dataextraction.buffer.FlushedBufferHandler;
import com.dataextraction.notify.FileWrittenObserver;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.pg.meta.PostgresLsn;
import com.dataextraction.cdc_consumer.process.EngineShutdownCoordinator;
import com.dataextraction.cdc_consumer.process.WalIdleTimeoutWatcher;
import com.dataextraction.sink.ExtractRecordSink;
import com.dataextraction.sink.ExtractRecordSinkFactory;
import com.dataextraction.cdc_consumer.JsonRecordDelegate;
import com.dataextraction.observability.Observability;
import com.dataextraction.schemaregistry.CdcSchemaRegistrar;
import com.dataextraction.schemaregistry.TableFqn;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.StopEngineException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Vector;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferedFileWriteConsumer implements Consumer<ChangeEvent<String, String>>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BufferedFileWriteConsumer.class);
    private static final Integer DEFAULT_MAX_BUFFER_SIZE_BYTES = 1024 * 1024 * 100;

    private static final String OUTCOME_HIGH_WATER_MARK_REACHED = "high_water_mark_reached";

    private Integer maxBufferSize;
    private Map<String, ExtractJsonBuffer> bufferMap;
    private PostgresLsn highWaterMark;
    private PostgresLsn effectiveHighWaterMark;
    private EngineShutdownCoordinator engineShutdownCoordinator;
    private WalIdleTimeoutWatcher walIdleTimeoutWatcher;
    private final Observability observability;
    private final Vector<FileWrittenObserver> fileWrittenObserver;
    private final String runGuid;
    private final String instanceName;
    private final String extractType;
    private final CdcSchemaRegistrar schemaRegistrar;
    private final FlushedBufferHandler flushedBufferHandler;
    private final ExtractRecordSinkFactory extractRecordSinkFactory;

    private BufferedFileWriteConsumer(Builder builder) {
        Objects.requireNonNull(builder.observability, "observability is required");
        Objects.requireNonNull(builder.fileWrittenObserver, "fileWrittenObserver is required");
        Objects.requireNonNull(builder.runGuid, "runGuid is required");
        Objects.requireNonNull(builder.instanceName, "instanceName is required");
        Objects.requireNonNull(builder.extractType, "extractType is required");
        Objects.requireNonNull(builder.schemaRegistrar, "schemaRegistrar is required");
        Objects.requireNonNull(builder.flushedBufferHandler, "flushedBufferHandler is required");
        Objects.requireNonNull(builder.extractRecordSinkFactory, "extractRecordSinkFactory is required");

        this.maxBufferSize = builder.maxBufferSize != null ? builder.maxBufferSize : DEFAULT_MAX_BUFFER_SIZE_BYTES;
        this.bufferMap = new HashMap<>();
        this.observability = builder.observability;
        this.fileWrittenObserver = builder.fileWrittenObserver;
        this.runGuid = builder.runGuid;
        this.instanceName = builder.instanceName;
        this.extractType = builder.extractType;
        this.schemaRegistrar = builder.schemaRegistrar;
        this.flushedBufferHandler = builder.flushedBufferHandler;
        this.extractRecordSinkFactory = builder.extractRecordSinkFactory;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Integer maxBufferSize;
        private Observability observability;
        private Vector<FileWrittenObserver> fileWrittenObserver;
        private String runGuid;
        private String instanceName;
        private String extractType;
        private CdcSchemaRegistrar schemaRegistrar;
        private FlushedBufferHandler flushedBufferHandler;
        private ExtractRecordSinkFactory extractRecordSinkFactory;

        public Builder withMaxBufferSize(Integer maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Builder withObservability(Observability observability) {
            this.observability = observability;
            return this;
        }

        public Builder withFileWrittenObserver(Vector<FileWrittenObserver> fileWrittenObserver) {
            this.fileWrittenObserver = fileWrittenObserver;
            return this;
        }

        public Builder withRunGuid(String runGuid) {
            this.runGuid = runGuid;
            return this;
        }

        public Builder withInstanceName(String instanceName) {
            this.instanceName = instanceName;
            return this;
        }

        public Builder withExtractType(String extractType) {
            this.extractType = extractType;
            return this;
        }

        public Builder withSchemaRegistrar(CdcSchemaRegistrar schemaRegistrar) {
            this.schemaRegistrar = schemaRegistrar;
            return this;
        }

        public Builder withFlushedBufferHandler(FlushedBufferHandler flushedBufferHandler) {
            this.flushedBufferHandler = flushedBufferHandler;
            return this;
        }

        public Builder withExtractRecordSinkFactory(ExtractRecordSinkFactory extractRecordSinkFactory) {
            this.extractRecordSinkFactory = extractRecordSinkFactory;
            return this;
        }

        public BufferedFileWriteConsumer build() {
            return new BufferedFileWriteConsumer(this);
        }
    }

    public void setHighWaterMark(PostgresLsn highWaterMark) {
        this.highWaterMark = highWaterMark;
        this.effectiveHighWaterMark = highWaterMark;
    }

    public void setEngineShutdownCoordinator(EngineShutdownCoordinator engineShutdownCoordinator) {
        this.engineShutdownCoordinator = engineShutdownCoordinator;
    }

    public EngineShutdownCoordinator getEngineShutdownCoordinator() {
        return this.engineShutdownCoordinator;
    }

    public void setWalIdleTimeoutWatcher(WalIdleTimeoutWatcher walIdleTimeoutWatcher) {
        this.walIdleTimeoutWatcher = walIdleTimeoutWatcher;
    }

    private ExtractJsonBuffer getBuffer(String tableName) {
        ExtractJsonBuffer buffer = this.bufferMap.get(tableName);
        if (buffer == null) {

            ExtractRecordSink recordSink = this.extractRecordSinkFactory.create();
            buffer = ExtractJsonBuffer.builder()
                .withTableName(tableName)
                .withExtractType(extractType)       
                .withExtractJobId(this.runGuid)
                .withMaxBufferSize(this.maxBufferSize)
                .withObservability(this.observability)
                .withOnFlush(this::notifyFileWrittenObservers)
                .withInstanceName(this.instanceName)
                .withSchemaRegistrar(this.schemaRegistrar)                
                .withSink(recordSink)
                .withFlushedBufferHandler(this.flushedBufferHandler)
                .build();
            this.bufferMap.put(tableName, buffer);
        }
        return buffer;
    }

    private void notifyFileWrittenObservers(FileWriteFlushEvent event) {
        if (this.fileWrittenObserver == null || this.fileWrittenObserver.isEmpty()) {
            return;
        }
        this.fileWrittenObserver.forEach(observer -> observer.observe(event));
    }

    private PostgresLsn resolveEffectiveHighWaterMark() {
        if (effectiveHighWaterMark != null) {
            return effectiveHighWaterMark;
        }
        effectiveHighWaterMark = highWaterMark;
        return effectiveHighWaterMark;
    }

    private boolean shouldContinue(Long recordLsn) {
        if (recordLsn == null) {
            log.warn("recordLsn is null on call to shouldContinue; continuing");
            return true;
        }
        PostgresLsn limit = resolveEffectiveHighWaterMark();
        if (limit == null) {
            return true;
        }
        PostgresLsn recordLsnValue = PostgresLsn.fromDebeziumLsn(recordLsn);
        boolean ret = recordLsnValue.compareTo(limit) <= 0;
        log.debug(
                "shouldContinue: recordLsn={} ({}) batchEndLsn={} ({}) -> {}",
                recordLsn,
                recordLsnValue,
                limit.toLong(),
                limit,
                ret);
        return ret;
    }

    private void shutdownEngine() {
        throw new StopEngineException("shutting down engine due to high water mark");
    }

    @Override
    public void accept(ChangeEvent<String, String> record) {
        observability.observeAcceptVoid(
                "accept",
                () -> acceptRecord(record),
                ex -> ex instanceof StopEngineException ? OUTCOME_HIGH_WATER_MARK_REACHED : "error");
    }

    private String acceptRecord(ChangeEvent<String, String> record) {
        try {
            if (JsonRecordDelegate.isHeartbeat(record.key(), record.value())) {
                log.debug("skipping heartbeat event");
                return "skipped_heartbeat";
            }
            if (JsonRecordDelegate.isTombstone(record.value())) {
                log.debug("skipping delete tombstone event");
                return "skipped_tombstone";
            }
            JsonRecordDelegate jsonRecordDelegate = new JsonRecordDelegate(record.value());
            if (!jsonRecordDelegate.isCdcChangeEvent()) {
                log.debug("skipping non-CDC event");
                return "skipped_non_cdc";
            }
            if (walIdleTimeoutWatcher != null) {
                walIdleTimeoutWatcher.resetOnCdcEvent();
            }
            String tableName = jsonRecordDelegate.getTableName();
            TableFqn fqn = TableFqn.parse(tableName);
            observability.tagSource(instanceName, fqn.databaseName(), fqn.schemaName(), fqn.tableName());
            Long recordLsn = jsonRecordDelegate.getLsn();
            ExtractJsonBuffer buffer = this.getBuffer(tableName);
            int bytesWritten = buffer.write(record);
            if (bytesWritten == -1) {
                log.error("failed to write CDC record");
                return "write_error";
            }
            log.debug("wrote {} bytes to buffer for table: {}", bytesWritten, tableName);
            if (!shouldContinue(recordLsn)) {
                log.info(
                        "Record LSN {} is past batch end LSN {}; stopping engine after processing this record",
                        recordLsn,
                        resolveEffectiveHighWaterMark());
                this.shutdownEngine();
            }
            return "success";
        } catch (StopEngineException e) {
            throw e;
        } catch (Exception e) {
            log.error("failed to write CDC record", e);
            return "error";
        }
    }

    public void init() throws IOException {
        log.info("initializing file writer");
    }

    public void shutdown() throws IOException {
        log.info("shutting down file writer");
        synchronized (this) {
            IOException firstError = null;
            for (ExtractJsonBuffer buffer : this.bufferMap.values()) {
                try {
                    buffer.close();
                } catch (IOException e) {
                    if (firstError == null) {
                        firstError = e;
                    } else {
                        firstError.addSuppressed(e);
                    }
                }
            }
            this.bufferMap.clear();
            if (!this.fileWrittenObserver.isEmpty()) {
                this.fileWrittenObserver.forEach(FileWrittenObserver::flush);
            }
            if (firstError != null) {
                throw firstError;
            }
        }
    }

    @Override
    public void close() throws IOException {
        shutdown();
    }
}
