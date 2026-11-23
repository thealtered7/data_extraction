package com.dataextraction.cdc_consumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Vector;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.buffer.ExtractBuffer;
import com.dataextraction.buffer.ExtractBufferFactory;
import com.dataextraction.buffer.ExtractBufferRequest;
import com.dataextraction.buffer.FlushedBufferHandler;
import com.dataextraction.cdc_consumer.format.RecordDeserializer;
import com.dataextraction.notify.FileWrittenObserver;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;
import com.dataextraction.schemaregistry.CdcSchemaRegistrar;
import com.dataextraction.schemaregistry.TableFqn;
import com.dataextraction.sink.ExtractRecordSink;
import com.dataextraction.sink.ExtractRecordSinkFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.StopEngineException;

public class SnapShotFileWriteConsumer<T> implements Consumer<ChangeEvent<T, T>>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SnapShotFileWriteConsumer.class);
    private static final Integer DEFAULT_MAX_BUFFER_SIZE_BYTES = 1024 * 1024 * 100;

    private static final String OUTCOME_SNAPSHOT_COMPLETED = "snapshot_completed";

    private Integer maxBufferSize;
    private Map<String, ExtractBuffer> bufferMap;
    private final Observability observability;
    private final Vector<FileWrittenObserver> fileWrittenObserver;
    private final String runGuid;
    private final String instanceName;
    private final String extractType;
    private final CdcSchemaRegistrar schemaRegistrar;
    private final FlushedBufferHandler flushedBufferHandler;
    private final ExtractRecordSinkFactory sinkFactory;
    private final RecordDeserializer<T> deserializer;
    private final RecordDelegateFactory delegateFactory;
    private final ExtractBufferFactory bufferFactory;

    private SnapShotFileWriteConsumer(Builder builder) {
        Objects.requireNonNull(builder.observability, "observability is required");
        Objects.requireNonNull(builder.runGuid, "runGuid is required");
        Objects.requireNonNull(builder.instanceName, "instanceName is required");
        Objects.requireNonNull(builder.extractType, "extractType is required");
        Objects.requireNonNull(builder.schemaRegistrar, "schemaRegistrar is required");
        Objects.requireNonNull(builder.flushedBufferHandler, "flushedBufferHandler is required");
        Objects.requireNonNull(builder.sinkFactory, "sinkFactory is required");
        Objects.requireNonNull(builder.deserializer, "deserializer is required");
        Objects.requireNonNull(builder.delegateFactory, "delegateFactory is required");
        Objects.requireNonNull(builder.bufferFactory, "bufferFactory is required");

        this.maxBufferSize = builder.maxBufferSize != null ? builder.maxBufferSize : DEFAULT_MAX_BUFFER_SIZE_BYTES;
        this.bufferMap = new HashMap<>();
        this.observability = builder.observability;
        this.fileWrittenObserver = new Vector<>(builder.fileWrittenObserver);
        this.runGuid = builder.runGuid;
        this.instanceName = builder.instanceName;
        this.extractType = builder.extractType;
        this.schemaRegistrar = builder.schemaRegistrar;
        this.flushedBufferHandler = builder.flushedBufferHandler;
        this.sinkFactory = builder.sinkFactory;
        this.deserializer = builder.deserializer;
        this.delegateFactory = builder.delegateFactory;
        this.bufferFactory = builder.bufferFactory;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private Integer maxBufferSize;
        private Observability observability;
        private final Vector<FileWrittenObserver> fileWrittenObserver = new Vector<>();
        private String runGuid;
        private String instanceName;
        private String extractType;
        private CdcSchemaRegistrar schemaRegistrar;
        private FlushedBufferHandler flushedBufferHandler;
        private ExtractRecordSinkFactory sinkFactory;
        private RecordDeserializer<T> deserializer;
        private RecordDelegateFactory delegateFactory;
        private ExtractBufferFactory bufferFactory;

        public Builder<T> withMaxBufferSize(Integer maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Builder<T> withObservability(Observability observability) {
            this.observability = observability;
            return this;
        }

        public Builder<T> addFileWrittenObserver(FileWrittenObserver fileWrittenObserver) {
            Objects.requireNonNull(fileWrittenObserver, "fileWrittenObserver is required");
            this.fileWrittenObserver.add(fileWrittenObserver);
            return this;
        }

        public Builder<T> withRunGuid(String runGuid) {
            this.runGuid = runGuid;
            return this;
        }

        public Builder<T> withInstanceName(String instanceName) {
            this.instanceName = instanceName;
            return this;
        }

        public Builder<T> withExtractType(String extractType) {
            this.extractType = extractType;
            return this;
        }

        public Builder<T> withSchemaRegistrar(CdcSchemaRegistrar schemaRegistrar) {
            this.schemaRegistrar = schemaRegistrar;
            return this;
        }

        public Builder<T> withFlushedBufferHandler(FlushedBufferHandler flushedBufferHandler) {
            this.flushedBufferHandler = flushedBufferHandler;
            return this;
        }

        public Builder<T> withSinkFactory(ExtractRecordSinkFactory sinkFactory) {
            this.sinkFactory = sinkFactory;
            return this;
        }

        public Builder<T> withDeserializer(RecordDeserializer<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public Builder<T> withDelegateFactory(RecordDelegateFactory delegateFactory) {
            this.delegateFactory = delegateFactory;
            return this;
        }

        public Builder<T> withBufferFactory(ExtractBufferFactory bufferFactory) {
            this.bufferFactory = bufferFactory;
            return this;
        }

        public SnapShotFileWriteConsumer<T> build() {
            return new SnapShotFileWriteConsumer<>(this);
        }
    }

    private ExtractBuffer getBuffer(String tableName) {
        ExtractBuffer buffer = this.bufferMap.get(tableName);
        if (buffer == null) {
            ExtractRecordSink recordSink = this.sinkFactory.create();
            buffer = this.bufferFactory.create(ExtractBufferRequest.builder()
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
                    .build());
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

    private void shutdownEngine() {
        throw new StopEngineException("shutting down engine after snapshot completed");
    }

    @Override
    public void accept(ChangeEvent<T, T> record) {
        observability.observeAcceptVoid(
                "accept",
                () -> acceptRecord(record),
                ex -> ex instanceof StopEngineException ? OUTCOME_SNAPSHOT_COMPLETED : "error");
    }

    private String acceptRecord(ChangeEvent<T, T> record) {
        try {
            RecordDelegate delegate = delegateFactory.create(
                    record.destination(),
                    deserializer.deserialize(record.destination(), record.key(), record.value()));
            if (delegate.isHeartbeat()) {
                log.debug("skipping heartbeat event");
                return "skipped_heartbeat";
            }
            if (delegate.isTombstone()) {
                log.debug("skipping delete tombstone event");
                return "skipped_tombstone";
            }
            if (!delegate.isSnapshotReadEvent()) {
                log.debug("skipping non-snapshot event");
                return "skipped_non_snapshot";
            }
            String tableName = delegate.getTableName();
            tagSource(tableName);
            ExtractBuffer buffer = this.getBuffer(tableName);
            int bytesWritten = buffer.write(delegate);
            if (bytesWritten == -1) {
                log.error("failed to write snapshot record");
                return "write_error";
            }
            log.debug("wrote {} bytes to buffer for table: {}", bytesWritten, tableName);
            if (delegate.isSnapshotLastEvent()) {
                log.info("Snapshot last event received for table {}; flushing buffer and stopping engine", tableName);
                buffer.flush();
                this.shutdownEngine();
            }
            return "success";
        } catch (StopEngineException e) {
            throw e;
        } catch (Exception e) {
            log.error("failed to write snapshot record", e);
            return "error";
        }
    }

    private void tagSource(String tableName) {
        TableFqn fqn = TableFqn.parse(tableName);
        observability.tagSource(instanceName, fqn.databaseName(), fqn.schemaName(), fqn.tableName());
    }

    public void init() throws IOException {
        log.info("initializing snapshot file writer");
    }

    public void shutdown() throws IOException {
        log.info("shutting down snapshot file writer");
        synchronized (this) {
            IOException firstError = null;
            for (Map.Entry<String, ExtractBuffer> entry : this.bufferMap.entrySet()) {
                try {
                    entry.getValue().close();
                } catch (IOException e) {
                    log.error("failed to close snapshot buffer for table {}", entry.getKey(), e);
                    if (firstError == null) {
                        firstError = e;
                    } else {
                        firstError.addSuppressed(e);
                    }
                }
            }
            this.bufferMap.clear();
            RuntimeException observerError = null;
            for (FileWrittenObserver observer : this.fileWrittenObserver) {
                try {
                    observer.flush();
                } catch (RuntimeException e) {
                    log.error("failed to flush file written observer {}", observer.getClass().getName(), e);
                    if (observerError == null) {
                        observerError = e;
                    } else {
                        observerError.addSuppressed(e);
                    }
                }
            }
            if (firstError != null) {
                if (observerError != null) {
                    firstError.addSuppressed(observerError);
                }
                throw firstError;
            }
            if (observerError != null) {
                throw observerError;
            }
            closeDeserializer();
        }
    }

    private void closeDeserializer() {
        try {
            deserializer.close();
        } catch (Exception e) {
            log.debug("Failed to close change deserializer", e);
        }
    }

    @Override
    public void close() throws IOException {
        shutdown();
    }
}
