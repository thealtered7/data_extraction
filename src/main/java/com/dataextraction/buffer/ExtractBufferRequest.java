package com.dataextraction.buffer;

import java.util.Objects;
import java.util.function.Consumer;

import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;
import com.dataextraction.schemaregistry.CdcSchemaRegistrar;
import com.dataextraction.sink.ExtractRecordSink;

public final class ExtractBufferRequest {

    private final Integer maxBufferSize;
    private final ExtractRecordSink sink;
    private final String tableName;
    private final String instanceName;
    private final Observability observability;
    private final String extractType;
    private final String extractJobId;
    private final CdcSchemaRegistrar schemaRegistrar;
    private final FlushedBufferHandler flushedBufferHandler;
    private final Consumer<FileWriteFlushEvent> onFlush;

    private ExtractBufferRequest(Builder builder) {
        this.maxBufferSize = Objects.requireNonNull(builder.maxBufferSize, "maxBufferSize is required");
        this.sink = Objects.requireNonNull(builder.sink, "sink is required");
        this.tableName = Objects.requireNonNull(builder.tableName, "tableName is required");
        this.instanceName = Objects.requireNonNull(builder.instanceName, "instanceName is required");
        this.observability = Objects.requireNonNull(builder.observability, "observability is required");
        this.extractType = Objects.requireNonNull(builder.extractType, "extractType is required");
        this.extractJobId = Objects.requireNonNull(builder.extractJobId, "extractJobId is required");
        this.schemaRegistrar = Objects.requireNonNull(builder.schemaRegistrar, "schemaRegistrar is required");
        this.flushedBufferHandler = Objects.requireNonNull(builder.flushedBufferHandler, "flushedBufferHandler is required");
        this.onFlush = Objects.requireNonNull(builder.onFlush, "onFlush is required");
    }

    public static Builder builder() {
        return new Builder();
    }

    public Integer getMaxBufferSize() {
        return maxBufferSize;
    }

    public ExtractRecordSink getSink() {
        return sink;
    }

    public String getTableName() {
        return tableName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public Observability getObservability() {
        return observability;
    }

    public String getExtractType() {
        return extractType;
    }

    public String getExtractJobId() {
        return extractJobId;
    }

    public CdcSchemaRegistrar getSchemaRegistrar() {
        return schemaRegistrar;
    }

    public FlushedBufferHandler getFlushedBufferHandler() {
        return flushedBufferHandler;
    }

    public Consumer<FileWriteFlushEvent> getOnFlush() {
        return onFlush;
    }

    public static final class Builder {
        private Integer maxBufferSize;
        private ExtractRecordSink sink;
        private String tableName;
        private String instanceName;
        private Observability observability;
        private String extractType;
        private String extractJobId;
        private CdcSchemaRegistrar schemaRegistrar;
        private FlushedBufferHandler flushedBufferHandler;
        private Consumer<FileWriteFlushEvent> onFlush;

        public Builder withMaxBufferSize(Integer maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Builder withSink(ExtractRecordSink sink) {
            this.sink = sink;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withInstanceName(String instanceName) {
            this.instanceName = instanceName;
            return this;
        }

        public Builder withObservability(Observability observability) {
            this.observability = observability;
            return this;
        }

        public Builder withExtractType(String extractType) {
            this.extractType = extractType;
            return this;
        }

        public Builder withExtractJobId(String extractJobId) {
            this.extractJobId = extractJobId;
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

        public Builder withOnFlush(Consumer<FileWriteFlushEvent> onFlush) {
            this.onFlush = onFlush;
            return this;
        }

        public ExtractBufferRequest build() {
            return new ExtractBufferRequest(this);
        }
    }
}
