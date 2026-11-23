package com.dataextraction.cdc_consumer.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.dataextraction.buffer.ExtractBufferFactory;
import com.dataextraction.buffer.FlushedBufferHandler;
import com.dataextraction.cdc_consumer.RecordDelegateFactory;
import com.dataextraction.cdc_consumer.SnapShotFileWriteConsumer;
import com.dataextraction.notify.FileWrittenObserver;
import com.dataextraction.observability.Observability;
import com.dataextraction.schemaregistry.CdcSchemaRegistrar;
import com.dataextraction.sink.ExtractRecordSinkFactory;

public final class SnapshotDependencies {

    private final Integer maxBufferSize;
    private final Observability observability;
    private final List<FileWrittenObserver> observers;
    private final String runGuid;
    private final String instanceName;
    private final String extractType;
    private final CdcSchemaRegistrar schemaRegistrar;
    private final FlushedBufferHandler flushedBufferHandler;
    private final ExtractRecordSinkFactory sinkFactory;

    private SnapshotDependencies(Builder builder) {
        this.maxBufferSize = builder.maxBufferSize;
        this.observability = Objects.requireNonNull(builder.observability, "observability is required");
        this.observers = List.copyOf(builder.observers);
        this.runGuid = Objects.requireNonNull(builder.runGuid, "runGuid is required");
        this.instanceName = Objects.requireNonNull(builder.instanceName, "instanceName is required");
        this.extractType = Objects.requireNonNull(builder.extractType, "extractType is required");
        this.schemaRegistrar = Objects.requireNonNull(builder.schemaRegistrar, "schemaRegistrar is required");
        this.flushedBufferHandler = Objects.requireNonNull(builder.flushedBufferHandler, "flushedBufferHandler is required");
        this.sinkFactory = Objects.requireNonNull(builder.sinkFactory, "sinkFactory is required");
    }

    public static Builder builder() {
        return new Builder();
    }

    public <T> SnapShotFileWriteConsumer<T> build(
            RecordDeserializer<T> deserializer,
            RecordDelegateFactory delegateFactory,
            ExtractBufferFactory bufferFactory) {
        SnapShotFileWriteConsumer.Builder<T> consumer = SnapShotFileWriteConsumer.<T>builder()
                .withMaxBufferSize(maxBufferSize)
                .withObservability(observability)
                .withRunGuid(runGuid)
                .withInstanceName(instanceName)
                .withExtractType(extractType)
                .withSchemaRegistrar(schemaRegistrar)
                .withFlushedBufferHandler(flushedBufferHandler)
                .withSinkFactory(sinkFactory)
                .withDeserializer(deserializer)
                .withDelegateFactory(delegateFactory)
                .withBufferFactory(bufferFactory);
        for (FileWrittenObserver observer : observers) {
            consumer.addFileWrittenObserver(observer);
        }
        return consumer.build();
    }

    public static final class Builder {
        private Integer maxBufferSize;
        private Observability observability;
        private final List<FileWrittenObserver> observers = new ArrayList<>();
        private String runGuid;
        private String instanceName;
        private String extractType;
        private CdcSchemaRegistrar schemaRegistrar;
        private FlushedBufferHandler flushedBufferHandler;
        private ExtractRecordSinkFactory sinkFactory;

        public Builder withMaxBufferSize(Integer maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Builder withObservability(Observability observability) {
            this.observability = observability;
            return this;
        }

        public Builder addFileWrittenObserver(FileWrittenObserver observer) {
            Objects.requireNonNull(observer, "observer is required");
            this.observers.add(observer);
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

        public Builder withSinkFactory(ExtractRecordSinkFactory sinkFactory) {
            this.sinkFactory = sinkFactory;
            return this;
        }

        public SnapshotDependencies build() {
            return new SnapshotDependencies(this);
        }
    }
}
