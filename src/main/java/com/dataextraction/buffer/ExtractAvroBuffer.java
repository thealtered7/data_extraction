package com.dataextraction.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.cdc_consumer.RecordDelegate;
import com.dataextraction.cdc_consumer.format.DeserializedRecord;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;
import com.dataextraction.schemaregistry.TableFqn;
import com.dataextraction.sink.ExtractOutputFormat;
import com.dataextraction.sink.ExtractRecordSink;

/**
 * Buffers Debezium Avro values and writes them through the Avro sink. Schema ids come from the
 * Confluent wire header. The schema documents are not registered again: the converter already
 * stored them, the file header carries the writer schema, and the key schema is file metadata.
 */
public final class ExtractAvroBuffer implements ExtractBuffer {

    private static final Logger log = LoggerFactory.getLogger(ExtractAvroBuffer.class);
    private static final String WRITE_TYPE = "raw";

    private final Integer maxBufferSize;
    private final ExtractRecordSink sink;
    private final String tableName;
    private final String instanceName;
    private final Observability observability;
    private final String extractType;
    private final String extractJobId;
    private final Consumer<FileWriteFlushEvent> onFlush;
    private final FlushedBufferHandler flushedBufferHandler;

    private Path tempFile;
    private Instant extractStartAt;
    private String bufferId;
    private long rowCount;
    private long bytesWritten;
    private String keySchemaId;
    private String valueSchemaId;
    private long keySchemaHash;
    private long valueSchemaHash;
    private Long sourceMinLsn;
    private Long sourceMaxLsn;

    private ExtractAvroBuffer(Builder builder) {
        Objects.requireNonNull(builder.maxBufferSize, "maxBufferSize is required");
        Objects.requireNonNull(builder.sink, "sink is required");
        Objects.requireNonNull(builder.tableName, "tableName is required");
        Objects.requireNonNull(builder.instanceName, "instanceName is required");
        Objects.requireNonNull(builder.observability, "observability is required");
        Objects.requireNonNull(builder.extractType, "extractType is required");
        Objects.requireNonNull(builder.extractJobId, "extractJobId is required");
        Objects.requireNonNull(builder.onFlush, "onFlush is required");
        Objects.requireNonNull(builder.flushedBufferHandler, "flushedBufferHandler is required");
        this.maxBufferSize = builder.maxBufferSize;
        this.sink = builder.sink;
        this.tableName = builder.tableName;
        this.instanceName = builder.instanceName;
        this.observability = builder.observability;
        this.extractType = builder.extractType;
        this.extractJobId = builder.extractJobId;
        this.onFlush = builder.onFlush;
        this.flushedBufferHandler = builder.flushedBufferHandler;
        this.sink.bindTelemetry(this.observability, tableTags());
        openTempFile();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ExtractOutputFormat outputFormat() {
        return this.sink.getOutputFormat();
    }

    @Override
    public int write(RecordDelegate record) throws IOException {
        final int[] written = {-1};
        observability.observeOperationVoid(
                Observability.CDC_EVENT_WRITTEN_TO_BUFFER_COUNTER,
                "write",
                tableTags(),
                () -> {
                    written[0] = writeRecord(record);
                    return written[0] == -1 ? "write_error" : "success";
                });
        return written[0];
    }

    private int writeRecord(RecordDelegate record) {
        try {
            DeserializedRecord decoded = record.decoded();
            if (!(decoded.value() instanceof GenericRecord value)) {
                log.error("Avro buffer expected a GenericRecord value for table {}", tableName);
                return -1;
            }
            String nextKeySchemaId = decoded.keySchemaId();
            String nextValueSchemaId = decoded.valueSchemaId();
            long nextKeySchemaHash = schemaHash(schemaIdentity(nextKeySchemaId, decoded.avroKeySchema()));
            long nextValueSchemaHash = schemaHash(schemaIdentity(nextValueSchemaId, decoded.avroValueSchema()));

            boolean schemaDrift = this.bytesWritten > 0
                    && hasSchemaDrift(nextKeySchemaId, nextKeySchemaHash, nextValueSchemaId, nextValueSchemaHash);
            if (schemaDrift) {
                log.info("schema drift detected for table {}; flushing buffer", tableName);
                flushBuffer();
            }

            int estimatedSize = Math.max(1, value.toString().length());
            if (this.bytesWritten > 0 && this.bytesWritten + estimatedSize > maxBufferSize) {
                flushBuffer();
            }

            captureSchemas(nextKeySchemaId, nextKeySchemaHash, nextValueSchemaId, nextValueSchemaHash);
            trackSourceLsn(record.getLsn());
            String extractedAt = Instant.now().truncatedTo(ChronoUnit.MICROS).toString();
            int written = this.sink.writeAvro(
                    value,
                    decoded.avroKeySchema(),
                    extractJobId,
                    bufferId,
                    extractType,
                    extractedAt);
            this.bytesWritten += written;
            rowCount++;
            return written;
        } catch (Exception e) {
            log.error("failed to write Avro CDC record", e);
            return -1;
        }
    }

    private boolean hasSchemaDrift(
            String nextKeySchemaId,
            long nextKeySchemaHash,
            String nextValueSchemaId,
            long nextValueSchemaHash) {
        return schemaDrifted(this.keySchemaId, this.keySchemaHash, nextKeySchemaId, nextKeySchemaHash)
                || schemaDrifted(this.valueSchemaId, this.valueSchemaHash, nextValueSchemaId, nextValueSchemaHash);
    }

    private static boolean schemaDrifted(
            String currentId, long currentHash, String nextId, long nextHash) {
        if (nextHash == 0L) {
            return false;
        }
        if (currentHash == 0L) {
            return false;
        }
        if (nextId != null && currentId != null) {
            return !nextId.equals(currentId);
        }
        return currentHash != nextHash;
    }

    private static long schemaHash(String identity) {
        if (identity == null || identity.isBlank()) {
            return 0L;
        }
        CRC32C crc = new CRC32C();
        crc.update(identity.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }

    private static String schemaIdentity(String schemaId, Schema schema) {
        if (schemaId != null && !schemaId.isBlank()) {
            return "id:" + schemaId;
        }
        if (schema == null) {
            return null;
        }
        return "schema:" + schema.toString();
    }

    private void captureSchemas(
            String nextKeySchemaId, long nextKeySchemaHash, String nextValueSchemaId, long nextValueSchemaHash) {
        if (nextKeySchemaHash != 0L) {
            this.keySchemaId = nextKeySchemaId;
            this.keySchemaHash = nextKeySchemaHash;
        }
        if (nextValueSchemaHash != 0L) {
            this.valueSchemaId = nextValueSchemaId;
            this.valueSchemaHash = nextValueSchemaHash;
        }
    }

    private void trackSourceLsn(Long lsn) {
        if (lsn == null) {
            return;
        }
        if (sourceMinLsn == null || lsn < sourceMinLsn) {
            sourceMinLsn = lsn;
        }
        if (sourceMaxLsn == null || lsn > sourceMaxLsn) {
            sourceMaxLsn = lsn;
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            observability.observeCallableVoid(
                    Observability.CDC_FILES_WRITTEN_TO_COUNTER,
                    "flush",
                    tableTags(),
                    () -> {
                        if (this.bytesWritten == 0) {
                            return "skipped";
                        }
                        flushBuffer();
                        return "success";
                    });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void flushBuffer() throws IOException {
        synchronized (this) {
            if (this.bytesWritten == 0) {
                return;
            }
            Instant writtenAt = Instant.now();
            Instant startedAt = this.extractStartAt != null ? this.extractStartAt : writtenAt;
            String flushedBufferId = this.bufferId;
            long flushedByteCount = this.bytesWritten;
            long flushedRowCount = rowCount;
            String flushedKeySchemaId = this.keySchemaId;
            String flushedValueSchemaId = this.valueSchemaId;
            Long flushedSourceMinLsn = this.sourceMinLsn;
            Long flushedSourceMaxLsn = this.sourceMaxLsn;

            this.sink.flush();
            this.sink.close();
            TableFqn fqn = TableFqn.parse(tableName);
            FileWriteFlushEvent fileWriteFlushEvent = new FileWriteFlushEvent(
                    WRITE_TYPE,
                    null,
                    fqn.tableName(),
                    extractJobId,
                    flushedBufferId,
                    extractType,
                    startedAt,
                    writtenAt,
                    flushedRowCount,
                    flushedByteCount,
                    instanceName,
                    fqn.databaseName(),
                    fqn.schemaName(),
                    fqn.tableName(),
                    null,
                    null,
                    null,
                    flushedKeySchemaId,
                    flushedValueSchemaId,
                    flushedSourceMinLsn,
                    flushedSourceMaxLsn);
            long contentLength = Files.size(this.tempFile);
            String writtenPath;
            try (InputStream readStream = Files.newInputStream(this.tempFile)) {
                writtenPath = flushedBufferHandler.handleFlushedBuffer(
                        fileWriteFlushEvent, readStream, contentLength);
            }
            log.info("flushed buffer to path: {}", writtenPath);

            Files.deleteIfExists(this.tempFile);
            this.tempFile = null;
            openTempFile();
            this.bytesWritten = 0;
            rowCount = 0;

            if (onFlush != null) {
                onFlush.accept(fileWriteFlushEvent);
            }
        }
    }

    private void openTempFile() {
        try {
            String nextBufferId = UUID.randomUUID().toString();
            String prefix = this.extractJobId + "-" + nextBufferId + ".";
            this.tempFile = Files.createTempFile(prefix, this.sink.getOutputFormat().fileSuffix());
            this.sink.open(this.tempFile);
            this.extractStartAt = Instant.now();
            this.bufferId = nextBufferId;
            this.keySchemaId = null;
            this.valueSchemaId = null;
            this.keySchemaHash = 0L;
            this.valueSchemaHash = 0L;
            this.sourceMinLsn = null;
            this.sourceMaxLsn = null;
        } catch (IOException e) {
            throw new RuntimeException("failed to create temp buffer file", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            observability.observeCallableVoid(
                    Observability.CDC_FILES_WRITTEN_TO_COUNTER,
                    "close",
                    tableTags(),
                    () -> {
                        flushBuffer();
                        if (this.sink != null) {
                            this.sink.close();
                        }
                        if (this.tempFile != null) {
                            Files.deleteIfExists(this.tempFile);
                            this.tempFile = null;
                        }
                        return "success";
                    });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Map<String, String> tableTags() {
        TableFqn fqn = TableFqn.parse(tableName);
        return Observability.sourceTags(
                instanceName, fqn.databaseName(), fqn.schemaName(), fqn.tableName());
    }

    public static final class Builder {
        private Integer maxBufferSize;
        private ExtractRecordSink sink;
        private String tableName;
        private String instanceName;
        private Observability observability;
        private String extractType;
        private String extractJobId;
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

        public Builder withFlushedBufferHandler(FlushedBufferHandler flushedBufferHandler) {
            this.flushedBufferHandler = flushedBufferHandler;
            return this;
        }

        public Builder withOnFlush(Consumer<FileWriteFlushEvent> onFlush) {
            this.onFlush = onFlush;
            return this;
        }

        public ExtractAvroBuffer build() {
            return new ExtractAvroBuffer(this);
        }
    }
}
