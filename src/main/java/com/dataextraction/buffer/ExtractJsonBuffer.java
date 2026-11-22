package com.dataextraction.buffer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;
import com.dataextraction.sink.ExtractOutputFormat;
import com.dataextraction.sink.ExtractRecordSink;
import com.dataextraction.schemaregistry.CdcSchemaRegistrar;
import com.dataextraction.schemaregistry.TableFqn;

import io.debezium.engine.ChangeEvent;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractJsonBuffer {
    private static final Logger log = LoggerFactory.getLogger(ExtractJsonBuffer.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String EXTRACT_FIELD = "extract";
    private static final String EXTRACT_JOB_ID_FIELD = "extract_job_id";
    private static final String EXTRACT_BUFFER_ID_FIELD = "extract_buffer_id";
    private static final String EXTRACT_TYPE_FIELD = "extract_type";
    private static final String EXTRACTED_AT_FIELD = "extracted_at";
    private static final String WRITE_TYPE = "raw";

    private Integer maxBufferSize;
    private Path tempFile;
    private ExtractRecordSink sink;
    private String tableName;
    private final String instanceName;
    private final Observability observability;
    private final String extractType;
    private final String extractJobId;
    private final Consumer<FileWriteFlushEvent> onFlush;
    private final CdcSchemaRegistrar schemaRegistrar;    
    private Instant extractStartAt;
    private String bufferId;
    private long rowCount;
    private long bytesWritten;
    private String keySchema;
    private String valueSchema;
    private long keySchemaHash;
    private long valueSchemaHash;
    private Long sourceMinLsn;
    private Long sourceMaxLsn;
    private FlushedBufferHandler flushedBufferHandler;

    private ExtractJsonBuffer(Builder builder) {
        Objects.requireNonNull(builder.maxBufferSize, "maxBufferSize is required");
        Objects.requireNonNull(builder.sink, "sink is required");
        Objects.requireNonNull(builder.tableName, "tableName is required");
        Objects.requireNonNull(builder.instanceName, "instanceName is required");
        Objects.requireNonNull(builder.observability, "observability is required");
        Objects.requireNonNull(builder.extractType, "extractType is required");
        Objects.requireNonNull(builder.extractJobId, "extractJobId is required");
        Objects.requireNonNull(builder.onFlush, "onFlush is required");
        Objects.requireNonNull(builder.schemaRegistrar, "schemaRegistrar is required");
        Objects.requireNonNull(builder.flushedBufferHandler, "flushedBufferHandler is required");
        this.maxBufferSize = builder.maxBufferSize;
        this.sink = builder.sink;
        this.tableName = builder.tableName;
        this.instanceName = builder.instanceName;
        this.observability = builder.observability;
        this.extractType = builder.extractType;
        this.extractJobId = builder.extractJobId;
        this.onFlush = builder.onFlush;
        this.schemaRegistrar = builder.schemaRegistrar;
        this.flushedBufferHandler = builder.flushedBufferHandler;
        this.sink.bindTelemetry(this.observability, tableTags());
        openTempFile();
    }

    public static Builder builder() {
        return new Builder();
    }



    public static class Builder {
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

        public Builder withOnFlush(Consumer<FileWriteFlushEvent> onFlush) {
            this.onFlush = onFlush;
            return this;
        }

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

        public ExtractJsonBuffer build() {
            return new ExtractJsonBuffer(this);
        }
    }
    
    public ExtractOutputFormat outputFormat() {
        return this.sink.getOutputFormat();
    }

    public int write(ChangeEvent<String, String> record) throws IOException {
        final int[] bytesWritten = {-1};
        observability.observeOperationVoid(
                Observability.CDC_EVENT_WRITTEN_TO_BUFFER_COUNTER,
                "write",
                tableTags(),
                () -> {
                    bytesWritten[0] = writeRecord(record);
                    return bytesWritten[0] == -1 ? "write_error" : "success";
                });
        return bytesWritten[0];
    }

    private int writeRecord(ChangeEvent<String, String> record) {
        try {
            if (record.value() == null) {
                return -1;
            }
            String nextKeySchema = extractConnectSchema(record.key());
            String nextValueSchema = extractConnectSchema(record.value());
            long nextKeySchemaHash = schemaHash(nextKeySchema);
            long nextValueSchemaHash = schemaHash(nextValueSchema);

            // Flush before mutating buffer schemas so flush metadata matches rows already written.
            // Schema drift: each buffer holds one key/value Connect schema pair; the drifted
            // record becomes the first row of a new buffer. Size overflow uses the same pattern.
            boolean schemaDrift = this.bytesWritten > 0
                    && hasSchemaDrift(nextKeySchema, nextKeySchemaHash, nextValueSchema, nextValueSchemaHash);
            if (schemaDrift) {
                log.info("schema drift detected for table {}; flushing buffer", tableName);
                flushBuffer();
            }

            ObjectNode enriched = enrichWithExtract(record.value());
            int estimatedSize = estimateSerializedSize(enriched);
            if (this.bytesWritten > 0 && this.bytesWritten + estimatedSize > maxBufferSize) {
                flushBuffer();
                enriched = enrichWithExtract(record.value());
            }

            captureSchemas(nextKeySchema, nextKeySchemaHash, nextValueSchema, nextValueSchemaHash);
            trackSourceLsn(record.value());
            if (log.isDebugEnabled()) {
                log.debug("writing CDC record: {}", OBJECT_MAPPER.writeValueAsString(enriched));
            }
            int written = this.sink.write(enriched, this.valueSchema);
            this.bytesWritten += written;
            rowCount++;
            log.debug("current buffer size: {}", this.bytesWritten);

            return written;
        } catch (Exception e) {
            log.error("failed to write CDC record", e);
            return -1;
        }
    }

    private boolean hasSchemaDrift(
            String nextKeySchema,
            long nextKeySchemaHash,
            String nextValueSchema,
            long nextValueSchemaHash) {
        return schemaDrifted(this.keySchema, this.keySchemaHash, nextKeySchema, nextKeySchemaHash)
                || schemaDrifted(this.valueSchema, this.valueSchemaHash, nextValueSchema, nextValueSchemaHash);
    }

    /**
     * Null/missing incoming schemas are ignored (not drift). Drift only when both sides are
     * present and their CRC32C hashes differ.
     */
    static boolean schemaDrifted(
            String currentSchema, long currentSchemaHash, String nextSchema, long nextSchemaHash) {
        if (nextSchema == null || nextSchema.isBlank()) {
            return false;
        }
        if (currentSchema == null || currentSchema.isBlank()) {
            return false;
        }
        return currentSchemaHash != nextSchemaHash;
    }

    /** CRC32C of UTF-8 schema JSON; {@code 0} when schema is null/blank. */
    static long schemaHash(String schemaJson) {
        if (schemaJson == null || schemaJson.isBlank()) {
            return 0L;
        }
        CRC32C crc = new CRC32C();
        crc.update(schemaJson.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }

    private void captureSchemas(
            String nextKeySchema, long nextKeySchemaHash, String nextValueSchema, long nextValueSchemaHash) {
        if (nextKeySchema != null) {
            this.keySchema = nextKeySchema;
            this.keySchemaHash = nextKeySchemaHash;
        }
        if (nextValueSchema != null) {
            this.valueSchema = nextValueSchema;
            this.valueSchemaHash = nextValueSchemaHash;
        }
    }

    private void trackSourceLsn(String valueJson) {
        if (valueJson == null || valueJson.isBlank()) {
            return;
        }
        try {
            JsonNode lsnNode = OBJECT_MAPPER.readTree(valueJson).path("payload").path("source").path("lsn");
            if (lsnNode.isMissingNode() || lsnNode.isNull()) {
                return;
            }
            long lsn = lsnNode.asLong();
            if (sourceMinLsn == null || lsn < sourceMinLsn) {
                sourceMinLsn = lsn;
            }
            if (sourceMaxLsn == null || lsn > sourceMaxLsn) {
                sourceMaxLsn = lsn;
            }
        } catch (IOException e) {
            log.debug("Unable to read source LSN from record", e);
        }
    }

    static String extractConnectSchema(String envelopeJson) {
        if (envelopeJson == null || envelopeJson.isBlank()) {
            return null;
        }
        try {
            JsonNode root = OBJECT_MAPPER.readTree(envelopeJson);
            JsonNode schema = root.get("schema");
            if (schema == null || schema.isNull() || schema.isMissingNode()) {
                return null;
            }
            return OBJECT_MAPPER.writeValueAsString(schema);
        } catch (IOException e) {
            log.warn("Failed to extract Connect schema from envelope", e);
            return null;
        }
    }

    private ObjectNode enrichWithExtract(String recordValue) throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(recordValue);
        ObjectNode objectRoot;
        if (root instanceof ObjectNode node) {
            objectRoot = node;
        } else {
            objectRoot = OBJECT_MAPPER.createObjectNode();
            objectRoot.set("value", root);
        }
        ObjectNode extract = OBJECT_MAPPER.createObjectNode();
        extract.put(EXTRACT_JOB_ID_FIELD, extractJobId);
        extract.put(EXTRACT_BUFFER_ID_FIELD, bufferId);
        extract.put(EXTRACT_TYPE_FIELD, extractType);
        extract.put(EXTRACTED_AT_FIELD, Instant.now().truncatedTo(ChronoUnit.MICROS).toString());
        objectRoot.set(EXTRACT_FIELD, extract);
        return objectRoot;
    }

    private static int estimateSerializedSize(ObjectNode enriched) throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(enriched).length + 1;
    }

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
            String flushedKeySchema = this.keySchema;
            String flushedValueSchema = this.valueSchema;
            String flushedKeySchemaId = registerSchemaId(tableName + "-key", flushedKeySchema);
            String flushedValueSchemaId = registerSchemaId(tableName + "-value", flushedValueSchema);
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
                flushedKeySchema,
                flushedValueSchema,
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

    private String getFileSuffix() {
        return this.sink.getOutputFormat().fileSuffix();
    }

    private void openTempFile() {
        try {
            String bufferId = UUID.randomUUID().toString();
            String prefix = this.extractJobId + "-" + bufferId + ".";
            this.tempFile = Files.createTempFile(prefix, getFileSuffix());
            this.sink.open(this.tempFile);
            this.extractStartAt = Instant.now();
            this.bufferId = bufferId;
            this.keySchema = null;
            this.valueSchema = null;
            this.keySchemaHash = 0L;
            this.valueSchemaHash = 0L;
            this.sourceMinLsn = null;
            this.sourceMaxLsn = null;
        } catch (IOException e) {
            throw new RuntimeException("failed to create temp buffer file", e);
        }
    }

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

    private String registerSchemaId(String subjectSuffix, String connectSchemaJson) {
        if (schemaRegistrar == null || connectSchemaJson == null || connectSchemaJson.isBlank()) {
            return null;
        }
        return schemaRegistrar.register("cdc." + subjectSuffix, connectSchemaJson, tableTags());
    }

    private Map<String, String> tableTags() {
        TableFqn fqn = TableFqn.parse(tableName);
        return Observability.sourceTags(
                instanceName, fqn.databaseName(), fqn.schemaName(), fqn.tableName());
    }
}
