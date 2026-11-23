package com.dataextraction.sink;

import com.dataextraction.observability.Observability;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Format-specific writer for enriched extract records into a temp buffer file.
 * One sink instance is bound to a single buffer (one schema per Avro OCF file).
 */
public interface ExtractRecordSink extends Closeable {

    /** Opens (or re-opens) the sink against {@code tempFile}. */
    void open(Path tempFile) throws IOException;

    /**
     * Attaches the shared observability object and the source labels used on this sink's
     * counters and timing histograms. Called once when the buffer that owns the sink is created.
     */
    default void bindTelemetry(Observability observability, Map<String, String> sourceTags) {
    }

    /**
     * Appends one enriched record. Returns approximate serialized size in bytes for buffer limits.
     *
     * @param enrichedRoot JSON object including {@code extract} and typically {@code payload}/{@code schema}
     * @param connectValueSchemaJson Connect schema JSON for the payload (may be null on early/simple records)
     */
    int write(JsonNode enrichedRoot, String connectValueSchemaJson) throws IOException;

    /**
     * Appends a Debezium Avro value wrapped with extract fields.
     * The key schema is written once into the file user metadata.
     */
    default int writeAvro(
            GenericRecord value,
            Schema keySchema,
            String extractJobId,
            String extractBufferId,
            String extractType,
            String extractedAt) throws IOException {
        throw new UnsupportedOperationException("Avro change records are not supported by " + getClass().getSimpleName());
    }

    void flush() throws IOException;

    @Override
    void close() throws IOException;    

    ExtractOutputFormat getOutputFormat();
}
