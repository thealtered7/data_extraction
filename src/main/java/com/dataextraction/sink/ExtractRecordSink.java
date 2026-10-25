package com.dataextraction.sink;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Format-specific writer for enriched extract records into a temp buffer file.
 * One sink instance is bound to a single buffer (one schema per Avro OCF file).
 */
public interface ExtractRecordSink extends Closeable {

    /** Opens (or re-opens) the sink against {@code tempFile}. */
    void open(Path tempFile) throws IOException;

    /**
     * Appends one enriched record. Returns approximate serialized size in bytes for buffer limits.
     *
     * @param enrichedRoot JSON object including {@code extract} and typically {@code payload}/{@code schema}
     * @param connectValueSchemaJson Connect schema JSON for the payload (may be null on early/simple records)
     */
    int write(JsonNode enrichedRoot, String connectValueSchemaJson) throws IOException;

    void flush() throws IOException;

    @Override
    void close() throws IOException;

    ExtractOutputFormat format();
}
