package com.dataextraction.sink.impl;

import com.dataextraction.sink.ExtractRecordSink;
import com.dataextraction.sink.ExtractOutputFormat;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

public final class AvroExtractRecordSink implements ExtractRecordSink {

    private Path tempFile;
    private CountingOutputStream countingStream;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private Schema topLevelSchema;
    private long bytesAtLastWrite;
    private ConnectAvroSchemas connectAvroSchemas;
    
    public AvroExtractRecordSink(ConnectAvroSchemas connectAvroSchemas) {
        this.connectAvroSchemas = connectAvroSchemas;
    }

    @Override
    public void open(Path tempFile) throws IOException {
        closeQuietly();
        this.tempFile = tempFile;
        this.topLevelSchema = null;
        this.bytesAtLastWrite = 0L;
        // Writer opens lazily on first record once the payload schema is known.
    }

    @Override
    public int write(JsonNode enrichedRoot, String connectValueSchemaJson) throws IOException {
        if (tempFile == null) {
            throw new IOException("Avro sink is not open");
        }
        JsonNode extractNode = enrichedRoot.get("extract");
        JsonNode payloadNode = enrichedRoot.get("payload");
        if (payloadNode == null || payloadNode.isMissingNode()) {
            // Schemaless test/simple records: treat non-metadata fields as payload.
            payloadNode = enrichedRoot.deepCopy();
            if (payloadNode instanceof com.fasterxml.jackson.databind.node.ObjectNode objectNode) {
                objectNode.remove("extract");
                objectNode.remove("schema");
            }
        }

        if (dataFileWriter == null) {
            Schema payloadSchema;
            if (connectValueSchemaJson != null && !connectValueSchemaJson.isBlank()) {
                payloadSchema = connectAvroSchemas.payloadSchemaFromConnectJson(connectValueSchemaJson);
            } else {
                payloadSchema = connectAvroSchemas.payloadSchemaFromJson(payloadNode);
            }
            topLevelSchema = connectAvroSchemas.topLevelSchema(payloadSchema);
            countingStream = new CountingOutputStream(new BufferedOutputStream(Files.newOutputStream(
                    tempFile,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.CREATE)));
            dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(topLevelSchema));
            dataFileWriter.setCodec(CodecFactory.snappyCodec());
            dataFileWriter.create(topLevelSchema, countingStream);
            bytesAtLastWrite = countingStream.bytesWritten();
        }

        GenericRecord record =
                connectAvroSchemas.toTopLevelRecord(topLevelSchema, extractNode, payloadNode);
        dataFileWriter.append(record);
        dataFileWriter.flush();
        long now = countingStream.bytesWritten();
        int delta = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, now - bytesAtLastWrite));
        bytesAtLastWrite = now;
        return Math.max(delta, 1);
    }

    @Override
    public void flush() throws IOException {
        if (dataFileWriter != null) {
            dataFileWriter.flush();
        }
        if (countingStream != null) {
            countingStream.flush();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (dataFileWriter != null) {
                dataFileWriter.close();
            } else if (countingStream != null) {
                countingStream.close();
            }
        } finally {
            dataFileWriter = null;
            countingStream = null;
            tempFile = null;
            topLevelSchema = null;
            bytesAtLastWrite = 0L;
        }
    }

    @Override
    public ExtractOutputFormat format() {
        return ExtractOutputFormat.AVRO;
    }

    private void closeQuietly() {
        try {
            close();
        } catch (IOException ignored) {
            // reset for reopen
        }
    }    
}
