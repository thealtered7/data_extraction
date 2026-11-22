package com.dataextraction.sink.impl;

import com.dataextraction.observability.Observability;
import com.dataextraction.sink.ExtractOutputFormat;
import com.dataextraction.sink.ExtractRecordSink;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonlExtractRecordSink implements ExtractRecordSink {
    private static final Logger logger = LoggerFactory.getLogger(JsonlExtractRecordSink.class);

    private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Path tempFile;
    private OutputStream tempStream;
    private final SinkTelemetry telemetry = new SinkTelemetry();

    @Override
    public void open(Path tempFile) throws IOException {
        closeQuietly();
        this.tempFile = tempFile;
        this.tempStream = new BufferedOutputStream(Files.newOutputStream(
                tempFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE));
    }

    @Override
    public void bindTelemetry(Observability observability, Map<String, String> sourceTags) {
        telemetry.bind(observability, sourceTags);
    }

    @Override
    public int write(JsonNode enrichedRoot, String connectValueSchemaJson) throws IOException {
        return telemetry.observe("jsonl_write", () -> append(enrichedRoot));
    }

    private int append(JsonNode enrichedRoot) throws IOException {
        ensureOpen();
        byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(enrichedRoot);
        tempStream.write(bytes);
        tempStream.write(NEWLINE);
        return bytes.length + NEWLINE.length;
    }

    @Override
    public void flush() throws IOException {
        telemetry.observe("jsonl_flush", () -> {
            if (tempStream != null) {
                tempStream.flush();
            }
            return "success";
        });
    }

    @Override
    public void close() throws IOException {
        telemetry.observe("jsonl_close", () -> {
            release();
            return "success";
        });
    }

    private void release() throws IOException {
        if (tempStream != null) {
            tempStream.close();
            tempStream = null;
        }
        tempFile = null;
    }

    private void ensureOpen() throws IOException {
        if (tempStream == null) {
            logger.error("JSONL sink is not open");
            throw new IOException("JSONL sink is not open");
        }
    }

    private void closeQuietly() {
        try {
            release();
        } catch (IOException ignored) {
            logger.error("Error closing JSONL sink: {}", ignored);
        }
    }

    public Path getTempFile() {
        return this.tempFile;
    }

    @Override
    public ExtractOutputFormat getOutputFormat() {
        return ExtractOutputFormat.JSONL;
    }
}
