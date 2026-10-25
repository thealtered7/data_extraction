package com.dataextraction.sink.impl;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonlExtractRecordSink implements ExtractRecordSink {
    private static final Logger logger = LoggerFactory.getLogger(JsonlExtractRecordSink.class);

    private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Path tempFile;
    private OutputStream tempStream;

    @Override
    public void open(Path tempFile) throws IOException {
        closeQuietly();
        this.tempFile = tempFile;
        this.tempStream = new BufferedOutputStream(Files.newOutputStream(
                tempFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE));
    }

    @Override
    public int write(JsonNode enrichedRoot, String connectValueSchemaJson) throws IOException {
        ensureOpen();
        byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(enrichedRoot);
        tempStream.write(bytes);
        tempStream.write(NEWLINE);
        return bytes.length + NEWLINE.length;
    }

    @Override
    public void flush() throws IOException {
        if (tempStream != null) {
            tempStream.flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (tempStream != null) {
            tempStream.close();
            tempStream = null;
        }
        tempFile = null;
    }

    @Override
    public ExtractOutputFormat format() {
        return ExtractOutputFormat.JSONL;
    }

    private void ensureOpen() throws IOException {
        if (tempStream == null) {
            logger.error("JSONL sink is not open");
            throw new IOException("JSONL sink is not open");
        }
    }

    private void closeQuietly() {
        try {
            close();
        } catch (IOException ignored) {
            logger.error("Error closing JSONL sink: {}", ignored);
            // reset for reopen
        }
    }

    public Path getTempFile() {
        return this.tempFile;
    }
}
