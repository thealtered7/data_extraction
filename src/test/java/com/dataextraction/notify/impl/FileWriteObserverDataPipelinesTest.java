package com.dataextraction.notify.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.dataextraction.config.DatapipelinesProperties;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FileWriteObserverDataPipelinesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private HttpServer server;
    private final AtomicReference<String> capturedBody = new AtomicReference<>();

    @BeforeEach
    void setUp() throws IOException {
        capturedBody.set(null);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/table-writes", this::handle);
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    void observe_snapshotPostsSourceAndFileFieldsAndOmitsLsns() throws Exception {
        observer().observe(flushEvent("snapshot", 100L, 200L));

        JsonNode body = MAPPER.readTree(capturedBody.get());
        assertEquals("raw", body.get("write_type").asText());
        assertEquals("test-instance", body.get("source_instance_name").asText());
        assertEquals("geo", body.get("source_database_name").asText());
        assertEquals("public", body.get("source_schema_name").asText());
        assertEquals("scalars", body.get("source_table_name").asText());
        assertEquals(2L, body.get("write_row_count").asLong());
        assertEquals("/opt/data/raw/geo/public/scalars/file.avro", body.get("raw_file_path").asText());
        assertEquals(42L, body.get("raw_file_size").asLong());
        assertEquals("job-1", body.get("extract_job_id").asText());
        assertEquals("buf-1", body.get("extract_buffer_id").asText());
        assertEquals("snapshot", body.get("extract_type").asText());
        assertFalse(body.hasNonNull("source_min_lsn"));
        assertFalse(body.hasNonNull("source_max_lsn"));
        assertFalse(body.has("file_path"));
        assertFalse(body.has("row_count"));
        assertFalse(body.has("instance_name"));
        assertFalse(body.has("file_size"));
    }

    @Test
    void observe_cdcPostsSourceLsns() throws Exception {
        observer().observe(flushEvent("cdc", 100L, 200L));

        JsonNode body = MAPPER.readTree(capturedBody.get());
        assertEquals("test-instance", body.get("source_instance_name").asText());
        assertEquals("geo", body.get("source_database_name").asText());
        assertEquals("public", body.get("source_schema_name").asText());
        assertEquals("scalars", body.get("source_table_name").asText());
        assertEquals(2L, body.get("write_row_count").asLong());
        assertEquals("/opt/data/raw/geo/public/scalars/file.avro", body.get("raw_file_path").asText());
        assertEquals(42L, body.get("raw_file_size").asLong());
        assertEquals(100L, body.get("source_min_lsn").asLong());
        assertEquals(200L, body.get("source_max_lsn").asLong());
    }

    private FileWriteObserverDataPipelines observer() {
        DatapipelinesProperties properties = new DatapipelinesProperties();
        properties.setBaseUrl("http://127.0.0.1:" + server.getAddress().getPort());
        return FileWriteObserverDataPipelines.builder()
                .withHttpClient(HttpClient.newHttpClient())
                .withDatapipelinesProperties(properties)
                .withObjectMapper(MAPPER)
                .withObservability(Observability.noop())
                .build();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            capturedBody.set(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        exchange.sendResponseHeaders(201, -1);
        exchange.close();
    }

    private static FileWriteFlushEvent flushEvent(String extractType, Long sourceMinLsn, Long sourceMaxLsn) {
        return new FileWriteFlushEvent(
                "raw",
                Path.of("/opt/data/raw/geo/public/scalars/file.avro"),
                "scalars",
                "job-1",
                "buf-1",
                extractType,
                Instant.parse("2026-07-05T20:59:00Z"),
                Instant.parse("2026-07-05T21:00:00Z"),
                2L,
                42L,
                "test-instance",
                "geo",
                "public",
                "scalars",
                Path.of("/opt/data/raw"),
                null,
                null,
                null,
                null,
                sourceMinLsn,
                sourceMaxLsn);
    }
}
