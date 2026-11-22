package com.dataextraction.datapipelines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExtractJobReporterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private HttpServer server;
    private String baseUrl;
    private final List<CapturedRequest> requests = new ArrayList<>();
    private final AtomicInteger nextId = new AtomicInteger(100);
    private final AtomicInteger postStatus = new AtomicInteger(201);

    @BeforeEach
    void setUp() throws IOException {
        requests.clear();
        nextId.set(100);
        postStatus.set(201);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/extracts", this::handle);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        String body;
        try (InputStream in = exchange.getRequestBody()) {
            body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        requests.add(new CapturedRequest(method, path, body));

        if ("POST".equals(method) && "/extracts".equals(path)) {
            int status = postStatus.get();
            if (status >= 200 && status < 300) {
                String response = "{\"id\":" + nextId.getAndIncrement() + "}";
                byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, bytes.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(bytes);
                }
            } else {
                exchange.sendResponseHeaders(status, -1);
            }
        } else if ("PUT".equals(method)) {
            exchange.sendResponseHeaders(200, -1);
        } else {
            exchange.sendResponseHeaders(404, -1);
        }
        exchange.close();
    }

    private DatapipelinesHttpClient newClient() {
        return new DatapipelinesHttpClient(
                HttpClient.newHttpClient(), MAPPER, Observability.noop(), baseUrl);
    }

    private ExtractJobReporter newReporter(DatapipelinesHttpClient client) {
        return new ExtractJobReporter(client, "job-abc", "snapshot", "lakehouse", "geo", "geo", "test-instance");
    }

    private static FileWriteFlushEvent flushEvent(
            String db, String schema, String table, long rows, long bytes, Long sourceMinLsn, Long sourceMaxLsn) {
        return new FileWriteFlushEvent(
                "raw",
                Path.of("/opt/data/raw/" + db + "/" + schema + "/" + table + "/file.avro"),
                table,
                "job-abc",
                "buf-1",
                "snapshot",
                Instant.parse("2026-07-05T20:59:00Z"),
                Instant.parse("2026-07-05T21:00:00Z"),
                rows,
                bytes,
                "test-instance",
                db,
                schema,
                table,
                Path.of("/opt/data/raw/" + db + "/" + schema + "/" + table),
                null,
                null,
                null,
                null,
                sourceMinLsn,
                sourceMaxLsn);
    }

    private static FileWriteFlushEvent flushEvent(String db, String schema, String table, long rows, long bytes) {
        return flushEvent(db, schema, table, rows, bytes, null, null);
    }

    @Test
    void startThenFinish_postsStartingAndPutsFinishedWithMetrics() throws Exception {
        ExtractJobReporter reporter = newReporter(newClient());
        reporter.start();
        reporter.recordFlush(flushEvent("geo", "public", "scalars", 100, 1000, 100L, 200L));
        reporter.recordFlush(flushEvent("geo", "public", "scalars", 50, 500, 150L, 300L));
        reporter.recordFlush(flushEvent("geo", "public", "others", 10, 200, null, null));
        reporter.finish();

        assertEquals(2, requests.size());
        CapturedRequest post = requests.get(0);
        assertEquals("POST", post.method());
        assertEquals("/extracts", post.path());
        JsonNode startBody = MAPPER.readTree(post.body());
        assertEquals("STARTING", startBody.get("extract_status").asText());
        assertEquals("job-abc", startBody.get("extract_job_id").asText());
        assertEquals("snapshot", startBody.get("extract_type").asText());
        assertEquals("geo", startBody.get("namespace_name").asText());
        assertEquals(0L, startBody.get("extract_row_count").asLong());
        assertFalse(startBody.hasNonNull("source_min_lsn"));
        assertFalse(startBody.hasNonNull("source_max_lsn"));

        CapturedRequest put = requests.get(1);
        assertEquals("PUT", put.method());
        assertEquals("/extracts/100", put.path());
        JsonNode finishBody = MAPPER.readTree(put.body());
        assertEquals("FINISHED", finishBody.get("extract_status").asText());
        assertEquals(160L, finishBody.get("extract_row_count").asLong());
        assertEquals(1700L, finishBody.get("extract_bytes_count").asLong());
        assertEquals(2L, finishBody.get("extract_table_count").asLong());
        assertTrue(finishBody.hasNonNull("extract_end_at"));
        assertEquals("lakehouse", finishBody.get("catalog_name").asText());
        assertEquals("geo", finishBody.get("namespace_name").asText());
        assertEquals("geo", finishBody.get("database_name").asText());
        assertEquals("job-abc", finishBody.get("extract_job_id").asText());
        assertEquals(100L, finishBody.get("source_min_lsn").asLong());
        assertEquals(300L, finishBody.get("source_max_lsn").asLong());
    }

    @Test
    void finish_omitsLsnWhenNoFlushHadLsn() throws Exception {
        ExtractJobReporter reporter = newReporter(newClient());
        reporter.start();
        reporter.recordFlush(flushEvent("geo", "public", "scalars", 5, 100));
        reporter.finish();

        JsonNode finishBody = MAPPER.readTree(requests.get(1).body());
        assertFalse(finishBody.hasNonNull("source_min_lsn"));
        assertFalse(finishBody.hasNonNull("source_max_lsn"));
    }

    @Test
    void markFailed_finishPutsFailedStatus() throws Exception {
        ExtractJobReporter reporter = newReporter(newClient());
        reporter.start();
        reporter.recordFlush(flushEvent("geo", "public", "scalars", 5, 100));
        reporter.markFailed();
        reporter.finish();

        assertEquals(2, requests.size());
        JsonNode finishBody = MAPPER.readTree(requests.get(1).body());
        assertEquals("FAILED", finishBody.get("extract_status").asText());
        assertEquals(5L, finishBody.get("extract_row_count").asLong());
        assertTrue(finishBody.hasNonNull("extract_end_at"));
    }

    @Test
    void finish_skippedWhenStartPostFails() {
        postStatus.set(500);
        ExtractJobReporter reporter = newReporter(newClient());
        reporter.start();
        reporter.recordFlush(flushEvent("geo", "public", "scalars", 1, 10));
        reporter.finish();

        assertEquals(1, requests.size());
        assertEquals("POST", requests.get(0).method());
    }

    @Test
    void finish_isIdempotent() throws Exception {
        ExtractJobReporter reporter = newReporter(newClient());
        reporter.start();
        reporter.finish();
        reporter.finish();

        assertEquals(2, requests.size());
        assertEquals("POST", requests.get(0).method());
        assertEquals("PUT", requests.get(1).method());
    }

    @Test
    void createOptional_returnsNullWhenClientNull() {
        assertNull(ExtractJobReporter.createOptional(
                null, "job", "snapshot", "lakehouse", "geo", "geo", "inst"));
    }

    @Test
    void createOptional_returnsReporterWhenClientPresent() {
        ExtractJobReporter reporter = ExtractJobReporter.createOptional(
                newClient(), "job", "snapshot", "lakehouse", "geo", "geo", "inst");
        assertTrue(reporter != null);
    }

    @Test
    void recordFlush_ignoresNullEvent() throws Exception {
        ExtractJobReporter reporter = newReporter(newClient());
        reporter.recordFlush(null);
        reporter.start();
        reporter.finish();

        JsonNode finishBody = MAPPER.readTree(requests.get(1).body());
        assertEquals(0L, finishBody.get("extract_row_count").asLong());
        assertEquals(0L, finishBody.get("extract_table_count").asLong());
    }

    private record CapturedRequest(String method, String path, String body) {}
}
