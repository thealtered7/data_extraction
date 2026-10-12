package com.dataextraction.datapipelines;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.dataextraction.datapipelines.requests.ExtractRequest;
import com.dataextraction.datapipelines.requests.TableWriteRequest;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP client for the datapipelines service: buffer-flush metadata ({@code POST /table-writes})
 * and extract lifecycle ({@code POST /extracts}, {@code PUT /extracts/{id}}).
 * Created only when {@code app.datapipelines.http.base-url} is configured; otherwise callers
 * should treat persistence as disabled.
 */
public final class DatapipelinesHttpClient {

    private static final Logger log = LoggerFactory.getLogger(DatapipelinesHttpClient.class);
    private static final String TABLE_WRITES_PATH = "/table-writes";
    private static final String EXTRACTS_PATH = "/extracts";
    private static final String WRITE_TYPE_RAW = "raw";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);


    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Observability observability;
    private final URI baseUri;
    private final URI tableWritesEndpoint;
    private final JwtTokenProvider jwtTokenProvider;

    public DatapipelinesHttpClient(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            Observability observability,
            String baseUrl) {
        this(httpClient, objectMapper, observability, baseUrl, null);
    }

    public DatapipelinesHttpClient(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            Observability observability,
            String baseUrl,
            JwtTokenProvider jwtTokenProvider) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.observability = Objects.requireNonNull(observability, "observability");
        this.baseUri = URI.create(stripTrailingSlash(Objects.requireNonNull(baseUrl, "baseUrl")));
        this.tableWritesEndpoint = URI.create(this.baseUri + TABLE_WRITES_PATH);
        this.jwtTokenProvider = jwtTokenProvider;
    }

    /**
     * Posts a single flush event as a raw table write. Throws {@link RuntimeException} on
     * transport failure or a non-2xx response so callers can log and continue.
     */
    public void postTableWrite(String jobId, FileWriteFlushEvent event) {
        Objects.requireNonNull(event, "event");
        try {
            observability.observeCallableVoid(
                    Observability.FILE_WRITES_PREFIX,
                    "http_post",
                    Map.of("table", event.tableName()),
                    () -> {
                        sendTableWrite(jobId, event);
                        return "success";
                    });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to POST table write", e);
        }
    }

    /**
     * Creates an extract record. Returns the generated {@code id} from the response body.
     * Throws {@link RuntimeException} on transport failure or a non-2xx response.
     */
    public long postExtract(ExtractRequest request) {
        Objects.requireNonNull(request, "request");
        try {
            return observability.observeCallable(
                    Observability.FILE_WRITES_PREFIX,
                    "extract_post",
                    Map.of(),
                    () -> sendPostExtract(request));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to POST extract", e);
        }
    }

    /**
     * Updates an extract record by id (full replace). Throws {@link RuntimeException} on
     * transport failure or a non-2xx response.
     */
    public void putExtract(long id, ExtractRequest request) {
        Objects.requireNonNull(request, "request");
        try {
            observability.observeCallableVoid(
                    Observability.FILE_WRITES_PREFIX,
                    "extract_put",
                    Map.of(),
                    () -> {
                        sendPutExtract(id, request);
                        return "success";
                    });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to PUT extract " + id, e);
        }
    }

    private void sendTableWrite(String jobId, FileWriteFlushEvent event) throws Exception {
        TableWriteRequest requestBody = new TableWriteRequest(
                WRITE_TYPE_RAW,
                null,
                null,
                null,
                null,
                event.instanceName(),
                event.databaseName(),
                event.schemaName(),
                event.tableName(),
                null,
                null,
                null,
                event.rowCount(),
                null,
                event.filePath().toString(),
                event.fileSize(),
                jobId,
                event.bufferId(),
                event.extractType(),
                event.extractStartAt(),
                event.writtenAt(),
                null,
                null,
                directoryPath(event.warehousePath()),
                event.keySchema(),
                event.valueSchema(),
                event.keySchemaId(),
                event.valueSchemaId(),
                event.sourceMinLsn(),
                event.sourceMaxLsn());
        HttpResponse<String> response = sendJson(tableWritesEndpoint, "POST", requestBody);
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException(
                    "Datapipelines returned status " + status + " for buffer " + event.bufferId()
                            + ": " + response.body());
        }
        log.debug("Posted table write for buffer {} (status {})", event.bufferId(), status);
    }

    private long sendPostExtract(ExtractRequest request) throws Exception {
        URI endpoint = URI.create(baseUri + EXTRACTS_PATH);
        HttpResponse<String> response = sendJson(endpoint, "POST", request);
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException(
                    "Datapipelines returned status " + status + " for POST /extracts: " + response.body());
        }
        JsonNode root = objectMapper.readTree(response.body());
        JsonNode idNode = root.get("id");
        if (idNode == null || !idNode.isNumber()) {
            throw new RuntimeException("Datapipelines POST /extracts response missing numeric id: " + response.body());
        }
        long id = idNode.asLong();
        log.info("Posted extract {} for job {} (status {})", id, request.extractJobId(), status);
        return id;
    }

    private void sendPutExtract(long id, ExtractRequest request) throws Exception {
        URI endpoint = URI.create(baseUri + EXTRACTS_PATH + "/" + id);
        HttpResponse<String> response = sendJson(endpoint, "PUT", request);
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException(
                    "Datapipelines returned status " + status + " for PUT /extracts/" + id + ": " + response.body());
        }
        log.info("Updated extract {} for job {} to status {} (http {})",
                id, request.extractJobId(), request.extractStatus(), status);
    }

    private HttpResponse<String> sendJson(URI endpoint, String method, Object requestBody) throws Exception {
        byte[] body = objectMapper.writeValueAsBytes(requestBody);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(endpoint)
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json");
        if (jwtTokenProvider != null) {
            requestBuilder.header("Authorization", "Bearer " + jwtTokenProvider.mintToken());
        }
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.ofByteArray(body);
        HttpRequest request = switch (method) {
            case "POST" -> requestBuilder.POST(publisher).build();
            case "PUT" -> requestBuilder.PUT(publisher).build();
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        };
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String stripTrailingSlash(String url) {
        String trimmed = url.trim();
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }

    private static String directoryPath(Path dir) {
        if (dir == null) {
            return null;
        }
        String path = dir.toAbsolutePath().normalize().toString();
        return path.endsWith("/") ? path : path + "/";
    }
}
