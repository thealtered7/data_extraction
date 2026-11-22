package com.dataextraction.notify.impl;


import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.config.DatapipelinesProperties;
import com.dataextraction.datapipelines.JwtTokenProvider;
import com.dataextraction.notify.FileWrittenObserver;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileWriteObserverDataPipelines implements FileWrittenObserver {
    private static final Logger log = LoggerFactory.getLogger(FileWriteObserverDataPipelines.class);
    private final HttpClient httpClient;
    private final DatapipelinesProperties datapipelinesProperties;
    private final JwtTokenProvider jwtTokenProvider;
    private final ObjectMapper objectMapper;
    private final Observability observability;    

    private FileWriteObserverDataPipelines(Builder builder) {
        Objects.requireNonNull(builder.httpClient, "httpClient is required");
        Objects.requireNonNull(builder.datapipelinesProperties, "datapipelinesProperties is required");
        Objects.requireNonNull(builder.objectMapper, "objectMapper is required");
        Objects.requireNonNull(builder.observability, "observability is required");

        this.httpClient = builder.httpClient;
        this.datapipelinesProperties = builder.datapipelinesProperties;
        this.jwtTokenProvider = builder.jwtTokenProvider;
        this.objectMapper = builder.objectMapper;
        this.observability = builder.observability;
        
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private HttpClient httpClient;
        private DatapipelinesProperties datapipelinesProperties;
        private JwtTokenProvider jwtTokenProvider;
        private ObjectMapper objectMapper;
        private Observability observability;

        public Builder withHttpClient(HttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public Builder withDatapipelinesProperties(DatapipelinesProperties datapipelinesProperties) {
            this.datapipelinesProperties = datapipelinesProperties;
            return this;
        }

        public Builder withJwtTokenProvider(JwtTokenProvider jwtTokenProvider) {
            this.jwtTokenProvider = jwtTokenProvider;
            return this;
        }

        public Builder withObjectMapper(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public Builder withObservability(Observability observability) {
            this.observability = observability;
            return this;
        }

        public FileWriteObserverDataPipelines build() {
            return new FileWriteObserverDataPipelines(this);
        }
    }

    public URI getEndpoint() {
        return URI.create(datapipelinesProperties.getBaseUrl() + "/table-writes");
    }


    @Override
    public void observe(FileWriteFlushEvent event) {
        log.info("Observing file write flush event: {}", event);
        Objects.requireNonNull(event, "event");
        try {
            observability.observeCallableVoid(
                    Observability.FILE_WRITES_PREFIX,
                    "http_post",
                    Observability.sourceTags(
                            event.getSourceInstanceName(),
                            event.getSourceDatabaseName(),
                            event.getSourceSchemaName(),
                            event.getSourceTableName()),
                    () -> {
                        send(event);
                        return "success";
                    });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to POST table write", e);
        }        
    }
    @Override
    public void flush() {
        //no op, no need to flush
    }

    private TableWriteRequest createFileWriteRequest(FileWriteFlushEvent event) {
        return new TableWriteRequest(
                event.getWriteType(),
                event.getRawFilePath(),
                null,
                event.getExtractStartAt(),
                event.getExtractEndAt(),
                event.getRawFileSize() == null ? 0L : event.getRawFileSize(),
                event.getWriteRowCount() == null ? 0L : event.getWriteRowCount(),
                event.getSourceInstanceName(),
                event.getSourceDatabaseName(),
                event.getSourceSchemaName(),
                event.getTableName(),
                event.getExtractJobId(),
                event.getExtractBufferId(),
                event.getExtractType(),
                event.getWarehousePath(),
                event.getKeySchema(),
                event.getValueSchema(),
                event.getKeySchemaId(),
                event.getValueSchemaId(),
                event.getSourceMinLsn(),
                event.getSourceMaxLsn(),
                null,
                null);
    }

    private void send(FileWriteFlushEvent event) throws Exception {
        TableWriteRequest requestBody = this.createFileWriteRequest(event);
        URI endpoint = this.getEndpoint();
        byte[] body = objectMapper.writeValueAsBytes(requestBody);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(datapipelinesProperties.getRequestTimeoutSeconds()))
                .header("Content-Type", "application/json");
        if (jwtTokenProvider != null) {
            requestBuilder.header("Authorization", "Bearer " + jwtTokenProvider.mintToken());
        }
        HttpRequest request = requestBuilder
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException(
                    "Datapipelines returned status " + status + " for buffer " + event.getExtractBufferId()
                            + ": " + response.body());
        }
        log.debug("Posted table write for buffer {} (status {})", event.getExtractBufferId(), status);
    }
}
