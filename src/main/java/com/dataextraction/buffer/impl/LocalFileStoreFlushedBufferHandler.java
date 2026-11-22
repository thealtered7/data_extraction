package com.dataextraction.buffer.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.buffer.FlushedBufferHandler;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;

public class LocalFileStoreFlushedBufferHandler implements FlushedBufferHandler {
    private static final Logger logger = LoggerFactory.getLogger(LocalFileStoreFlushedBufferHandler.class);
    private Path basePath;
    private String environment;
    private String fileExtension;
    private final Observability observability;

    public LocalFileStoreFlushedBufferHandler(
            Path basePath, String environment, String fileExtension, Observability observability) {
        this.basePath = basePath;
        this.environment = environment;
        fileExtension = fileExtension.replace(".", "").trim().toLowerCase();
        this.fileExtension = fileExtension;
        this.observability = Objects.requireNonNull(observability, "observability");
    }

    @Override
    public String handleFlushedBuffer(FileWriteFlushEvent event, InputStream readStream, long contentLength) {
        logger.info("Handling flushed buffer: {}", event);
        try {
            return observability.observeCallable(
                    Observability.FILE_WRITES_PREFIX,
                    "file_store",
                    Observability.sourceTags(
                            event.getSourceInstanceName(),
                            event.getSourceDatabaseName(),
                            event.getSourceSchemaName(),
                            event.getSourceTableName()),
                    () -> writeFile(event, readStream));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to store flushed buffer", e);
        }
    }

    private String writeFile(FileWriteFlushEvent event, InputStream readStream) throws IOException {
        Path eventPath = this.getEventPath(event);
        try {
            Files.createDirectories(eventPath.getParent());
            Files.write(eventPath, readStream.readAllBytes());
        } catch (IOException e) {
            logger.error("Error handling flushed buffer: {}", e);
            throw e;
        }
        return eventPath.toString();
    }

    private Path getEventPath(FileWriteFlushEvent event) {
        String extractJobId = event.getExtractJobId();
        String bufferId = event.getExtractBufferId();

        Instant extractStartAt = event.getExtractStartAt();

        ZonedDateTime startTime = extractStartAt.atZone(ZoneOffset.UTC);
        int year = startTime.getYear();
        int month = startTime.getMonthValue();
        int day = startTime.getDayOfMonth();

        String fileName = String.format("%s_%s_%s.%s", extractJobId, bufferId, extractStartAt.toString(), getFileExtension());
        Path artifactSubPath = Path.of(
                getEnvironment(),
                event.getSourceInstanceName(),
                event.getSourceDatabaseName(),
                event.getSourceSchemaName(),
                event.getSourceTableName(),
                String.format("%04d", year),
                String.format("%02d", month),
                String.format("%02d", day),
                fileName);
        Path fullPath = this.basePath.resolve(artifactSubPath);
        event.setRawFilePath(fullPath);
        event.setWarehousePath(this.basePath);
        return fullPath;        
    }

    public Path getBasePath() {
        return this.basePath;
    }

    public void setBasePath(Path basePath) {
        this.basePath = basePath;
    }

    public String getEnvironment() {
        return this.environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getFileExtension() {
        return this.fileExtension;
    }

    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }
}
