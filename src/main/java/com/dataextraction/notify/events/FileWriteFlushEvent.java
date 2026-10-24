package com.dataextraction.notify.events;

import java.nio.file.Path;
import java.time.Instant;

public record FileWriteFlushEvent(
        String writeType,
        Path filePath,
        Instant writtenAt,
        Instant extractStartAt,
        Instant extractEndAt,
        long fileSize,
        long rowCount,
        String instanceName,
        String databaseName,
        String schemaName,
        String tableName,
        String extractJobId,
        String bufferId,
        String extractType,
        Path warehousePath,
        String keySchema,
        String valueSchema,
        String keySchemaId,
        String valueSchemaId,
        Long sourceMinLsn,
        Long sourceMaxLsn) {}
