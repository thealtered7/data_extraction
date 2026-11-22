package com.dataextraction.notify.events;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Flush notification published to Schema Registry subject {@code cdc-file-write-value}.
 * JSON names match that subject's schema.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class FileWriteFlushEvent {

    private String writeType;
    private Path rawFilePath;
    private String tableName;
    private String extractJobId;
    private String extractBufferId;
    private String extractType;
    private Instant extractStartAt;
    private Instant extractEndAt;
    private Long writeRowCount;
    private Long rawFileSize;
    private String sourceInstanceName;
    private String sourceDatabaseName;
    private String sourceSchemaName;
    private String sourceTableName;
    private Path warehousePath;
    private String keySchema;
    private String valueSchema;
    private String keySchemaId;
    private String valueSchemaId;
    private Long sourceMinLsn;
    private Long sourceMaxLsn;

    public FileWriteFlushEvent() {
    }

    public FileWriteFlushEvent(
            String writeType,
            Path rawFilePath,
            String tableName,
            String extractJobId,
            String extractBufferId,
            String extractType,
            Instant extractStartAt,
            Instant extractEndAt,
            Long writeRowCount,
            Long rawFileSize,
            String sourceInstanceName,
            String sourceDatabaseName,
            String sourceSchemaName,
            String sourceTableName,
            Path warehousePath,
            String keySchema,
            String valueSchema,
            String keySchemaId,
            String valueSchemaId,
            Long sourceMinLsn,
            Long sourceMaxLsn) {
        this.writeType = writeType;
        this.rawFilePath = rawFilePath;
        this.tableName = tableName;
        this.extractJobId = extractJobId;
        this.extractBufferId = extractBufferId;
        this.extractType = extractType;
        this.extractStartAt = extractStartAt;
        this.extractEndAt = extractEndAt;
        this.writeRowCount = writeRowCount;
        this.rawFileSize = rawFileSize;
        this.sourceInstanceName = sourceInstanceName;
        this.sourceDatabaseName = sourceDatabaseName;
        this.sourceSchemaName = sourceSchemaName;
        this.sourceTableName = sourceTableName;
        this.warehousePath = warehousePath;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.keySchemaId = keySchemaId;
        this.valueSchemaId = valueSchemaId;
        this.sourceMinLsn = sourceMinLsn;
        this.sourceMaxLsn = sourceMaxLsn;
    }

    public String getWriteType() {
        return writeType;
    }

    public void setWriteType(String writeType) {
        this.writeType = writeType;
    }

    public Path getRawFilePath() {
        return rawFilePath;
    }

    public void setRawFilePath(Path rawFilePath) {
        this.rawFilePath = rawFilePath;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getExtractJobId() {
        return extractJobId;
    }

    public void setExtractJobId(String extractJobId) {
        this.extractJobId = extractJobId;
    }

    public String getExtractBufferId() {
        return extractBufferId;
    }

    public void setExtractBufferId(String extractBufferId) {
        this.extractBufferId = extractBufferId;
    }

    public String getExtractType() {
        return extractType;
    }

    public void setExtractType(String extractType) {
        this.extractType = extractType;
    }

    public Instant getExtractStartAt() {
        return extractStartAt;
    }

    public void setExtractStartAt(Instant extractStartAt) {
        this.extractStartAt = extractStartAt;
    }

    public Instant getExtractEndAt() {
        return extractEndAt;
    }

    public void setExtractEndAt(Instant extractEndAt) {
        this.extractEndAt = extractEndAt;
    }

    public Long getWriteRowCount() {
        return writeRowCount;
    }

    public void setWriteRowCount(Long writeRowCount) {
        this.writeRowCount = writeRowCount;
    }

    public Long getRawFileSize() {
        return rawFileSize;
    }

    public void setRawFileSize(Long rawFileSize) {
        this.rawFileSize = rawFileSize;
    }

    public String getSourceInstanceName() {
        return sourceInstanceName;
    }

    public void setSourceInstanceName(String sourceInstanceName) {
        this.sourceInstanceName = sourceInstanceName;
    }

    public String getSourceDatabaseName() {
        return sourceDatabaseName;
    }

    public void setSourceDatabaseName(String sourceDatabaseName) {
        this.sourceDatabaseName = sourceDatabaseName;
    }

    public String getSourceSchemaName() {
        return sourceSchemaName;
    }

    public void setSourceSchemaName(String sourceSchemaName) {
        this.sourceSchemaName = sourceSchemaName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public Path getWarehousePath() {
        return warehousePath;
    }

    public void setWarehousePath(Path warehousePath) {
        this.warehousePath = warehousePath;
    }

    public String getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(String keySchema) {
        this.keySchema = keySchema;
    }

    public String getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(String valueSchema) {
        this.valueSchema = valueSchema;
    }

    public String getKeySchemaId() {
        return keySchemaId;
    }

    public void setKeySchemaId(String keySchemaId) {
        this.keySchemaId = keySchemaId;
    }

    public String getValueSchemaId() {
        return valueSchemaId;
    }

    public void setValueSchemaId(String valueSchemaId) {
        this.valueSchemaId = valueSchemaId;
    }

    public Long getSourceMinLsn() {
        return sourceMinLsn;
    }

    public void setSourceMinLsn(Long sourceMinLsn) {
        this.sourceMinLsn = sourceMinLsn;
    }

    public Long getSourceMaxLsn() {
        return sourceMaxLsn;
    }

    public void setSourceMaxLsn(Long sourceMaxLsn) {
        this.sourceMaxLsn = sourceMaxLsn;
    }
}
