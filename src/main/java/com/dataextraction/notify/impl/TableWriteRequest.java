package com.dataextraction.notify.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;

/**
 * Request body for the datapipelines {@code POST /table-writes} endpoint.
 * Field names use the snake_case shape the service expects.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TableWriteRequest(
        @JsonProperty("write_type") String writeType,
        @JsonProperty("file_path") Path filePath,
        @JsonProperty("written_at") Instant writtenAt,
        @JsonProperty("extract_start_at") Instant extractStartAt,
        @JsonProperty("extract_end_at") Instant extractEndAt,
        @JsonProperty("file_size") long fileSize,
        @JsonProperty("row_count") long rowCount,
        @JsonProperty("instance_name") String instanceName,
        @JsonProperty("database_name") String databaseName,
        @JsonProperty("schema_name") String schemaName,
        @JsonProperty("table_name") String tableName,
        @JsonProperty("extract_job_id") String extractJobId,
        @JsonProperty("buffer_id") String bufferId,
        @JsonProperty("extract_type") String extractType,
        @JsonProperty("warehouse_path") Path warehousePath,
        @JsonProperty("key_schema") String keySchema,
        @JsonProperty("value_schema") String valueSchema,
        @JsonProperty("key_schema_id") String keySchemaId,
        @JsonProperty("value_schema_id") String valueSchemaId,
        @JsonProperty("source_min_lsn") Long sourceMinLsn,
        @JsonProperty("source_max_lsn") Long sourceMaxLsn,
        @JsonProperty("process_date") LocalDate processDate,
        @JsonProperty("extract_namespace") String extractNamespace) {}
