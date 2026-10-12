package com.dataextraction.datapipelines.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Request body for the datapipelines {@code POST /extracts} and {@code PUT /extracts/{id}}
 * endpoints. Field names use the snake_case shape the service expects.
 * PUT is a full replace, so callers must resend identity fields on finish.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ExtractRequest(
        @JsonProperty("extract_job_id") String extractJobId,
        @JsonProperty("extract_type") String extractType,
        @JsonProperty("extract_start_at") Instant extractStartAt,
        @JsonProperty("extract_end_at") Instant extractEndAt,
        @JsonProperty("extract_status") String extractStatus,
        @JsonProperty("catalog_name") String catalogName,
        @JsonProperty("namespace_name") String namespaceName,
        @JsonProperty("database_name") String databaseName,
        @JsonProperty("source_instance_name") String sourceInstanceName,
        @JsonProperty("source_min_lsn") Long sourceMinLsn,
        @JsonProperty("source_max_lsn") Long sourceMaxLsn,
        @JsonProperty("extract_table_count") Long extractTableCount,
        @JsonProperty("extract_row_count") Long extractRowCount,
        @JsonProperty("extract_bytes_count") Long extractBytesCount) {}
