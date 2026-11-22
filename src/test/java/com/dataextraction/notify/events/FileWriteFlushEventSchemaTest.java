package com.dataextraction.notify.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FileWriteFlushEventSchemaTest {

    private static final Set<String> REGISTRY_PROPERTIES = Set.of(
            "write_type",
            "raw_file_path",
            "table_name",
            "extract_job_id",
            "extract_buffer_id",
            "extract_type",
            "extract_start_at",
            "extract_end_at",
            "write_row_count",
            "raw_file_size",
            "source_instance_name",
            "source_database_name",
            "source_schema_name",
            "source_table_name",
            "warehouse_path",
            "key_schema",
            "value_schema",
            "key_schema_id",
            "value_schema_id",
            "source_min_lsn",
            "source_max_lsn");

    @Test
    void jsonSchemaMatchesRegistrySubjectFields() throws Exception {
        JsonNode schema = new JsonSchemaGenerator(new ObjectMapper())
                .generateJsonSchema(FileWriteFlushEvent.class);
        JsonNode properties = schema.get("properties");

        assertEquals(REGISTRY_PROPERTIES.size(), properties.size());
        for (String name : REGISTRY_PROPERTIES) {
            assertFalse(properties.path(name).isMissingNode(), name);
        }
        assertFalse(schema.has("required"));
    }
}
