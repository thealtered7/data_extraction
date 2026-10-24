package com.dataextraction.schemaregistry;

/**
 * Registers Debezium Connect JSON schema documents with a schema registry and returns the
 * registry id as a string (Confluent uses integer ids; Glue may use ARNs later).
 *
 * <p>Returns {@code null} when registration is skipped or unavailable; callers should still
 * persist the Connect schema JSON.
 */
public interface CdcSchemaRegistrar {

    /**
     * @param subject registry subject (e.g. {@code cdc.geo.public.scalars-key})
     * @param connectSchemaJson Connect schema document JSON, or null
     * @return registry id string, or null if skipped/unavailable
     */
    String register(String subject, String connectSchemaJson);
}
