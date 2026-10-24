package com.dataextraction.schemaregistry.confluent;

import com.dataextraction.schemaregistry.CdcSchemaRegistrar;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.Objects;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers Connect schema documents with Confluent Schema Registry by wrapping them in a JSON
 * Schema object that carries the Connect document under {@code x-kafka-connect-schema}.
 */
final class ConfluentCdcSchemaRegistrar implements CdcSchemaRegistrar {

    private static final Logger log = LoggerFactory.getLogger(ConfluentCdcSchemaRegistrar.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int IDENTITY_MAP_CAPACITY = 100;

    private ConfluentConfig confluentConfig;
    private SchemaRegistryClient client;


    public ConfluentCdcSchemaRegistrar(ConfluentConfig confluentConfig) {
        this.confluentConfig = Objects.requireNonNull(confluentConfig, "confluentConfig");
        this.client = new CachedSchemaRegistryClient(confluentConfig.getUrl(), IDENTITY_MAP_CAPACITY);
    }
    

    /** Visible for tests. */
    ConfluentCdcSchemaRegistrar(BiFunction<String, String, Integer> registration) {
        this.confluentConfig = null;
        this.client = null;
    }

    @Override
    public String register(String subject, String connectSchemaJson) {
        if (connectSchemaJson == null || connectSchemaJson.isBlank()) {
            return null;
        }
        if (subject == null || subject.isBlank()) {
            log.warn("Skipping schema registration: subject is blank");
            return null;
        }
        try {
            String wrapped = wrapConnectSchema(connectSchemaJson);
            Integer id = this.client.register(subject, new JsonSchema(wrapped));
            return id == null ? null : Integer.toString(id);
        } catch (Exception e) {
            log.warn(
                    "Failed to register Connect schema under subject {}; leaving schema id null",
                    subject,
                    e);
            return null;
        }
    }

    private String wrapConnectSchema(String connectSchemaJson) throws Exception {
        JsonNode connectSchema = MAPPER.readTree(connectSchemaJson);
        ObjectNode wrapper = MAPPER.createObjectNode();
        wrapper.put("$schema", "http://json-schema.org/draft-07/schema#");
        wrapper.put("type", "object");
        wrapper.put("title", "kafka-connect-schema");
        wrapper.put("additionalProperties", true);
        wrapper.set("x-kafka-connect-schema", connectSchema);
        return MAPPER.writeValueAsString(wrapper);
    }
}
