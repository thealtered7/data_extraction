package com.dataextraction.cdc_consumer.format;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

/**
 * Proxies {@link KafkaAvroDeserializer}. Schema ids are read from the Confluent wire header
 * before the third-party deserializer turns the payload into a {@code GenericRecord}.
 */
public final class AvroRecordDeserializer implements RecordDeserializer<byte[]> {

    private static final Logger log = LoggerFactory.getLogger(AvroRecordDeserializer.class);
    private static final String DEFAULT_TOPIC = "debezium";

    private final Deserializer<Object> keyDeserializer;
    private final Deserializer<Object> valueDeserializer;

    private AvroRecordDeserializer(Deserializer<Object> keyDeserializer, Deserializer<Object> valueDeserializer) {
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer, "keyDeserializer is required");
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer, "valueDeserializer is required");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static AvroRecordDeserializer create(String schemaRegistryUrl) {
        return builder().withSchemaRegistryUrl(schemaRegistryUrl).build();
    }

    @Override
    public DeserializedRecord deserialize(String destination, byte[] key, byte[] value) {
        String topic = destination == null || destination.isBlank() ? DEFAULT_TOPIC : destination;
        Object decodedKey = key == null ? null : keyDeserializer.deserialize(topic, key);
        Object decodedValue = value == null ? null : valueDeserializer.deserialize(topic, value);
        return DecodedChange.avro(
                decodedKey,
                decodedValue,
                ConfluentWireFormat.schemaId(key),
                ConfluentWireFormat.schemaId(value),
                schemaOf(decodedKey),
                schemaOf(decodedValue));
    }

    @Override
    public void close() {
        closeQuietly(keyDeserializer);
        closeQuietly(valueDeserializer);
    }

    private static Schema schemaOf(Object decoded) {
        if (decoded instanceof GenericContainer container) {
            return container.getSchema();
        }
        return null;
    }

    private static void closeQuietly(Deserializer<Object> deserializer) {
        try {
            deserializer.close();
        } catch (Exception e) {
            log.debug("Failed to close Avro deserializer", e);
        }
    }

    public static final class Builder {
        private String schemaRegistryUrl;
        private Deserializer<Object> keyDeserializer;
        private Deserializer<Object> valueDeserializer;

        public Builder withSchemaRegistryUrl(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            return this;
        }

        /** Visible for tests that substitute the third-party deserializer. */
        public Builder withKeyDeserializer(Deserializer<Object> keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
            return this;
        }

        /** Visible for tests that substitute the third-party deserializer. */
        public Builder withValueDeserializer(Deserializer<Object> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
            return this;
        }

        public AvroRecordDeserializer build() {
            Deserializer<Object> keys = keyDeserializer;
            Deserializer<Object> values = valueDeserializer;
            if (keys == null || values == null) {
                if (schemaRegistryUrl == null || schemaRegistryUrl.isBlank()) {
                    throw new IllegalArgumentException("schemaRegistryUrl is required");
                }
                if (keys == null) {
                    keys = confluentDeserializer(schemaRegistryUrl, true);
                }
                if (values == null) {
                    values = confluentDeserializer(schemaRegistryUrl, false);
                }
            }
            return new AvroRecordDeserializer(keys, values);
        }

        private static Deserializer<Object> confluentDeserializer(String schemaRegistryUrl, boolean key) {
            KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
            Map<String, Object> configs = new HashMap<>();
            configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
            deserializer.configure(configs, key);
            return deserializer;
        }
    }
}
