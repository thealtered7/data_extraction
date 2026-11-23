package com.dataextraction.cdc_consumer.format;

import org.apache.avro.Schema;

public final class DecodedChange implements DeserializedRecord {

    private final Object key;
    private final Object value;
    private final String keySchemaId;
    private final String valueSchemaId;
    private final Schema avroKeySchema;
    private final Schema avroValueSchema;

    private DecodedChange(
            Object key,
            Object value,
            String keySchemaId,
            String valueSchemaId,
            Schema avroKeySchema,
            Schema avroValueSchema) {
        this.key = key;
        this.value = value;
        this.keySchemaId = keySchemaId;
        this.valueSchemaId = valueSchemaId;
        this.avroKeySchema = avroKeySchema;
        this.avroValueSchema = avroValueSchema;
    }

    public static DecodedChange passthrough(Object key, Object value) {
        return new DecodedChange(key, value, null, null, null, null);
    }

    public static DecodedChange avro(
            Object key,
            Object value,
            String keySchemaId,
            String valueSchemaId,
            Schema avroKeySchema,
            Schema avroValueSchema) {
        return new DecodedChange(key, value, keySchemaId, valueSchemaId, avroKeySchema, avroValueSchema);
    }

    @Override
    public Object key() {
        return key;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public String keySchemaId() {
        return keySchemaId;
    }

    @Override
    public String valueSchemaId() {
        return valueSchemaId;
    }

    @Override
    public Schema avroKeySchema() {
        return avroKeySchema;
    }

    @Override
    public Schema avroValueSchema() {
        return avroValueSchema;
    }
}
