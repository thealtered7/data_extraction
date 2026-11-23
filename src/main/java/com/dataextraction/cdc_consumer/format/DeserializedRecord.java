package com.dataextraction.cdc_consumer.format;

import org.apache.avro.Schema;

/**
 * Key and value after the format-specific deserializer has run.
 * JSON leaves the Connect envelope strings untouched. Avro exposes {@code GenericRecord}s
 * plus the schema ids carried in the Confluent wire header.
 */
public interface DeserializedRecord {

    Object key();

    Object value();

    String keySchemaId();

    String valueSchemaId();

    Schema avroKeySchema();

    Schema avroValueSchema();
}
