package com.dataextraction.sink;

/**
 * User metadata stored once in an Avro object-container header.
 * The writer schema in that header is the record schema. The Debezium key schema is not part of
 * the written records, so it is stored beside the writer schema.
 */
public final class AvroFileMetadata {

    public static final String KEY_SCHEMA = "debezium.key.schema";

    private AvroFileMetadata() {
    }
}
