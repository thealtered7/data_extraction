package com.dataextraction.cdc_consumer.format;

/**
 * Turns the raw key and value from a Debezium change event into a {@link DeserializedRecord}.
 *
 * @param <T> engine wire type: {@code String} for JSON, {@code byte[]} for Avro
 */
public interface RecordDeserializer<T> extends AutoCloseable {

    DeserializedRecord deserialize(String destination, T key, T value);

    @Override
    default void close() {
    }
}
