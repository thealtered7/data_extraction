package com.dataextraction.cdc_consumer.format;

import java.util.Locale;

/**
 * Wire format of change events consumed from the embedded Debezium engine.
 */
public enum ChangeFormat {
    JSON,
    AVRO;

    public static ChangeFormat fromName(String name) {
        if (name == null || name.isBlank()) {
            return JSON;
        }
        try {
            return ChangeFormat.valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unsupported debezium.format '" + name + "'; expected json or avro", e);
        }
    }
}
