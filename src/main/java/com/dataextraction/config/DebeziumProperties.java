package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.dataextraction.cdc_consumer.format.ChangeFormat;

/**
 * Selects how the embedded engine delivers change events.
 * {@code json} is the Connect JSON envelope. {@code avro} is Confluent Avro wire format.
 */
@ConfigurationProperties(prefix = "debezium")
public class DebeziumProperties {

    private ChangeFormat format = ChangeFormat.JSON;

    public ChangeFormat getFormat() {
        return format == null ? ChangeFormat.JSON : format;
    }

    public void setFormat(String format) {
        this.format = ChangeFormat.fromName(format);
    }
}
