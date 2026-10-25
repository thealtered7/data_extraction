package com.thealtered7;

import java.util.Locale;
import java.util.Objects;

/** On-disk format for raw CDC extract files flushed by {@link ExtractJsonBuffer}. */
public enum ExtractOutputFormat {
    JSONL("jsonl"),
    AVRO("avro");

    private final String extension;

    ExtractOutputFormat(String extension) {
        this.extension = extension;
    }

    public String extension() {
        return extension;
    }

    public String fileSuffix() {
        return "." + extension;
    }

    public static ExtractOutputFormat fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return JSONL;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (ExtractOutputFormat format : values()) {
            if (format.extension.equals(normalized) || format.name().equalsIgnoreCase(normalized)) {
                return format;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported app.cdc.output.format '" + value + "'; expected jsonl or avro");
    }

    public static ExtractOutputFormat require(ExtractOutputFormat format) {
        return Objects.requireNonNullElse(format, JSONL);
    }
}
