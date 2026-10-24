package com.dataextraction.schemaregistry;

import java.util.Arrays;
import java.util.Optional;

public enum SchemaRegistryBackend {
    NONE("none"),
    CONFLUENT("confluent"),
    AWSGLUE("awsglue");

    private final String name;

    SchemaRegistryBackend(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static Optional<SchemaRegistryBackend> fromName(String name) {
        if (name == null || name.isBlank()) {
            return Optional.empty();
        }
        String normalizedName = name.toLowerCase().trim();
        return Arrays.stream(values())
                .filter(type -> type.getName().equals(normalizedName))
                .findFirst();
    }
}
