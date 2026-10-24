package com.dataextraction.schemaregistry;

/** No-op registrar used when {@code schema.registry.type=none}. */
final class NoneCdcSchemaRegistrar implements CdcSchemaRegistrar {

    @Override
    public String register(String subject, String connectSchemaJson) {
        return null;
    }
}
