package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.dataextraction.schemaregistry.SchemaRegistryBackend;

@ConfigurationProperties(prefix = "schema-registry")
public class SchemaRegistryProperties {

    public static class ConfluentProperties {
        private String url;
        private boolean autoRegisterSchemas;
        private String compatibilityLevel;

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUrl() {
            return url;
        }

        public void setAutoRegisterSchemas(boolean autoRegisterSchemas) {
            this.autoRegisterSchemas = autoRegisterSchemas;
        }

        public boolean isAutoRegisterSchemas() {
            return autoRegisterSchemas;
        }

        public void setCompatibilityLevel(String compatibilityLevel) {
            this.compatibilityLevel = compatibilityLevel;
        }

        public String getCompatibilityLevel() {
            return compatibilityLevel;
        }
    }

    public static class AwsGlueProperties {
        private String region;
        private String registryName;
        private boolean autoRegisterSchemas;
        private String compatibilityLevel;

        public void setRegion(String region) {
            this.region = region;
        }

        public String getRegion() {
            return region;
        }

        public void setRegistryName(String registryName) {
            this.registryName = registryName;
        }

        public String getRegistryName() {
            return registryName;
        }

        public void setAutoRegisterSchemas(boolean autoRegisterSchemas) {
            this.autoRegisterSchemas = autoRegisterSchemas;
        }

        public boolean isAutoRegisterSchemas() {
            return autoRegisterSchemas;
        }

        public void setCompatibilityLevel(String compatibilityLevel) {
            this.compatibilityLevel = compatibilityLevel;
        }

        public String getCompatibilityLevel() {
            return compatibilityLevel;
        }
    }

    private String type;
    private ConfluentProperties confluent;
    private AwsGlueProperties glue;

    public void setConfluent(ConfluentProperties confluent) {
        this.confluent = confluent;
    }

    public ConfluentProperties getConfluent() {
        return confluent;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setGlue(AwsGlueProperties glue) {
        this.glue = glue;
    }

    public AwsGlueProperties getGlue() {
        return glue;
    }

    public SchemaRegistryBackend getSchemaRegistryBackend() {
        return SchemaRegistryBackend.fromName(getType()).orElse(SchemaRegistryBackend.NONE);
    }
}
