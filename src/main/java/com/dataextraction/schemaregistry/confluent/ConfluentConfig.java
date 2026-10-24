package com.dataextraction.schemaregistry.confluent;

public class ConfluentConfig {
    private String url;
    private boolean autoRegisterSchemas;
    private String compatibilityLevel;

    public ConfluentConfig(String url, boolean autoRegisterSchemas, String compatibilityLevel) {
        this.url = url;
        this.autoRegisterSchemas = autoRegisterSchemas;
        this.compatibilityLevel = compatibilityLevel;
    }

    public ConfluentConfig() {
        this.url = null;
        this.autoRegisterSchemas = false;
        this.compatibilityLevel = null;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setAutoRegisterSchemas(boolean autoRegisterSchemas) {
        this.autoRegisterSchemas = autoRegisterSchemas;
    }

    public void setCompatibilityLevel(String compatibilityLevel) {
        this.compatibilityLevel = compatibilityLevel;
    }

    public String getUrl() {
        return url;
    }

    public boolean isAutoRegisterSchemas() {
        return autoRegisterSchemas;
    }

    public String getCompatibilityLevel() {
        return compatibilityLevel;
    }
}
