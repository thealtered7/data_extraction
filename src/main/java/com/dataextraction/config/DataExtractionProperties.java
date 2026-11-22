package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dataextraction")
public class DataExtractionProperties {

    private HelloerType helloer = HelloerType.SIMPLE;
    private int maxBufferSizeBytes = 1073741824; // 1GB
    private String extractNamespace;

    public HelloerType getHelloer() {
        return helloer;
    }

    public void setHelloer(HelloerType helloer) {
        this.helloer = helloer;
    }

    public int getMaxBufferSizeBytes() {
        return maxBufferSizeBytes;
    }

    public void setMaxBufferSizeBytes(int maxBufferSizeBytes) {
        this.maxBufferSizeBytes = maxBufferSizeBytes;
    }

    public enum HelloerType {
        SIMPLE,
        FART
    }

    public String getExtractNamespace() {
        return extractNamespace;
    }

    public void setExtractNamespace(String extractNamespace) {
        this.extractNamespace = extractNamespace;
    }
}
