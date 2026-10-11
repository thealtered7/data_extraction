package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dataextraction")
public class DataExtractionProperties {

    private HelloerType helloer = HelloerType.SIMPLE;

    public HelloerType getHelloer() {
        return helloer;
    }

    public void setHelloer(HelloerType helloer) {
        this.helloer = helloer;
    }

    public enum HelloerType {
        SIMPLE,
        FART
    }
}
