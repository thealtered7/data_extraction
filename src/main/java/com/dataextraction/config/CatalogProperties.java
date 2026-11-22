package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "catalog")
public class CatalogProperties {

    private String name = "lakehouse";

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
