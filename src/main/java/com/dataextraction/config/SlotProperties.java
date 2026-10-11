package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "slot")
public class SlotProperties {

    private String name = "extract_slot";

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
