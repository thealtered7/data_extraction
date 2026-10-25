package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "sink")
public class SinkProperties {

    private String sinkType;
    private String bufferFlushHandler;

    public String getSinkType() {
        return this.sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }
    
    public String getBufferFlushHandler() {
        return this.bufferFlushHandler;
    }

    public void setBufferFlushHandler(String bufferFlushHandler) {
        this.bufferFlushHandler = bufferFlushHandler;
    }

    
}
