package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Catch-up replication. The engine stops once a change passes the WAL LSN captured at
 * start, and also stops after {@code walIdleTimeoutSeconds} with no CDC record.
 */
@ConfigurationProperties(prefix = "cdc")
public class CdcReplicationProperties {

    private boolean highWaterMarkEnabled = true;
    private long walIdleTimeoutSeconds = 10;

    public boolean isHighWaterMarkEnabled() {
        return highWaterMarkEnabled;
    }

    public void setHighWaterMarkEnabled(boolean highWaterMarkEnabled) {
        this.highWaterMarkEnabled = highWaterMarkEnabled;
    }

    public long getWalIdleTimeoutSeconds() {
        return walIdleTimeoutSeconds;
    }

    public void setWalIdleTimeoutSeconds(long walIdleTimeoutSeconds) {
        this.walIdleTimeoutSeconds = walIdleTimeoutSeconds;
    }
}
