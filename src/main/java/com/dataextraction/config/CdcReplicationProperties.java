package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Catch-up replication. The engine stops when the slot is already at the WAL LSN captured at
 * start, when a change reaches that LSN, or after {@code walIdleTimeoutSeconds} with no further
 * WAL data once that LSN has been passed. The idle countdown starts when replication starts.
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
