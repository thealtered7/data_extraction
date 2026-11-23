package com.dataextraction.cdc_consumer;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.config.DatabaseProperties;

/**
 * Debezium connector settings for {@code cdc-replicate}. The slot is the snapshot slot.
 * Offsets live in a separate file that this command leaves in place across runs.
 * A missing offset file uses {@code snapshot.mode=no_data}: schema only, then stream.
 */
public final class ReplicationConnectorConfig {

    private static final Logger log = LoggerFactory.getLogger(ReplicationConnectorConfig.class);

    public Properties create(DatabaseProperties database) {
        Properties connectorProps = new Properties();
        connectorProps.setProperty("name", "cdc-replicate");
        connectorProps.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        connectorProps.setProperty("plugin.name", "pgoutput");
        connectorProps.setProperty("database.hostname", database.getHostname());
        connectorProps.setProperty("database.port", Integer.toString(database.getPort()));
        connectorProps.setProperty("database.user", database.getUser());
        connectorProps.setProperty("database.password", database.getPassword());
        connectorProps.setProperty("database.dbname", database.getDbname());
        connectorProps.setProperty("slot.name", database.getSlotName());
        connectorProps.setProperty("publication.name", database.getPublicationName());
        connectorProps.setProperty("slot.drop.on.stop", "false");
        connectorProps.setProperty("tombstones.on.delete", "false");
        connectorProps.setProperty("topic.prefix", "cdc");
        connectorProps.setProperty("snapshot.mode", "no_data");
        // Keep the stream alive and advance offsets through WAL stretches with no table events
        // (large slot lag). Without this, wal-idle-timeout can stop catch-up early.
        connectorProps.setProperty("heartbeat.interval.ms", "1000");
        connectorProps.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        connectorProps.setProperty("offset.storage.file.filename", database.getCdcOffsetStorageFileFilename());
        connectorProps.setProperty("record.processing.threads", "1");
        String tableIncludeList = database.getTableIncludeList();
        if (tableIncludeList != null && !tableIncludeList.isBlank()) {
            connectorProps.setProperty("table.include.list", tableIncludeList.trim());
            log.info("Replication table include list: {}", tableIncludeList.trim());
        }
        return connectorProps;
    }
}
