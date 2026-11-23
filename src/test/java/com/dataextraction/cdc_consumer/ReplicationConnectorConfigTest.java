package com.dataextraction.cdc_consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.dataextraction.config.DatabaseProperties;

class ReplicationConnectorConfigTest {

    @Test
    void streamsFromTheSnapshotSlotWithASeparateOffsetFile() {
        DatabaseProperties database = new DatabaseProperties();
        database.setHostname("postgres");
        database.setPort(5432);
        database.setUser("extract_pipeline_user");
        database.setPassword("extract_pipeline_user");
        database.setDbname("geo");
        database.setSlotName("extract_slot");
        database.setPublicationName("extract_publication");
        database.setOffsetStorageFileFilename("/opt/data/raw/snapshot-offsets.dat");
        database.setCdcOffsetStorageFileFilename("/opt/data/raw/cdc-offsets.dat");
        database.setTableIncludeList(" public\\.wide ");

        Properties props = new ReplicationConnectorConfig().create(database);

        assertEquals("cdc-replicate", props.getProperty("name"));
        assertEquals("pgoutput", props.getProperty("plugin.name"));
        assertEquals("extract_slot", props.getProperty("slot.name"));
        assertEquals("extract_publication", props.getProperty("publication.name"));
        assertEquals("false", props.getProperty("slot.drop.on.stop"));
        assertEquals("false", props.getProperty("tombstones.on.delete"));
        assertEquals("no_data", props.getProperty("snapshot.mode"));
        assertEquals("1000", props.getProperty("heartbeat.interval.ms"));
        assertEquals("/opt/data/raw/cdc-offsets.dat", props.getProperty("offset.storage.file.filename"));
        assertFalse(props.getProperty("offset.storage.file.filename").contains("snapshot-offsets"));
        assertEquals("public\\.wide", props.getProperty("table.include.list"));
        assertEquals("org.apache.kafka.connect.storage.FileOffsetBackingStore", props.getProperty("offset.storage"));
    }

    @Test
    void omitsTableIncludeListWhenUnset() {
        DatabaseProperties database = new DatabaseProperties();
        database.setCdcOffsetStorageFileFilename("cdc-offsets.dat");

        Properties props = new ReplicationConnectorConfig().create(database);

        assertNull(props.getProperty("table.include.list"));
        assertEquals("cdc-offsets.dat", props.getProperty("offset.storage.file.filename"));
    }
}
