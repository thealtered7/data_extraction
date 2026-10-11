package com.dataextraction.pg.meta;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PostgresMetaCommandsTest {

    @Test
    void formatPrintsHighWaterMarkAndSlot() {
        PostgresReplicationSlotHighWaterMark highWaterMark = new PostgresReplicationSlotHighWaterMark();
        highWaterMark.setHighWaterMark(new PostgresLsn("0/16B3748"));
        ReplicationSlotInfo slot = new ReplicationSlotInfo("geo", "extract_slot", "0/16B3000", "0/16B2000", "0/16B3748", "1864");

        String output = PostgresMetaCommands.format(highWaterMark, "extract_slot", slot);

        assertTrue(output.contains("high water mark: 0/16B3748"));
        assertTrue(output.contains("replication slot: extract_slot"));
        assertTrue(output.contains("flush lsn: 0/16B3000"));
        assertTrue(output.contains("lag bytes: 1864"));
    }

    @Test
    void formatReportsMissingSlot() {
        PostgresReplicationSlotHighWaterMark highWaterMark = new PostgresReplicationSlotHighWaterMark();
        highWaterMark.setHighWaterMark(new PostgresLsn("0/1"));

        String output = PostgresMetaCommands.format(highWaterMark, "missing_slot", null);

        assertTrue(output.contains("high water mark: 0/1"));
        assertTrue(output.contains("replication slot: not found (missing_slot)"));
    }
}
