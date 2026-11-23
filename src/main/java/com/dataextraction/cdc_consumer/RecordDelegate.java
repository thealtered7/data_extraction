package com.dataextraction.cdc_consumer;

import com.dataextraction.cdc_consumer.format.DeserializedRecord;

/**
 * Format-neutral view of one change event. Callers use this instead of JSON or Avro types.
 */
public interface RecordDelegate {

    String getTableName();

    Long getLsn();

    boolean isHeartbeat();

    boolean isTombstone();

    boolean isCdcChangeEvent();

    boolean isSnapshotReadEvent();

    boolean isSnapshotLastEvent();

    DeserializedRecord decoded();
}
