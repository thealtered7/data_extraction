package com.dataextraction.cdc_consumer;

import java.util.Objects;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.cdc_consumer.format.DeserializedRecord;

/**
 * Reads the Debezium envelope from an Avro {@link GenericRecord}. Avro conversion drops the
 * Connect JSON {@code payload} wrapper, so {@code op} and {@code source} sit on the record itself.
 */
public final class AvroRecordDelegate implements RecordDelegate {

    private static final Logger log = LoggerFactory.getLogger(AvroRecordDelegate.class);
    private static final String HEARTBEAT_MARKER = "__debezium-heartbeat";

    private final String destination;
    private final DeserializedRecord decoded;

    private AvroRecordDelegate(String destination, DeserializedRecord decoded) {
        this.destination = destination;
        this.decoded = Objects.requireNonNull(decoded, "decoded record is required");
    }

    public static AvroRecordDelegate from(String destination, DeserializedRecord decoded) {
        return new AvroRecordDelegate(destination, decoded);
    }

    @Override
    public DeserializedRecord decoded() {
        return decoded;
    }

    @Override
    public String getTableName() {
        GenericRecord source = source();
        if (source == null) {
            log.warn("Avro change event missing source; using unknown table FQN");
            return "unknown";
        }
        String db = text(field(source, "db"));
        String schema = text(field(source, "schema"));
        String table = text(field(source, "table"));
        if (db.isEmpty() || schema.isEmpty() || table.isEmpty()) {
            log.warn("Avro change event source missing db/schema/table; using unknown table FQN");
            return "unknown";
        }
        return db + "." + schema + "." + table;
    }

    @Override
    public Long getLsn() {
        GenericRecord source = source();        
        if (isHeartbeat()) {
            log.debug("Heartbeat event; using unknown LSN");
            return null;
        }
        if (source == null) {
            log.warn("Avro change event missing source; using unknown LSN");
            return null;
        }
        Object lsn = field(source, "lsn");
        if (lsn == null) {
            log.warn("Avro change event missing lsn; using unknown LSN");
            return null;
        }
        if (lsn instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(lsn.toString(), 10);
        } catch (NumberFormatException e) {
            log.warn("Avro change event lsn is not a number: {}", lsn);
            return null;
        }
    }

    @Override
    public boolean isHeartbeat() {
        if (destination != null && destination.contains(HEARTBEAT_MARKER)) {
            return true;
        }
        Schema keySchema = decoded.avroKeySchema();
        return keySchema != null && keySchema.getFullName() != null && keySchema.getFullName().contains(HEARTBEAT_MARKER);
    }

    @Override
    public boolean isTombstone() {
        return decoded.value() == null;
    }

    @Override
    public boolean isCdcChangeEvent() {
        GenericRecord value = valueRecord();
        if (value == null) {
            return false;
        }
        GenericRecord source = source();
        if (source == null) {
            return false;
        }
        if (text(field(source, "table")).isEmpty()) {
            return false;
        }
        return !text(field(value, "op")).isEmpty();
    }

    @Override
    public boolean isSnapshotReadEvent() {
        GenericRecord value = valueRecord();
        if (value == null) {
            return false;
        }
        return "r".equals(text(field(value, "op")));
    }

    @Override
    public boolean isSnapshotLastEvent() {
        GenericRecord source = source();
        if (source == null) {
            return false;
        }
        return "last".equalsIgnoreCase(text(field(source, "snapshot")));
    }

    private GenericRecord valueRecord() {
        Object value = decoded.value();
        if (value instanceof GenericRecord record) {
            return record;
        }
        return null;
    }

    private GenericRecord source() {
        Object source = field(valueRecord(), "source");
        if (source instanceof GenericRecord record) {
            return record;
        }
        return null;
    }

    private static Object field(GenericRecord record, String name) {
        if (record == null || record.getSchema() == null || record.getSchema().getField(name) == null) {
            return null;
        }
        return record.get(name);
    }

    private static String text(Object value) {
        return value == null ? "" : Objects.toString(value, "");
    }
}
