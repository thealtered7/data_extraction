package com.dataextraction.cdc_consumer;

import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.cdc_consumer.format.DecodedChange;
import com.dataextraction.cdc_consumer.format.DeserializedRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonRecordDelegate implements RecordDelegate {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(JsonRecordDelegate.class);

    private final String destination;
    private final String key;
    private final String value;
    private final DeserializedRecord decoded;
    private Map<String, Object> record;
    private boolean parsed;

    public JsonRecordDelegate(Map<String, Object> record) {
        this.destination = null;
        this.key = null;
        this.value = null;
        this.decoded = DecodedChange.passthrough(null, null);
        this.record = record;
        this.parsed = true;
    }

    public JsonRecordDelegate(String json) throws JsonProcessingException {
        this(null, null, json, DecodedChange.passthrough(null, json));
        this.record = parse(json);
        this.parsed = true;
    }

    private JsonRecordDelegate(String destination, String key, String value, DeserializedRecord decoded) {
        this.destination = destination;
        this.key = key;
        this.value = value;
        this.decoded = decoded;
    }

    public static JsonRecordDelegate from(String destination, DeserializedRecord decoded) {
        String key = asString(decoded == null ? null : decoded.key());
        String value = asString(decoded == null ? null : decoded.value());
        DeserializedRecord stored = decoded == null ? DecodedChange.passthrough(key, value) : decoded;
        return new JsonRecordDelegate(destination, key, value, stored);
    }

    @Override
    public DeserializedRecord decoded() {
        return decoded;
    }

    public Map<String, Object> getRecord() {
        return record();
    }

    @Override
    public String getTableName() {
        Map<String, Object> parsedRecord = record();
        if (parsedRecord == null || parsedRecord.isEmpty()) {
            log.warn("ChangeEvent has no value; using unknown table FQN");
            return "unknown";
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) parsedRecord.get("payload");
            if (payload == null) {
                log.warn("ChangeEvent missing payload; using unknown table FQN");
                return "unknown";
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) payload.get("source");
            if (source == null) {
                log.warn("ChangeEvent missing payload.source; using unknown table FQN");
                return "unknown";
            }
            String db = Objects.toString(source.get("db"), "");
            String schema = Objects.toString(source.get("schema"), "");
            String table = Objects.toString(source.get("table"), "");
            if (db.isEmpty() || schema.isEmpty() || table.isEmpty()) {
                log.warn("ChangeEvent source missing db/schema/table; using unknown table FQN");
                return "unknown";
            }
            return db + "." + schema + "." + table;
        } catch (Exception e) {
            log.warn("Failed to parse table FQN from ChangeEvent", e);
            return "unknown";
        }
    }

    @Override
    public Long getLsn() {
        Map<String, Object> parsedRecord = record();
        if (parsedRecord == null) {
            log.warn("ChangeEvent missing lsn; using unknown LSN");
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) parsedRecord.get("payload");
        if (payload == null) {
            log.warn("ChangeEvent missing lsn; using unknown LSN");
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) payload.get("source");
        if (source == null) {
            log.warn("ChangeEvent missing lsn; using unknown LSN");
            return null;
        }
        Object lsn = source.get("lsn");
        if (lsn == null) {
            log.warn("ChangeEvent missing lsn; using unknown LSN");
            return null;
        }
        return Long.parseLong(lsn.toString(), 10);
    }

    public static boolean isHeartbeat(String key, String value) {
        return key != null && key.contains("__debezium-heartbeat");
    }

    /**
     * Debezium emits a tombstone (null value, or {@code {"schema":null,"payload":null}}) after each
     * delete for Kafka log compaction. File sinks should skip these; the preceding {@code op:"d"}
     * event carries the delete payload.
     */
    public static boolean isTombstone(String value) {
        if (value == null) {
            return true;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty()
                || "{\"schema\":null,\"payload\":null}".equals(trimmed)
                || "{\"schema\": null, \"payload\": null}".equals(trimmed);
    }

    @Override
    public boolean isHeartbeat() {
        if (destination != null && destination.contains("__debezium-heartbeat")) {
            return true;
        }
        return isHeartbeat(key, value);
    }

    @Override
    public boolean isTombstone() {
        return isTombstone(value);
    }

    @Override
    public boolean isCdcChangeEvent() {
        Map<String, Object> parsedRecord = record();
        if (parsedRecord == null || parsedRecord.isEmpty()) {
            return false;
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) parsedRecord.get("payload");
            if (payload == null) {
                return false;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) payload.get("source");
            if (source == null) {
                return false;
            }
            String table = Objects.toString(source.get("table"), "");
            if (table.isEmpty()) {
                return false;
            }
            String op = Objects.toString(payload.get("op"), "");
            return !op.isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean isSnapshotReadEvent() {
        Map<String, Object> parsedRecord = record();
        if (parsedRecord == null || parsedRecord.isEmpty()) {
            return false;
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) parsedRecord.get("payload");
            if (payload == null) {
                return false;
            }
            return "r".equals(Objects.toString(payload.get("op"), ""));
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean isSnapshotLastEvent() {
        Map<String, Object> parsedRecord = record();
        if (parsedRecord == null || parsedRecord.isEmpty()) {
            return false;
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) parsedRecord.get("payload");
            if (payload == null) {
                return false;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) payload.get("source");
            if (source == null) {
                return false;
            }
            return "last".equalsIgnoreCase(Objects.toString(source.get("snapshot"), ""));
        } catch (Exception e) {
            return false;
        }
    }

    private Map<String, Object> record() {
        if (!parsed) {
            parsed = true;
            try {
                record = parse(value);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Change event value is not JSON", e);
            }
        }
        return record;
    }

    private static Map<String, Object> parse(String json) throws JsonProcessingException {
        if (json == null) {
            return null;
        }
        return OBJECT_MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
    }

    private static String asString(Object value) {
        if (value == null) {
            return null;
        }
        return value instanceof String text ? text : value.toString();
    }
}
