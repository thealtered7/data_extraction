package com.dataextraction.cdc_consumer;

import java.util.Map;
import java.util.Objects;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;



public class JsonRecordDelegate {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(JsonRecordDelegate.class);

    private Map<String, Object> record;

    public JsonRecordDelegate(Map<String, Object> record) {
        this.record = record;
    }

    public JsonRecordDelegate(String json) throws JsonProcessingException {
        if (json == null) {
            this.record = null;
            return;
        }
        this.record = OBJECT_MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
    }

    public Map<String, Object> getRecord() {
        return record;
    }

    public String getTableName() {
        if (record == null || record.isEmpty()) {
            log.warn("ChangeEvent has no value; using unknown table FQN");
            return "unknown";
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) record.get("payload");
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

    public Long getLsn() {
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) record.get("payload");
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) payload.get("source");
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

    public boolean isCdcChangeEvent() {
        if (record == null || record.isEmpty()) {
            return false;
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) record.get("payload");
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

    public boolean isSnapshotReadEvent() {
        if (record == null || record.isEmpty()) {
            return false;
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) record.get("payload");
            if (payload == null) {
                return false;
            }
            return "r".equals(Objects.toString(payload.get("op"), ""));
        } catch (Exception e) {
            return false;
        }
    }

    public boolean isSnapshotLastEvent() {
        if (record == null || record.isEmpty()) {
            return false;
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) record.get("payload");
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

}
