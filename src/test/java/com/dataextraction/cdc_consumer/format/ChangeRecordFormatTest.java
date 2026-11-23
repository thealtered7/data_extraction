package com.dataextraction.cdc_consumer.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dataextraction.cdc_consumer.AvroRecordDelegate;
import com.dataextraction.cdc_consumer.JsonRecordDelegate;
import com.dataextraction.cdc_consumer.RecordDelegate;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

class ChangeRecordFormatTest {

    @Test
    void jsonDeserializerPassesTheEnvelopeThrough() {
        JsonRecordDeserializer deserializer = JsonRecordDeserializer.create();
        String value = """
                {"schema":{"type":"struct"},"payload":{"op":"r","source":{"db":"geo","schema":"public","table":"wide","lsn":42,"snapshot":"last"}}}
                """;
        DeserializedRecord decoded = deserializer.deserialize("snapshot.geo.public.wide", "{\"id\":1}", value);

        assertEquals("{\"id\":1}", decoded.key());
        assertSame(value, decoded.value());
        assertNull(decoded.keySchemaId());
        assertNull(decoded.valueSchemaId());

        RecordDelegate delegate = JsonRecordDelegate.from("snapshot.geo.public.wide", decoded);
        assertEquals("geo.public.wide", delegate.getTableName());
        assertEquals(42L, delegate.getLsn());
        assertTrue(delegate.isSnapshotReadEvent());
        assertTrue(delegate.isSnapshotLastEvent());
        assertFalse(delegate.isHeartbeat());
        assertFalse(delegate.isTombstone());
    }

    @Test
    void jsonHeartbeatAndTombstoneDoNotRequireAPayload() {
        DeserializedRecord heartbeat = DecodedChange.passthrough("{\"schema\":\"__debezium-heartbeat\"}", "{}");
        RecordDelegate heartbeatDelegate = JsonRecordDelegate.from("topic", heartbeat);
        assertTrue(heartbeatDelegate.isHeartbeat());

        DeserializedRecord tombstone = DecodedChange.passthrough("{\"id\":1}", null);
        RecordDelegate tombstoneDelegate = JsonRecordDelegate.from("topic", tombstone);
        assertTrue(tombstoneDelegate.isTombstone());
    }

    @Test
    void avroDeserializerProxiesTheThirdPartyDecoderAndKeepsSchemaIds() {
        Schema keySchema = SchemaBuilder.record("Key").fields().name("id").type().intType().noDefault().endRecord();
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("id", 7);
        GenericRecord value = envelope("r", "last", 99L);
        Deserializer<Object> keys = (topic, data) -> key;
        Deserializer<Object> values = (topic, data) -> value;

        AvroRecordDeserializer deserializer = AvroRecordDeserializer.builder()
                .withKeyDeserializer(keys)
                .withValueDeserializer(values)
                .build();
        DeserializedRecord decoded = deserializer.deserialize(
                "snapshot.geo.public.wide",
                ConfluentWireFormat.payload(3, new byte[] {1}),
                ConfluentWireFormat.payload(8, new byte[] {2}));

        assertSame(key, decoded.key());
        assertSame(value, decoded.value());
        assertEquals("3", decoded.keySchemaId());
        assertEquals("8", decoded.valueSchemaId());
        assertEquals(keySchema, decoded.avroKeySchema());

        RecordDelegate delegate = AvroRecordDelegate.from("snapshot.geo.public.wide", decoded);
        assertEquals("geo.public.wide", delegate.getTableName());
        assertEquals(99L, delegate.getLsn());
        assertTrue(delegate.isSnapshotReadEvent());
        assertTrue(delegate.isSnapshotLastEvent());
        assertTrue(delegate.isCdcChangeEvent());
        assertFalse(delegate.isTombstone());
    }

    @Test
    void avroHeartbeatUsesTheDestinationAndNullValueIsATombstone() {
        RecordDelegate heartbeat = AvroRecordDelegate.from(
                "snapshot.__debezium-heartbeat.geo",
                DecodedChange.avro(null, envelope("r", "false", 1L), null, "1", null, null));
        assertTrue(heartbeat.isHeartbeat());

        RecordDelegate tombstone = AvroRecordDelegate.from(
                "snapshot.geo.public.wide",
                DecodedChange.avro(null, null, "3", null, null, null));
        assertTrue(tombstone.isTombstone());
    }

    @Test
    void confluentWireFormatReadsTheSchemaId() {
        assertEquals("42", ConfluentWireFormat.schemaId(ConfluentWireFormat.payload(42, new byte[] {9})));
        assertNull(ConfluentWireFormat.schemaId(new byte[] {1, 0, 0, 0, 1}));
        assertNull(ConfluentWireFormat.schemaId(null));
    }

    private static GenericRecord envelope(String op, String snapshot, long lsn) {
        Schema sourceSchema = SchemaBuilder.record("source")
                .fields()
                .name("db").type().stringType().noDefault()
                .name("schema").type().stringType().noDefault()
                .name("table").type().stringType().noDefault()
                .name("lsn").type().longType().noDefault()
                .name("snapshot").type().stringType().noDefault()
                .endRecord();
        Schema envelopeSchema = SchemaBuilder.record("Envelope")
                .fields()
                .name("op").type().stringType().noDefault()
                .name("source").type(sourceSchema).noDefault()
                .endRecord();
        GenericRecord source = new GenericData.Record(sourceSchema);
        source.put("db", "geo");
        source.put("schema", "public");
        source.put("table", "wide");
        source.put("lsn", lsn);
        source.put("snapshot", snapshot);
        GenericRecord envelope = new GenericData.Record(envelopeSchema);
        envelope.put("op", op);
        envelope.put("source", source);
        return envelope;
    }
}
