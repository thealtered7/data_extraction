package com.dataextraction.sink.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dataextraction.sink.AvroFileMetadata;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class AvroExtractRecordSinkAvroWriteTest {

    @Test
    void keySchemaIsFileMetadataAndValueIsWrappedOnce() throws Exception {
        Schema keySchema = SchemaBuilder.record("Key").fields().name("id").type().longType().noDefault().endRecord();
        Schema valueSchema = SchemaBuilder.record("Envelope")
                .namespace("io.debezium")
                .fields()
                .name("op").type().stringType().noDefault()
                .endRecord();
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("op", "r");

        Path file = Files.createTempFile("avro-write", ".avro");
        AvroExtractRecordSink sink = new AvroExtractRecordSink(new ConnectAvroSchemas("geo"));
        sink.open(file);
        sink.writeAvro(value, keySchema, "job-1", "buffer-1", "snapshot", "2026-11-22T00:00:00Z");
        sink.close();

        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(file.toFile(), new GenericDatumReader<>())) {
            assertEquals(keySchema.toString(), reader.getMetaString(AvroFileMetadata.KEY_SCHEMA));
            assertEquals("geo.ExtractRecord", reader.getSchema().getFullName());
            assertTrue(reader.hasNext());
            GenericRecord written = reader.next();
            GenericRecord extract = (GenericRecord) written.get("extract");
            GenericRecord payload = (GenericRecord) written.get("payload");
            assertEquals("job-1", extract.get("extract_job_id").toString());
            assertEquals("r", payload.get("op").toString());
            assertTrue(reader.getSchema().getField("payload").schema().toString().contains("Envelope"));
        }
    }
}
