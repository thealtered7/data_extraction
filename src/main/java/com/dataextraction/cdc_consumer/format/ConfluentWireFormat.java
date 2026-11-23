package com.dataextraction.cdc_consumer.format;

import java.nio.ByteBuffer;

/**
 * Confluent wire format: magic byte {@code 0}, a 4-byte schema id, then the Avro payload.
 * The schema document itself stays in the schema registry.
 */
public final class ConfluentWireFormat {

    public static final byte MAGIC = 0x0;

    private ConfluentWireFormat() {
    }

    public static String schemaId(byte[] payload) {
        if (payload == null || payload.length < 5 || payload[0] != MAGIC) {
            return null;
        }
        int id = ByteBuffer.wrap(payload, 1, 4).getInt();
        return Integer.toString(id);
    }

    public static byte[] payload(int schemaId, byte[] body) {
        byte[] avroBody = body == null ? new byte[0] : body;
        ByteBuffer buffer = ByteBuffer.allocate(5 + avroBody.length);
        buffer.put(MAGIC);
        buffer.putInt(schemaId);
        buffer.put(avroBody);
        return buffer.array();
    }
}
